use super::{
    EvalDataset, EvalNodeWithStats, TupleSelector, encode_initial_bindings, encode_variable,
    eval_node_label,
};
#[cfg(feature = "sparql-12")]
use crate::ExpressionTerm;
#[cfg(feature = "sparql-12")]
use crate::ExpressionTriple;
use crate::expression::CustomFunctionRegistry;
use crate::service::ServiceHandlerRegistry;
use crate::{
    CancellationToken, CustomAggregateFunctionRegistry, Delta, DeltaKind, IncrementalQuadSource,
    IncrementalQueryableDataset, InternalQuad, QueryDatasetSpecification, QueryEvaluationError,
    QuerySolution,
};
use differential_dataflow::collection::VecCollection;
use differential_dataflow::input::{Input, InputSession};
use oxiri::Iri;
#[cfg(feature = "sparql-12")]
use oxrdf::Triple;
use oxrdf::{Term, Variable};
use oxsdatatypes::{DateTime, DayTimeDuration};
use oxstr::OxString;
use rustc_hash::FxHashMap;
use spargebra::term::GroundTerm;
#[cfg(feature = "sparql-12")]
use spargebra::term::GroundTriple;
use sparopt::algebra::{JoinAlgorithm, QueryExpression};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use timely::communication::{Allocator, allocator::thread::Thread};
use timely::dataflow::{ProbeHandle, Scope};
use timely::worker::Worker;

pub type QuerySolutionChange = Delta<QuerySolution>;

/// Compatibility type for consumers of the previous streaming source API.
pub enum StreamingItem<T> {
    Item(T),
    Pending,
}

pub enum IncrementalDriverState<T> {
    Item(T),
    Pending,
    Done,
}

/// Query-local notification state shared weakly with its live dataset subscription.
pub struct IncrementalQueryNotifier {
    generation: AtomicU64,
    waker: Mutex<Option<Waker>>,
}

impl IncrementalQueryNotifier {
    fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            waker: Mutex::new(None),
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn register_waker(&self, waker: &Waker) {
        let mut stored = self.waker.lock().unwrap();
        if stored.as_ref().is_none_or(|old| !old.will_wake(waker)) {
            *stored = Some(waker.clone());
        }
    }

    pub fn clear_waker(&self) {
        *self.waker.lock().unwrap() = None;
    }

    pub fn notify(&self) {
        self.generation.fetch_add(1, Ordering::Release);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    pub(crate) fn changed_since<'n, 'c>(
        &'n self,
        baseline: u64,
        cancellation_token: &'c CancellationToken,
    ) -> NotifierChangeFuture<'n, 'c> {
        NotifierChangeFuture {
            notifier: self,
            baseline,
            cancellation_token,
            cancellation_waker: None,
        }
    }
}

pub(crate) struct NotifierChangeFuture<'n, 'c> {
    notifier: &'n IncrementalQueryNotifier,
    baseline: u64,
    cancellation_token: &'c CancellationToken,
    cancellation_waker: Option<Waker>,
}

impl Future for NotifierChangeFuture<'_, '_> {
    type Output = Result<(), QueryEvaluationError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.cancellation_token.ensure_alive()?;
        if this.notifier.generation() != this.baseline {
            return Poll::Ready(Ok(()));
        }
        this.notifier.register_waker(cx.waker());
        if let Some(old) = &this.cancellation_waker
            && !old.will_wake(cx.waker())
        {
            this.cancellation_token.unregister_waker(old);
        }
        this.cancellation_token.register_waker(cx.waker());
        this.cancellation_waker = Some(cx.waker().clone());
        this.cancellation_token.ensure_alive()?;
        if this.notifier.generation() == this.baseline {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl Drop for NotifierChangeFuture<'_, '_> {
    fn drop(&mut self) {
        self.notifier.clear_waker();
        if let Some(waker) = &self.cancellation_waker {
            self.cancellation_token.unregister_waker(waker);
        }
    }
}

/// The read-only evaluator remains unchanged. This driver owns one dataflow for the entire
/// incremental query; only the result adapter outside the graph is pull-based.
pub struct IncrementalSelectDriver<'a, D: IncrementalQueryableDataset<'a>> {
    dataset: EvalDataset<'a, D>,
    notifier: Arc<IncrementalQueryNotifier>,
    variables: Arc<[Variable]>,
    output_indices: Vec<usize>,
    source: Box<dyn IncrementalQuadSource<D::InternalTerm, D::Error> + 'a>,
    ids: QueryLocalTermIds<D::InternalTerm>,
    triple_components: Rc<RefCell<FxHashMap<u64, [u64; 3]>>>,
    needs_triples: bool,
    quad_input: InputSession<u64, EncodedQuad, isize>,
    values_inputs: Vec<(InputSession<u64, EncodedTuple, isize>, Vec<EncodedTuple>)>,
    worker: Worker,
    probe: ProbeHandle<u64>,
    output: Rc<RefCell<VecDeque<(EncodedTuple, u64, isize)>>>,
    pending_output: Option<(EncodedTuple, isize)>,
    epoch: u64,
    initialized: bool,
}

pub struct SimpleIncrementalEvaluator<'a, D: IncrementalQueryableDataset<'a>> {
    dataset: EvalDataset<'a, D>,
    base_iri: Option<Iri<OxString>>,
    now: DateTime,
    service_handler: Rc<ServiceHandlerRegistry>,
    custom_functions: Rc<CustomFunctionRegistry>,
    custom_aggregate_functions: Rc<CustomAggregateFunctionRegistry>,
    run_stats: bool,
}

impl<'a, D: IncrementalQueryableDataset<'a>> SimpleIncrementalEvaluator<'a, D> {
    pub fn new(
        dataset: D,
        base_iri: Option<Iri<OxString>>,
        service_handler: Rc<ServiceHandlerRegistry>,
        custom_functions: Rc<CustomFunctionRegistry>,
        custom_aggregate_functions: Rc<CustomAggregateFunctionRegistry>,
        cancellation_token: CancellationToken,
        dataset_spec: QueryDatasetSpecification,
        run_stats: bool,
    ) -> Result<Self, QueryEvaluationError> {
        Ok(Self {
            dataset: EvalDataset::new(dataset, dataset_spec, cancellation_token)?,
            base_iri,
            now: DateTime::now(),
            service_handler,
            custom_functions,
            custom_aggregate_functions,
            run_stats,
        })
    }

    pub fn evaluate_select_driver(
        &self,
        pattern: &QueryExpression,
        substitutions: impl IntoIterator<Item = (Variable, Term)>,
    ) -> (
        Result<IncrementalSelectDriver<'a, D>, QueryEvaluationError>,
        Rc<EvalNodeWithStats>,
    ) {
        let mut variables = Vec::new();
        let mut ids = QueryLocalTermIds::new();
        let plan = self.build_plan(pattern, &mut variables, &mut ids);
        let stats = match &plan {
            Ok(plan) => Rc::clone(&plan.stats),
            Err(_) => Rc::new(EvalNodeWithStats {
                label: eval_node_label(pattern),
                children: Vec::new(),
                exec_count: Cell::new(0),
                exec_duration: Cell::new(self.run_stats.then(DayTimeDuration::default)),
            }),
        };
        let plan = match plan {
            Ok(plan) => plan,
            Err(error) => return (Err(error), stats),
        };
        let output_indices = if let QueryExpression::Project {
            variables: selected,
            ..
        } = pattern
        {
            selected
                .iter()
                .map(|v| encode_variable(&mut variables, v))
                .collect::<Vec<_>>()
        } else {
            (0..variables.len()).collect()
        };
        let output_variables: Vec<_> = output_indices
            .iter()
            .map(|&i| variables[i].clone())
            .collect();
        let bindings = match encode_initial_bindings(&self.dataset, &variables, substitutions) {
            Ok(bindings) => EncodedTuple::from_internal(bindings, &mut ids),
            Err(error) => return (Err(error), stats),
        };
        let notifier = Arc::new(IncrementalQueryNotifier::new());
        let needs_triples = plan.needs_triples();
        let triple_components = Rc::new(RefCell::new(FxHashMap::default()));
        let source =
            Rc::clone(&self.dataset.dataset).incremental_quad_source(Arc::downgrade(&notifier));
        let mut worker = Worker::new(
            timely::WorkerConfig::default(),
            Allocator::Thread(Thread::default()),
            Some(std::time::Instant::now()),
        );
        let output: Rc<RefCell<VecDeque<(EncodedTuple, u64, isize)>>> =
            Rc::new(RefCell::new(VecDeque::new()));
        let observed = Rc::clone(&output);
        let matcher_components = Rc::clone(&triple_components);
        let (quad_input, values_inputs, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (quad_input, quads) = scope.new_collection::<EncodedQuad, isize>();
            let mut values_inputs = Vec::new();
            let result = compile_plan(
                &scope,
                &quads,
                &plan,
                &matcher_components,
                &mut values_inputs,
            )
            .flat_map(move |row| EncodedTuple::combine(&row, &bindings))
            .inspect(move |change| observed.borrow_mut().push_back(change.clone()));
            let (probe, _) = result.probe();
            (quad_input, values_inputs, probe)
        });
        (
            Ok(IncrementalSelectDriver {
                dataset: self.dataset.clone(),
                notifier,
                variables: Arc::from(output_variables),
                output_indices,
                source,
                ids,
                triple_components,
                needs_triples,
                quad_input,
                values_inputs,
                worker,
                probe,
                output,
                pending_output: None,
                epoch: 0,
                initialized: false,
            }),
            stats,
        )
    }

    fn build_plan(
        &self,
        pattern: &QueryExpression,
        variables: &mut Vec<Variable>,
        ids: &mut QueryLocalTermIds<D::InternalTerm>,
    ) -> Result<Plan, QueryEvaluationError> {
        let mut children = Vec::new();
        let kind = match pattern {
            QueryExpression::Values {
                variables: names,
                bindings,
            } => {
                let columns = names
                    .iter()
                    .map(|v| encode_variable(variables, v))
                    .collect::<Vec<_>>();
                let mut rows = Vec::with_capacity(bindings.len());
                for binding in bindings {
                    let mut row = EncodedTuple::empty();
                    for (index, value) in binding.iter().enumerate() {
                        if let Some(value) = value {
                            let term = match value {
                                GroundTerm::NamedNode(node) => self.encode_term(node.clone()),
                                GroundTerm::Literal(literal) => self.encode_term(literal.clone()),
                                #[cfg(feature = "sparql-12")]
                                GroundTerm::Triple(triple) => self.encode_triple(triple),
                            }?;
                            row.set(columns[index], ids.intern(term));
                        }
                    }
                    rows.push(row);
                }
                PlanKind::Values(rows)
            }
            QueryExpression::QuadPattern {
                subject,
                predicate,
                object,
                graph_name,
            } => {
                let subject = TupleSelector::from_term_pattern(subject, variables, &self.dataset)?;
                let predicate =
                    TupleSelector::from_named_node_pattern(predicate, variables, &self.dataset)?;
                let object = TupleSelector::from_term_pattern(object, variables, &self.dataset)?;
                let graph_name = graph_name
                    .as_ref()
                    .map(|g| TupleSelector::from_named_node_pattern(g, variables, &self.dataset))
                    .transpose()?;
                let defaults = self.dataset.specification.default.as_ref().map(|graphs| {
                    graphs
                        .iter()
                        .map(|g| g.as_ref().map(|g| ids.intern(g.clone())))
                        .collect()
                });
                let named = self
                    .dataset
                    .specification
                    .named
                    .as_ref()
                    .map(|graphs| graphs.iter().map(|g| ids.intern(g.clone())).collect());
                PlanKind::Pattern {
                    subject: encode_selector(subject, ids),
                    predicate: encode_selector(predicate, ids),
                    object: encode_selector(object, ids),
                    graph_name: graph_name.map(|g| encode_selector(g, ids)),
                    defaults,
                    named,
                }
            }
            QueryExpression::Join {
                left,
                right,
                algorithm,
            } => {
                let left = self.build_plan(left, variables, ids)?;
                let right = self.build_plan(right, variables, ids)?;
                children.extend([Rc::clone(&left.stats), Rc::clone(&right.stats)]);
                let JoinAlgorithm::HashBuildLeftProbeRight { keys } = algorithm;
                let keys = keys.iter().map(|v| encode_variable(variables, v)).collect();
                PlanKind::Join(Box::new(left), Box::new(right), keys)
            }
            QueryExpression::Project {
                inner,
                variables: selected,
            } => {
                let selected = selected
                    .iter()
                    .map(|v| encode_variable(variables, v))
                    .collect();
                let inner = self.build_plan(inner, variables, ids)?;
                children.push(Rc::clone(&inner.stats));
                PlanKind::Project(Box::new(inner), selected)
            }
            _ => return Err(unsupported_incremental_pattern(pattern)),
        };
        Ok(Plan {
            kind,
            stats: Rc::new(EvalNodeWithStats {
                label: eval_node_label(pattern),
                children,
                exec_count: Cell::new(0),
                exec_duration: Cell::new(self.run_stats.then(DayTimeDuration::default)),
            }),
        })
    }

    fn encode_term(&self, term: impl Into<Term>) -> Result<D::InternalTerm, QueryEvaluationError> {
        self.dataset.internalize_term(term.into())
    }

    #[cfg(feature = "sparql-12")]
    fn encode_triple(
        &self,
        triple: &GroundTriple,
    ) -> Result<D::InternalTerm, QueryEvaluationError> {
        self.dataset.internalize_expression_term(
            ExpressionTriple::from(Triple::from(triple.clone())).into(),
        )
    }
}

impl<'a, D: IncrementalQueryableDataset<'a>> Clone for SimpleIncrementalEvaluator<'a, D> {
    fn clone(&self) -> Self {
        Self {
            dataset: self.dataset.clone(),
            base_iri: self.base_iri.clone(),
            now: self.now,
            service_handler: Rc::clone(&self.service_handler),
            custom_functions: Rc::clone(&self.custom_functions),
            custom_aggregate_functions: Rc::clone(&self.custom_aggregate_functions),
            run_stats: self.run_stats,
        }
    }
}

struct Plan {
    kind: PlanKind,
    stats: Rc<EvalNodeWithStats>,
}

impl Plan {
    fn needs_triples(&self) -> bool {
        match &self.kind {
            PlanKind::Values(_) => false,
            PlanKind::Pattern {
                subject,
                predicate,
                object,
                graph_name,
                ..
            } => {
                subject.needs_triples()
                    || predicate.needs_triples()
                    || object.needs_triples()
                    || graph_name.as_ref().is_some_and(IdSelector::needs_triples)
            }
            PlanKind::Join(left, right, _) => left.needs_triples() || right.needs_triples(),
            PlanKind::Project(inner, _) => inner.needs_triples(),
        }
    }
}

enum PlanKind {
    Values(Vec<EncodedTuple>),
    Pattern {
        subject: IdSelector,
        predicate: IdSelector,
        object: IdSelector,
        graph_name: Option<IdSelector>,
        defaults: Option<Vec<Option<u64>>>,
        named: Option<Vec<u64>>,
    },
    Join(Box<Plan>, Box<Plan>, Vec<usize>),
    Project(Box<Plan>, Vec<usize>),
}

#[derive(Clone, Debug)]
enum IdSelector {
    Constant(u64),
    Variable(usize),
    #[cfg(feature = "sparql-12")]
    Triple(Box<[IdSelector; 3]>),
}

impl IdSelector {
    fn needs_triples(&self) -> bool {
        match self {
            #[cfg(feature = "sparql-12")]
            Self::Triple(_) => true,
            _ => false,
        }
    }

    #[cfg_attr(not(feature = "sparql-12"), expect(unused_variables))]
    fn put(&self, value: u64, row: &mut EncodedTuple, triples: &FxHashMap<u64, [u64; 3]>) -> bool {
        match self {
            Self::Constant(constant) => *constant == value,
            Self::Variable(index) => {
                if let Some(old) = row.values.get(*index).copied().flatten() {
                    old == value
                } else {
                    row.set(*index, value);
                    true
                }
            }
            #[cfg(feature = "sparql-12")]
            Self::Triple(selectors) => triples.get(&value).is_some_and(|components| {
                selectors
                    .iter()
                    .zip(components)
                    .all(|(selector, component)| selector.put(*component, row, triples))
            }),
        }
    }
}

fn encode_selector<T: Clone + Eq + Hash>(
    selector: TupleSelector<T>,
    ids: &mut QueryLocalTermIds<T>,
) -> IdSelector {
    match selector {
        TupleSelector::Constant(term) => IdSelector::Constant(ids.intern(term)),
        TupleSelector::Variable(index) => IdSelector::Variable(index),
        #[cfg(feature = "sparql-12")]
        TupleSelector::TriplePattern(triple) => IdSelector::Triple(Box::new([
            encode_selector(triple.subject.clone(), ids),
            encode_selector(triple.predicate.clone(), ids),
            encode_selector(triple.object.clone(), ids),
        ])),
    }
}

fn compile_plan<'scope>(
    scope: &Scope<'scope, u64>,
    quads: &VecCollection<'scope, u64, EncodedQuad, isize>,
    plan: &Plan,
    triple_components: &Rc<RefCell<FxHashMap<u64, [u64; 3]>>>,
    values_inputs: &mut Vec<(InputSession<u64, EncodedTuple, isize>, Vec<EncodedTuple>)>,
) -> VecCollection<'scope, u64, EncodedTuple, isize> {
    let result = match &plan.kind {
        PlanKind::Values(rows) => {
            let (input, collection) = scope.new_collection();
            values_inputs.push((input, rows.clone()));
            collection
        }
        PlanKind::Pattern {
            subject,
            predicate,
            object,
            graph_name,
            defaults,
            named,
        } => {
            let subject = subject.clone();
            let predicate = predicate.clone();
            let object = object.clone();
            let graph_name = graph_name.clone();
            let defaults = defaults.clone();
            let named = named.clone();
            let triple_components = Rc::clone(triple_components);
            // None means the union of every store graph. The ordinary default
            // graph is represented by Some([None]), not by None.
            let merged =
                graph_name.is_none() && defaults.as_ref().is_none_or(|graphs| graphs.len() > 1);
            let rows = quads.clone().flat_map(move |quad| {
                let triples = triple_components.borrow();
                if let Some(selector) = &graph_name {
                    let graph = quad.graph_name?;
                    if named
                        .as_ref()
                        .is_some_and(|graphs| !graphs.contains(&graph))
                    {
                        return None;
                    }
                    let mut row = EncodedTuple::empty();
                    (selector.put(graph, &mut row, &triples)
                        && subject.put(quad.subject, &mut row, &triples)
                        && predicate.put(quad.predicate, &mut row, &triples)
                        && object.put(quad.object, &mut row, &triples))
                    .then_some(row)
                } else {
                    let included = defaults
                        .as_ref()
                        .is_none_or(|graphs| graphs.contains(&quad.graph_name));
                    if !included {
                        return None;
                    }
                    let mut row = EncodedTuple::empty();
                    (subject.put(quad.subject, &mut row, &triples)
                        && predicate.put(quad.predicate, &mut row, &triples)
                        && object.put(quad.object, &mut row, &triples))
                    .then_some(row)
                }
            });
            if merged { rows.distinct() } else { rows }
        }
        PlanKind::Join(left, right, keys) => {
            let left_keys = keys.clone();
            let right_keys = keys.clone();
            let left = compile_plan(scope, quads, left, triple_components, values_inputs)
                .map(move |row| (row.key(&left_keys), row));
            let right = compile_plan(scope, quads, right, triple_components, values_inputs)
                .map(move |row| (row.key(&right_keys), row));
            left.join_map(right, |_key, left, right| {
                EncodedTuple::combine(left, right)
            })
            .flat_map(|row| row)
        }
        PlanKind::Project(inner, selected) => {
            let selected = selected.clone();
            compile_plan(scope, quads, inner, triple_components, values_inputs)
                .map(move |row| row.project(&selected))
        }
    };
    let stats = Rc::clone(&plan.stats);
    result.inspect(move |change| {
        let count = stats.exec_count.get();
        stats
            .exec_count
            .set(count.saturating_add(change.2.unsigned_abs()));
    })
}

impl<'a, D: IncrementalQueryableDataset<'a>> IncrementalSelectDriver<'a, D> {
    pub fn variables(&self) -> Arc<[Variable]> {
        Arc::clone(&self.variables)
    }

    pub fn change_generation(&self) -> u64 {
        self.notifier.generation()
    }

    pub fn register_waker(&self, waker: &Waker) {
        self.notifier.register_waker(waker);
    }

    pub(crate) fn wait_for_change(&self, baseline: u64) -> NotifierChangeFuture<'_, '_> {
        self.notifier
            .changed_since(baseline, &self.dataset.cancellation_token)
    }

    #[expect(
        clippy::expect_used,
        reason = "a query cannot process u64::MAX update batches"
    )]
    fn flush_epoch(&mut self) {
        let next = self
            .epoch
            .checked_add(1)
            .expect("too many incremental epochs");
        self.quad_input.advance_to(next);
        self.quad_input.flush();
        for (input, _) in &mut self.values_inputs {
            input.advance_to(next);
            input.flush();
        }
        while self.probe.less_than(&next) {
            self.worker.step();
        }
        self.epoch = next;
    }

    fn encode_quad(
        dataset: &D,
        ids: &mut QueryLocalTermIds<D::InternalTerm>,
        triples: &mut FxHashMap<u64, [u64; 3]>,
        needs_triples: bool,
        quad: InternalQuad<D::InternalTerm>,
    ) -> Result<EncodedQuad, D::Error> {
        Ok(EncodedQuad {
            subject: intern_term(dataset, ids, triples, quad.subject, needs_triples)?,
            predicate: intern_term(dataset, ids, triples, quad.predicate, needs_triples)?,
            object: intern_term(dataset, ids, triples, quad.object, needs_triples)?,
            graph_name: quad
                .graph_name
                .map(|g| intern_term(dataset, ids, triples, g, needs_triples))
                .transpose()?,
        })
    }

    fn next_output(&mut self) -> Result<Option<QuerySolutionChange>, QueryEvaluationError> {
        loop {
            if let Some((row, weight)) = &mut self.pending_output {
                if *weight != 0 {
                    let kind = if *weight > 0 {
                        DeltaKind::Addition
                    } else {
                        DeltaKind::Deletion
                    };
                    *weight -= weight.signum();
                    let mut terms = Vec::with_capacity(self.output_indices.len());
                    for index in &self.output_indices {
                        let term = row
                            .values
                            .get(*index)
                            .copied()
                            .flatten()
                            .map(|id| self.dataset.externalize_term(self.ids.term(id)))
                            .transpose()?;
                        terms.push(term);
                    }
                    return Ok(Some(Delta::with_kind(
                        kind,
                        (Arc::clone(&self.variables), terms).into(),
                    )));
                }
                self.pending_output = None;
            }
            let Some((row, _, weight)) = self.output.borrow_mut().pop_front() else {
                return Ok(None);
            };
            self.pending_output = Some((row, weight));
        }
    }

    pub fn poll_next(
        &mut self,
    ) -> Result<IncrementalDriverState<QuerySolutionChange>, QueryEvaluationError> {
        loop {
            self.dataset.cancellation_token.ensure_alive()?;
            if let Some(change) = self.next_output()? {
                return Ok(IncrementalDriverState::Item(change));
            }
            if !self.initialized {
                let input = &mut self.quad_input;
                let ids = &mut self.ids;
                let dataset = &self.dataset.dataset;
                let cancellation_token = &self.dataset.cancellation_token;
                let mut triples = self.triple_components.borrow_mut();
                let needs_triples = self.needs_triples;
                self.source.snapshot(&mut |quad| {
                    cancellation_token.ensure_alive()?;
                    input.update(
                        Self::encode_quad(dataset, ids, &mut triples, needs_triples, quad)
                            .map_err(|e| QueryEvaluationError::Dataset(Box::new(e)))?,
                        1,
                    );
                    Ok(())
                })?;
                for (input, rows) in &mut self.values_inputs {
                    for row in rows {
                        input.update(row.clone(), 1);
                    }
                }
                drop(triples);
                self.initialized = true;
                self.flush_epoch();
                continue;
            }
            let Some(batch) = self
                .source
                .next_batch()
                .map_err(|e| QueryEvaluationError::Dataset(Box::new(e)))?
            else {
                return Ok(if self.source.is_live() {
                    IncrementalDriverState::Pending
                } else {
                    IncrementalDriverState::Done
                });
            };
            for delta in batch {
                let (kind, quad) = delta.into_parts();
                let encoded = Self::encode_quad(
                    &self.dataset.dataset,
                    &mut self.ids,
                    &mut self.triple_components.borrow_mut(),
                    self.needs_triples,
                    quad,
                )
                .map_err(|e| QueryEvaluationError::Dataset(Box::new(e)))?;
                self.quad_input.update(
                    encoded,
                    if matches!(kind, DeltaKind::Addition) {
                        1
                    } else {
                        -1
                    },
                );
            }
            self.flush_epoch();
        }
    }

    pub fn drain_ready(
        &mut self,
        limit: Option<usize>,
    ) -> Result<IncrementalDriverState<Vec<QuerySolutionChange>>, QueryEvaluationError> {
        let mut results = Vec::new();
        loop {
            if limit.is_some_and(|limit| results.len() >= limit) {
                return Ok(IncrementalDriverState::Item(results));
            }
            match self.poll_next()? {
                IncrementalDriverState::Item(item) => results.push(item),
                IncrementalDriverState::Pending if results.is_empty() => {
                    return Ok(IncrementalDriverState::Pending);
                }
                IncrementalDriverState::Done if results.is_empty() => {
                    return Ok(IncrementalDriverState::Done);
                }
                IncrementalDriverState::Pending | IncrementalDriverState::Done => {
                    return Ok(IncrementalDriverState::Item(results));
                }
            }
        }
    }
}

#[cfg_attr(
    not(feature = "sparql-12"),
    expect(
        unused_variables,
        clippy::needless_pass_by_value,
        clippy::unnecessary_wraps
    )
)]
fn intern_term<'a, D: IncrementalQueryableDataset<'a>>(
    dataset: &D,
    ids: &mut QueryLocalTermIds<D::InternalTerm>,
    triples: &mut FxHashMap<u64, [u64; 3]>,
    term: D::InternalTerm,
    needs_triples: bool,
) -> Result<u64, D::Error> {
    let id = ids.intern(term.clone());
    #[cfg(feature = "sparql-12")]
    if needs_triples
        && !triples.contains_key(&id)
        && let ExpressionTerm::Triple(triple) = dataset.externalize_expression_term(term)?
    {
        let subject = dataset.internalize_expression_term(triple.subject.into())?;
        let predicate = dataset.internalize_expression_term(triple.predicate.into())?;
        let object = dataset.internalize_expression_term(triple.object)?;
        let subject = intern_term(dataset, ids, triples, subject, true)?;
        let predicate = intern_term(dataset, ids, triples, predicate, true)?;
        let object = intern_term(dataset, ids, triples, object, true)?;
        triples.insert(id, [subject, predicate, object]);
    }
    Ok(id)
}

struct QueryLocalTermIds<T> {
    by_term: FxHashMap<T, u64>,
    by_id: Vec<T>,
}

impl<T: Clone + Eq + Hash> QueryLocalTermIds<T> {
    fn new() -> Self {
        Self {
            by_term: FxHashMap::default(),
            by_id: Vec::new(),
        }
    }

    fn intern(&mut self, term: T) -> u64 {
        if let Some(id) = self.by_term.get(&term) {
            return *id;
        }
        let id = self.by_id.len() as u64;
        self.by_term.insert(term.clone(), id);
        self.by_id.push(term);
        id
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "IDs originate from usize indices"
    )]
    fn term(&self, id: u64) -> T {
        self.by_id[id as usize].clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
struct EncodedQuad {
    subject: u64,
    predicate: u64,
    object: u64,
    graph_name: Option<u64>,
}

#[derive(
    Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
)]
struct EncodedTuple {
    values: Vec<Option<u64>>,
}

impl EncodedTuple {
    fn empty() -> Self {
        Self { values: Vec::new() }
    }

    fn from_internal<T: Clone + Eq + Hash>(
        tuple: super::InternalTuple<T>,
        ids: &mut QueryLocalTermIds<T>,
    ) -> Self {
        Self {
            values: tuple
                .inner
                .into_iter()
                .map(|v| v.map(|v| ids.intern(v)))
                .collect(),
        }
    }

    fn set(&mut self, index: usize, value: u64) {
        if self.values.len() <= index {
            self.values.resize(index + 1, None);
        }
        self.values[index] = Some(value);
    }

    fn key(&self, keys: &[usize]) -> Vec<Option<u64>> {
        keys.iter()
            .map(|i| self.values.get(*i).copied().flatten())
            .collect()
    }

    fn combine(left: &Self, right: &Self) -> Option<Self> {
        let mut result = left.clone();
        for (index, value) in right.values.iter().enumerate() {
            if let Some(value) = value {
                match result.values.get(index).copied().flatten() {
                    Some(old) if old != *value => return None,
                    Some(_) => {}
                    None => result.set(index, *value),
                }
            }
        }
        Some(result)
    }

    fn project(mut self, selected: &[usize]) -> Self {
        for (index, value) in self.values.iter_mut().enumerate() {
            if !selected.contains(&index) {
                *value = None;
            }
        }
        self
    }
}

fn unsupported_incremental_pattern(pattern: &QueryExpression) -> QueryEvaluationError {
    QueryEvaluationError::Unexpected(Box::new(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("incremental SELECT core does not support {pattern:?} yet"),
    )))
}
