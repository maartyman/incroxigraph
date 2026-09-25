use super::{
    EvalDataset, EvalNodeWithStats, InternalTuple, Timer, TupleSelector, encode_initial_bindings,
    encode_variable, eval_node_label, put_pattern_value,
};
#[cfg(feature = "sparql-12")]
use crate::ExpressionTriple;
use crate::batch::{BatchedFilterMap, BatchedMap};
use crate::expression::CustomFunctionRegistry;
use crate::service::ServiceHandlerRegistry;
use crate::{
    BatchedIterator, CancellationToken, CustomAggregateFunctionRegistry, Delta, DeltaKind,
    IncrementalQueryableDataset, InternalQuad, IteratorBatched, QueryDatasetSpecification,
    QueryEvaluationError, QuerySolution,
};
use oxiri::Iri;
#[cfg(feature = "sparql-12")]
use oxrdf::Triple;
use oxrdf::{Term, Variable};
use oxsdatatypes::{DateTime, DayTimeDuration};
use oxstr::OxString;
use rustc_hash::{FxHashMap, FxHasher};
use spargebra::term::GroundTerm;
#[cfg(feature = "sparql-12")]
use spargebra::term::GroundTriple;
use sparopt::algebra::{JoinAlgorithm, QueryExpression};
use std::cell::Cell;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::iter::{empty, once};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

/// Item returned by a streaming incremental iterator.
///
/// `Pending` means the iterator is live but has no item available right now.
/// `None` from the standard `Iterator::next` means the iterator is permanently
/// exhausted.
pub enum StreamingItem<T> {
    Item(T),
    Pending,
}

type InternalTupleDeltaEvaluator<'a, T> =
    Rc<dyn Fn(InternalTuple<T>) -> InternalTupleDeltasIterator<'a, T> + 'a>;

type InternalTupleDeltasIterator<'a, T> = Box<
    dyn BatchedIterator<Item = Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>>
        + 'a,
>;

const DEFAULT_BATCH_SIZE: usize = 128;

fn is_streaming_boundary<T, E>(item: &Result<StreamingItem<T>, E>) -> bool {
    matches!(item, Ok(StreamingItem::Pending))
}

pub type QuerySolutionChange = Delta<QuerySolution>;

pub enum IncrementalDriverState<T> {
    Item(T),
    Pending,
    Done,
}

/// Query-local notification state shared weakly with its live dataset subscriptions.
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
        let waker = self.waker.lock().unwrap().take();
        if let Some(waker) = waker {
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

/// Future that resolves when a query notifier observes a change after `baseline`.
pub(crate) struct NotifierChangeFuture<'n, 'c> {
    notifier: &'n IncrementalQueryNotifier,
    baseline: u64,
    cancellation_token: &'c CancellationToken,
    cancellation_waker: Option<Waker>,
}

impl Future for NotifierChangeFuture<'_, '_> {
    type Output = Result<(), QueryEvaluationError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), QueryEvaluationError>> {
        let this = self.get_mut();
        this.cancellation_token.ensure_alive()?;
        if this.notifier.generation() != this.baseline {
            return Poll::Ready(Ok(()));
        }
        this.notifier.register_waker(cx.waker());
        if let Some(old_waker) = &this.cancellation_waker
            && !old_waker.will_wake(cx.waker())
        {
            this.cancellation_token.unregister_waker(old_waker);
        }
        this.cancellation_token.register_waker(cx.waker());
        this.cancellation_waker = Some(cx.waker().clone());
        this.cancellation_token.ensure_alive()?;
        if this.notifier.generation() != this.baseline {
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
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

impl<'a, D: IncrementalQueryableDataset<'a>> EvalDataset<'a, D> {
    fn underlying_internal_quad_deltas_for_pattern(
        &self,
        subject: Option<&D::InternalTerm>,
        predicate: Option<&D::InternalTerm>,
        object: Option<&D::InternalTerm>,
        graph_name: Option<Option<&D::InternalTerm>>,
        notifier: Weak<IncrementalQueryNotifier>,
    ) -> impl BatchedIterator<
        Item = Result<StreamingItem<Delta<InternalQuad<D::InternalTerm>>>, QueryEvaluationError>,
    > + use<'a, D> {
        let cancellation_token = self.cancellation_token.clone();
        BatchedMap::new(
            self.dataset
                .internal_quad_deltas_for_pattern(subject, predicate, object, graph_name, notifier),
            move |r: Result<StreamingItem<Delta<InternalQuad<D::InternalTerm>>>, D::Error>| {
                cancellation_token.ensure_alive()?;
                r.map_err(|e| QueryEvaluationError::Dataset(Box::new(e)))
            },
            is_streaming_boundary,
        )
    }

    fn internal_quad_deltas_for_pattern(
        &self,
        subject: Option<&D::InternalTerm>,
        predicate: Option<&D::InternalTerm>,
        object: Option<&D::InternalTerm>,
        graph_name: Option<Option<&D::InternalTerm>>,
        notifier: Weak<IncrementalQueryNotifier>,
    ) -> Box<
        dyn BatchedIterator<
                Item = Result<
                    StreamingItem<Delta<InternalQuad<D::InternalTerm>>>,
                    QueryEvaluationError,
                >,
            > + 'a,
    > {
        if let Some(graph_name) = graph_name {
            if let Some(graph_name) = graph_name {
                if self
                    .specification
                    .named
                    .as_ref()
                    .is_none_or(|d| d.contains(graph_name))
                {
                    Box::new(self.underlying_internal_quad_deltas_for_pattern(
                        subject,
                        predicate,
                        object,
                        Some(Some(graph_name)),
                        notifier,
                    ))
                } else {
                    Box::new(IteratorBatched::new(empty()))
                }
            } else if let Some(default_graph_graphs) = &self.specification.default {
                if default_graph_graphs.len() == 1 {
                    Box::new(BatchedMap::new(
                        self.underlying_internal_quad_deltas_for_pattern(
                            subject,
                            predicate,
                            object,
                            Some(default_graph_graphs[0].as_ref()),
                            notifier,
                        ),
                        |quad: Result<
                            StreamingItem<Delta<InternalQuad<D::InternalTerm>>>,
                            QueryEvaluationError,
                        >| {
                            (|| {
                                let quad = quad?;
                                Ok(match quad {
                                    StreamingItem::Item(mut quad) => {
                                        quad.value_mut().graph_name = None;
                                        StreamingItem::Item(quad)
                                    }
                                    StreamingItem::Pending => StreamingItem::Pending,
                                })
                            })()
                        },
                        is_streaming_boundary,
                    ))
                } else {
                    let iters = default_graph_graphs
                        .iter()
                        .map(
                            |graph_name| -> InternalQuadDeltaIterator<'a, D::InternalTerm> {
                                Box::new(self.underlying_internal_quad_deltas_for_pattern(
                                    subject,
                                    predicate,
                                    object,
                                    Some(graph_name.as_ref()),
                                    Weak::clone(&notifier),
                                ))
                            },
                        )
                        .collect::<Vec<_>>();
                    Box::new(DeduplicatingDefaultGraphIterator::new(BatchedMap::new(
                        RoundRobinStreamingIterator::new(iters),
                        |quad: Result<
                            StreamingItem<Delta<InternalQuad<D::InternalTerm>>>,
                            QueryEvaluationError,
                        >| {
                            (|| {
                                let quad = quad?;
                                Ok(match quad {
                                    StreamingItem::Item(mut quad) => {
                                        quad.value_mut().graph_name = None;
                                        StreamingItem::Item(quad)
                                    }
                                    StreamingItem::Pending => StreamingItem::Pending,
                                })
                            })()
                        },
                        is_streaming_boundary,
                    )))
                }
            } else {
                Box::new(DeduplicatingDefaultGraphIterator::new(BatchedMap::new(
                    self.underlying_internal_quad_deltas_for_pattern(
                        subject, predicate, object, None, notifier,
                    ),
                    |quad: Result<
                        StreamingItem<Delta<InternalQuad<D::InternalTerm>>>,
                        QueryEvaluationError,
                    >| {
                        (|| {
                            let quad = quad?;
                            Ok(match quad {
                                StreamingItem::Item(mut quad) => {
                                    quad.value_mut().graph_name = None;
                                    StreamingItem::Item(quad)
                                }
                                StreamingItem::Pending => StreamingItem::Pending,
                            })
                        })()
                    },
                    is_streaming_boundary,
                )))
            }
        } else if let Some(named_graphs) = &self.specification.named {
            let iters = named_graphs
                .iter()
                .map(
                    |graph_name| -> InternalQuadDeltaIterator<'a, D::InternalTerm> {
                        Box::new(self.underlying_internal_quad_deltas_for_pattern(
                            subject,
                            predicate,
                            object,
                            Some(Some(graph_name)),
                            Weak::clone(&notifier),
                        ))
                    },
                )
                .collect::<Vec<_>>();
            Box::new(RoundRobinStreamingIterator::new(iters))
        } else {
            Box::new(BatchedFilterMap::new(
                self.underlying_internal_quad_deltas_for_pattern(
                    subject, predicate, object, None, notifier,
                ),
                |q: Result<
                    StreamingItem<Delta<InternalQuad<D::InternalTerm>>>,
                    QueryEvaluationError,
                >| {
                    (!q.as_ref().is_ok_and(|q| {
                        matches!(
                            q,
                            StreamingItem::Item(quad_delta)
                                if quad_delta.value().graph_name.is_none()
                        )
                    }))
                    .then_some(q)
                },
                is_streaming_boundary,
            ))
        }
    }
}

type InternalQuadDeltaIterator<'a, T> = Box<
    dyn BatchedIterator<Item = Result<StreamingItem<Delta<InternalQuad<T>>>, QueryEvaluationError>>
        + 'a,
>;

/// Polls each live source once per round, so a pending source cannot starve the others.
struct RoundRobinStreamingIterator<'a, T> {
    iterators: Vec<Option<InternalQuadDeltaIterator<'a, T>>>,
    inputs: Vec<Vec<Result<StreamingItem<Delta<InternalQuad<T>>>, QueryEvaluationError>>>,
    next: usize,
}

impl<'a, T> RoundRobinStreamingIterator<'a, T> {
    fn new(iterators: Vec<InternalQuadDeltaIterator<'a, T>>) -> Self {
        let len = iterators.len();
        Self {
            iterators: iterators.into_iter().map(Some).collect(),
            inputs: (0..len).map(|_| Vec::new()).collect(),
            next: 0,
        }
    }
}

impl<T> BatchedIterator for RoundRobinStreamingIterator<'_, T> {
    type Item = Result<StreamingItem<Delta<InternalQuad<T>>>, QueryEvaluationError>;

    fn next_batch(&mut self, output: &mut Vec<Self::Item>, batch_size: usize) -> usize {
        let initial_len = output.len();
        while output.len() - initial_len < batch_size {
            if self.iterators.is_empty() {
                break;
            }
            let mut live = false;
            let mut result = None;
            for _ in 0..self.iterators.len() {
                let index = self.next;
                self.next = (self.next + 1) % self.iterators.len();
                let Some(iterator) = &mut self.iterators[index] else {
                    continue;
                };
                if self.inputs[index].is_empty() {
                    if iterator.next_batch(&mut self.inputs[index], 1) == 0 {
                        self.iterators[index] = None;
                        continue;
                    }
                    self.inputs[index].reverse();
                }
                match self.inputs[index].pop() {
                    Some(Ok(StreamingItem::Pending)) => live = true,
                    Some(item) => {
                        result = Some(item);
                        break;
                    }
                    None => self.iterators[index] = None,
                }
            }
            if let Some(result) = result {
                output.push(result);
            } else if live {
                output.push(Ok(StreamingItem::Pending));
                break;
            } else {
                break;
            }
        }
        output.len() - initial_len
    }
}

/// Turns a union of RDF graphs into an RDF graph by retaining one copy of each triple.
struct DeduplicatingDefaultGraphIterator<I, T> {
    inner: I,
    counts: FxHashMap<InternalQuad<T>, usize>,
    input: Vec<Result<StreamingItem<Delta<InternalQuad<T>>>, QueryEvaluationError>>,
}

impl<I, T> DeduplicatingDefaultGraphIterator<I, T> {
    fn new(inner: I) -> Self {
        Self {
            inner,
            counts: FxHashMap::default(),
            input: Vec::new(),
        }
    }
}

impl<I, T> BatchedIterator for DeduplicatingDefaultGraphIterator<I, T>
where
    I: BatchedIterator<Item = Result<StreamingItem<Delta<InternalQuad<T>>>, QueryEvaluationError>>,
    T: Clone + Eq + Hash,
{
    type Item = Result<StreamingItem<Delta<InternalQuad<T>>>, QueryEvaluationError>;

    fn next_batch(&mut self, output: &mut Vec<Self::Item>, batch_size: usize) -> usize {
        let initial_len = output.len();
        while output.len() - initial_len < batch_size {
            if self.input.is_empty() {
                let remaining = batch_size - (output.len() - initial_len);
                if self.inner.next_batch(&mut self.input, remaining) == 0 {
                    break;
                }
                self.input.reverse();
            }
            while let Some(item) = self.input.pop() {
                let delta = match item {
                    Ok(StreamingItem::Pending) => {
                        output.push(Ok(StreamingItem::Pending));
                        return output.len() - initial_len;
                    }
                    Err(error) => {
                        output.push(Err(error));
                        continue;
                    }
                    Ok(StreamingItem::Item(delta)) => delta,
                };
                match delta {
                    Delta::Addition(quad) => {
                        let count = self.counts.entry(quad.clone()).or_default();
                        *count += 1;
                        if *count == 1 {
                            output.push(Ok(StreamingItem::Item(Delta::Addition(quad))));
                        }
                    }
                    Delta::Deletion(quad) => match self.counts.get_mut(&quad) {
                        Some(count) if *count > 1 => *count -= 1,
                        Some(_) => {
                            self.counts.remove(&quad);
                            output.push(Ok(StreamingItem::Item(Delta::Deletion(quad))));
                        }
                        None => output.push(Err(non_existing_tuple_deleted())),
                    },
                }
                if output.len() - initial_len >= batch_size {
                    break;
                }
            }
        }
        output.len() - initial_len
    }
}

struct IncrementalDriverCore<'a, D: IncrementalQueryableDataset<'a>> {
    dataset: EvalDataset<'a, D>,
    notifier: Arc<IncrementalQueryNotifier>,
}

impl<'a, D: IncrementalQueryableDataset<'a>> IncrementalDriverCore<'a, D> {
    fn change_generation(&self) -> u64 {
        self.notifier.generation()
    }

    fn register_waker(&self, waker: &Waker) {
        self.notifier.register_waker(waker);
    }
}

pub struct IncrementalSelectDriver<'a, D: IncrementalQueryableDataset<'a>> {
    core: IncrementalDriverCore<'a, D>,
    variables: Arc<[Variable]>,
    iter: InternalTupleDeltasIterator<'a, D::InternalTerm>,
    input: Vec<Result<StreamingItem<Delta<InternalTuple<D::InternalTerm>>>, QueryEvaluationError>>,
}

pub struct SimpleIncrementalEvaluator<'a, D: IncrementalQueryableDataset<'a>> {
    dataset: EvalDataset<'a, D>,
    notifier: Arc<IncrementalQueryNotifier>,
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
            notifier: Arc::new(IncrementalQueryNotifier::new()),
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
        let notifier = Arc::new(IncrementalQueryNotifier::new());
        let evaluator = Self {
            dataset: self.dataset.clone(),
            notifier: Arc::clone(&notifier),
            base_iri: self.base_iri.clone(),
            now: self.now,
            service_handler: Rc::clone(&self.service_handler),
            custom_functions: Rc::clone(&self.custom_functions),
            custom_aggregate_functions: Rc::clone(&self.custom_aggregate_functions),
            run_stats: self.run_stats,
        };
        let mut variables = Vec::new();
        let (eval, stats) = evaluator.graph_pattern_evaluator(pattern, &mut variables);
        let eval = match eval {
            Ok(e) => e,
            Err(e) => return (Err(e), stats),
        };
        let from = match encode_initial_bindings(&evaluator.dataset, &variables, substitutions) {
            Ok(from) => from,
            Err(e) => return (Err(e), stats),
        };
        (
            Ok(IncrementalSelectDriver {
                core: IncrementalDriverCore {
                    dataset: evaluator.dataset.clone(),
                    notifier,
                },
                variables: Arc::from(variables),
                iter: eval(from),
                input: Vec::with_capacity(DEFAULT_BATCH_SIZE),
            }),
            stats,
        )
    }

    fn graph_pattern_evaluator(
        &self,
        pattern: &QueryExpression,
        encoded_variables: &mut Vec<Variable>,
    ) -> (
        Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError>,
        Rc<EvalNodeWithStats>,
    ) {
        let mut stat_children = Vec::new();
        let evaluator =
            self.build_graph_pattern_evaluator(pattern, encoded_variables, &mut stat_children);
        let stats = Rc::new(EvalNodeWithStats {
            label: eval_node_label(pattern),
            children: stat_children,
            exec_count: Cell::new(0),
            exec_duration: Cell::new(self.run_stats.then(DayTimeDuration::default)),
        });
        let mut evaluator = match evaluator {
            Ok(e) => e,
            Err(e) => return (Err(e), stats),
        };
        if self.run_stats {
            let stats = Rc::clone(&stats);
            evaluator = Rc::new(move |tuple| {
                let start = Timer::now();
                let inner = evaluator(tuple);
                let duration = start.elapsed();
                stats.exec_duration.set(
                    stats
                        .exec_duration
                        .get()
                        .and_then(|d| d.checked_add(duration?)),
                );
                Box::new(StatsDeltaIterator {
                    inner,
                    stats: Rc::clone(&stats),
                })
            })
        }
        (Ok(evaluator), stats)
    }

    fn build_graph_pattern_evaluator(
        &self,
        pattern: &QueryExpression,
        encoded_variables: &mut Vec<Variable>,
        stat_children: &mut Vec<Rc<EvalNodeWithStats>>,
    ) -> Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError> {
        Ok(match pattern {
            QueryExpression::Values {
                variables,
                bindings,
            } => {
                let encoding = variables
                    .iter()
                    .map(|v| encode_variable(encoded_variables, v))
                    .collect::<Vec<_>>();
                let encoded_tuples = bindings
                    .iter()
                    .map(|row| {
                        let mut result = InternalTuple::with_capacity(variables.len());
                        for (key, value) in row.iter().enumerate() {
                            if let Some(term) = value {
                                result.set(
                                    encoding[key],
                                    match term {
                                        GroundTerm::NamedNode(node) => {
                                            self.encode_term(node.clone())
                                        }
                                        GroundTerm::Literal(literal) => {
                                            self.encode_term(literal.clone())
                                        }
                                        #[cfg(feature = "sparql-12")]
                                        GroundTerm::Triple(triple) => self.encode_triple(triple),
                                    }?,
                                );
                            }
                        }
                        Ok(result)
                    })
                    .collect::<Result<Vec<_>, QueryEvaluationError>>()?;
                Rc::new(move |from| {
                    Box::new(IteratorBatched::new(
                        encoded_tuples
                            .iter()
                            .filter_map(move |t| from.combine_with(t))
                            .map(|tuple| Ok(StreamingItem::Item(Delta::addition(tuple))))
                            .collect::<Vec<_>>()
                            .into_iter(),
                    ))
                })
            }
            QueryExpression::QuadPattern {
                subject,
                predicate,
                object,
                graph_name,
            } => {
                let subject_selector =
                    TupleSelector::from_term_pattern(subject, encoded_variables, &self.dataset)?;
                let predicate_selector = TupleSelector::from_named_node_pattern(
                    predicate,
                    encoded_variables,
                    &self.dataset,
                )?;
                let object_selector =
                    TupleSelector::from_term_pattern(object, encoded_variables, &self.dataset)?;
                let graph_name_selector = if let Some(graph_name) = graph_name.as_ref() {
                    Some(TupleSelector::from_named_node_pattern(
                        graph_name,
                        encoded_variables,
                        &self.dataset,
                    )?)
                } else {
                    None
                };
                let dataset = self.dataset.clone();
                let notifier = Arc::downgrade(&self.notifier);
                Rc::new(move |from| {
                    let input_subject = match subject_selector.get_pattern_value(
                        &from,
                        #[cfg(feature = "sparql-12")]
                        &dataset,
                    ) {
                        Ok(value) => value,
                        Err(e) => return Box::new(IteratorBatched::new(once(Err(e)))),
                    };
                    let input_predicate = match predicate_selector.get_pattern_value(
                        &from,
                        #[cfg(feature = "sparql-12")]
                        &dataset,
                    ) {
                        Ok(value) => value,
                        Err(e) => return Box::new(IteratorBatched::new(once(Err(e)))),
                    };
                    let input_object = match object_selector.get_pattern_value(
                        &from,
                        #[cfg(feature = "sparql-12")]
                        &dataset,
                    ) {
                        Ok(value) => value,
                        Err(e) => return Box::new(IteratorBatched::new(once(Err(e)))),
                    };
                    let input_graph_name = if let Some(graph_name_selector) = &graph_name_selector {
                        match graph_name_selector.get_pattern_value(
                            &from,
                            #[cfg(feature = "sparql-12")]
                            &dataset,
                        ) {
                            Ok(value) => value,
                            Err(e) => return Box::new(IteratorBatched::new(once(Err(e)))),
                        }
                        .map(Some)
                    } else {
                        Some(from.graph_name.clone()) // default graph
                    };
                    let iter = dataset.internal_quad_deltas_for_pattern(
                        input_subject.as_ref(),
                        input_predicate.as_ref(),
                        input_object.as_ref(),
                        input_graph_name.as_ref().map(|g| g.as_ref()),
                        Weak::clone(&notifier),
                    );
                    let subject_selector = subject_selector.clone();
                    let predicate_selector = predicate_selector.clone();
                    let object_selector = object_selector.clone();
                    let graph_name_selector = graph_name_selector.clone();
                    #[cfg(feature = "sparql-12")]
                    let dataset = dataset.clone();
                    Box::new(BatchedFilterMap::new(
                        iter,
                        move |quad: Result<
                            StreamingItem<Delta<InternalQuad<D::InternalTerm>>>,
                            QueryEvaluationError,
                        >| {
                            let result: Result<
                                Option<StreamingItem<Delta<InternalTuple<D::InternalTerm>>>>,
                                QueryEvaluationError,
                            > = (|| match quad? {
                                StreamingItem::Pending => Ok(Some(StreamingItem::Pending)),
                                StreamingItem::Item(quad) => {
                                    let (kind, quad) = quad.into_parts();
                                    let mut new_tuple = from.clone();
                                    if !put_pattern_value::<D>(
                                        &subject_selector,
                                        quad.subject,
                                        &mut new_tuple,
                                        #[cfg(feature = "sparql-12")]
                                        &dataset,
                                    )? {
                                        return Ok(None);
                                    }
                                    if !put_pattern_value::<D>(
                                        &predicate_selector,
                                        quad.predicate,
                                        &mut new_tuple,
                                        #[cfg(feature = "sparql-12")]
                                        &dataset,
                                    )? {
                                        return Ok(None);
                                    }
                                    if !put_pattern_value::<D>(
                                        &object_selector,
                                        quad.object,
                                        &mut new_tuple,
                                        #[cfg(feature = "sparql-12")]
                                        &dataset,
                                    )? {
                                        return Ok(None);
                                    }
                                    if let Some(graph_name_selector) = &graph_name_selector {
                                        let Some(quad_graph_name) = quad.graph_name else {
                                            return Err(
                                                QueryEvaluationError::UnexpectedDefaultGraph,
                                            );
                                        };
                                        if !put_pattern_value::<D>(
                                            graph_name_selector,
                                            quad_graph_name,
                                            &mut new_tuple,
                                            #[cfg(feature = "sparql-12")]
                                            &dataset,
                                        )? {
                                            return Ok(None);
                                        }
                                    }
                                    Ok(Some(StreamingItem::Item(Delta::with_kind(kind, new_tuple))))
                                }
                            })();
                            result.transpose()
                        },
                        is_streaming_boundary,
                    ))
                })
            }
            QueryExpression::Path { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::Graph { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::Join {
                left,
                right,
                algorithm,
            } => {
                let (left, left_stats) = self.graph_pattern_evaluator(left, encoded_variables);
                stat_children.push(left_stats);
                let (right, right_stats) = self.graph_pattern_evaluator(right, encoded_variables);
                stat_children.push(right_stats);
                let left = left?;
                let right = right?;
                match algorithm {
                    JoinAlgorithm::HashBuildLeftProbeRight { keys } => {
                        if keys.is_empty() {
                            Rc::new(move |from| {
                                Box::new(SymmetricNestedLoopJoinIterator {
                                    left_iter: left(from.clone()),
                                    right_iter: right(from),
                                    left_input: Vec::with_capacity(DEFAULT_BATCH_SIZE),
                                    right_input: Vec::with_capacity(DEFAULT_BATCH_SIZE),
                                    left_values: Vec::new(),
                                    right_values: Vec::new(),
                                    buffered_results: Vec::new(),
                                    left_done: false,
                                    right_done: false,
                                    active_side: JoinSide::Left,
                                })
                            })
                        } else {
                            let keys = keys
                                .iter()
                                .map(|v| encode_variable(encoded_variables, v))
                                .collect::<Vec<_>>();
                            Rc::new(move |from| {
                                let left_iter = left(from.clone());
                                let right_iter = right(from);
                                let mut left_values = InternalTupleSet::new(keys.clone());
                                let mut right_values = InternalTupleSet::new(keys.clone());
                                left_values.reserve(left_iter.size_hint().0);
                                right_values.reserve(right_iter.size_hint().0);
                                Box::new(SymmetricHashJoinIterator {
                                    left_iter,
                                    right_iter,
                                    left_input: Vec::with_capacity(DEFAULT_BATCH_SIZE),
                                    right_input: Vec::with_capacity(DEFAULT_BATCH_SIZE),
                                    left_values,
                                    right_values,
                                    left_done: false,
                                    right_done: false,
                                    active_side: JoinSide::Left,
                                })
                            })
                        }
                    }
                }
            }
            #[cfg(feature = "sep-0006")]
            QueryExpression::Lateral { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::Minus { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::LeftJoin { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::Filter { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::Union { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::Extend { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::OrderBy { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::Distinct { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::Reduced { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::Slice { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::Project { inner, variables } => {
                let mut inner_encoded_variables = variables.clone();
                let (child, child_stats) =
                    self.graph_pattern_evaluator(inner, &mut inner_encoded_variables);
                stat_children.push(child_stats);
                let child = child?;
                let mapping = variables
                    .iter()
                    .enumerate()
                    .map(|(new_variable, variable)| {
                        (new_variable, encode_variable(encoded_variables, variable))
                    })
                    .collect::<Rc<[(usize, usize)]>>();
                Rc::new(move |from| {
                    let mapping = Rc::clone(&mapping);
                    let mut input_tuple = InternalTuple::with_capacity(mapping.len());
                    for (input_key, output_key) in &*mapping {
                        if let Some(value) = from.get(*output_key) {
                            input_tuple.set(*input_key, value.clone());
                        }
                    }
                    input_tuple.graph_name.clone_from(&from.graph_name);
                    Box::new(BatchedFilterMap::new(
                        child(input_tuple),
                        move |change: Result<
                            StreamingItem<Delta<InternalTuple<D::InternalTerm>>>,
                            QueryEvaluationError,
                        >| {
                            match change {
                                Ok(StreamingItem::Item(delta)) => {
                                    let (kind, tuple) = delta.into_parts();
                                    let mut output_tuple = from.clone();
                                    for (input_key, output_key) in &*mapping {
                                        if let Some(value) = tuple.get(*input_key) {
                                            if let Some(existing_value) =
                                                output_tuple.get(*output_key)
                                            {
                                                if existing_value != value {
                                                    return None; // Conflict
                                                }
                                            } else {
                                                output_tuple.set(*output_key, value.clone());
                                            }
                                        }
                                    }
                                    Some(Ok(StreamingItem::Item(Delta::with_kind(
                                        kind,
                                        output_tuple,
                                    ))))
                                }
                                Ok(StreamingItem::Pending) => Some(Ok(StreamingItem::Pending)),
                                Err(error) => Some(Err(error)),
                            }
                        },
                        is_streaming_boundary,
                    ))
                })
            }
            QueryExpression::Group { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
            QueryExpression::Service { .. } => {
                return Err(unsupported_incremental_pattern(pattern));
            }
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
            notifier: Arc::clone(&self.notifier),
            base_iri: self.base_iri.clone(),
            now: self.now,
            service_handler: Rc::clone(&self.service_handler),
            custom_functions: Rc::clone(&self.custom_functions),
            custom_aggregate_functions: Rc::clone(&self.custom_aggregate_functions),
            run_stats: self.run_stats,
        }
    }
}

impl<'a, D: IncrementalQueryableDataset<'a>> IncrementalSelectDriver<'a, D> {
    /// The variables produced by the underlying SELECT query, in projection order.
    pub fn variables(&self) -> Arc<[Variable]> {
        Arc::clone(&self.variables)
    }

    /// A monotonically increasing counter of changes observed by this query.
    pub fn change_generation(&self) -> u64 {
        self.core.change_generation()
    }

    /// Registers `waker` to be notified when this query observes a dataset change.
    pub fn register_waker(&self, waker: &Waker) {
        self.core.register_waker(waker);
    }

    pub(crate) fn wait_for_change(&self, baseline: u64) -> NotifierChangeFuture<'_, '_> {
        self.core
            .notifier
            .changed_since(baseline, &self.core.dataset.cancellation_token)
    }

    pub fn poll_next(
        &mut self,
    ) -> Result<IncrementalDriverState<QuerySolutionChange>, QueryEvaluationError> {
        if self.input.is_empty() {
            self.iter.next_batch(&mut self.input, DEFAULT_BATCH_SIZE);
            self.input.reverse();
        }
        match self.input.pop() {
            Some(Ok(StreamingItem::Item(delta))) => {
                let (kind, tuple) = delta.into_parts();
                let mut result = vec![None; self.variables.len()];
                for (index, value) in tuple.iter().enumerate() {
                    if let Some(term) = value {
                        result[index] = Some(self.core.dataset.externalize_term(term)?);
                    }
                }
                Ok(IncrementalDriverState::Item(Delta::with_kind(
                    kind,
                    (Arc::clone(&self.variables), result).into(),
                )))
            }
            Some(Ok(StreamingItem::Pending)) => Ok(IncrementalDriverState::Pending),
            Some(Err(error)) => Err(error),
            None => Ok(IncrementalDriverState::Done),
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

struct SymmetricHashJoinIterator<'a, T> {
    left_iter: InternalTupleDeltasIterator<'a, T>,
    right_iter: InternalTupleDeltasIterator<'a, T>,
    left_input: Vec<Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>>,
    right_input: Vec<Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>>,
    left_values: InternalTupleSet<T>,
    right_values: InternalTupleSet<T>,
    left_done: bool,
    right_done: bool,
    active_side: JoinSide,
}

impl<T: Clone + Eq + Hash> BatchedIterator for SymmetricHashJoinIterator<'_, T> {
    type Item = Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>;

    fn next_batch(&mut self, output: &mut Vec<Self::Item>, batch_size: usize) -> usize {
        let initial_len = output.len();
        while output.len() < batch_size {
            if self.left_done && self.right_done {
                break;
            }

            let first_state = self.poll_active_side(output, batch_size);
            self.active_side = self.active_side.other();
            let second_state = self.poll_active_side(output, batch_size);

            if self.left_done && self.right_done {
                break;
            }
            if matches!(first_state, PollState::Pending | PollState::Done)
                && matches!(second_state, PollState::Pending | PollState::Done)
            {
                output.push(Ok(StreamingItem::Pending));
                break;
            }
        }
        output.len() - initial_len
    }
}

impl<T: Clone + Eq + Hash> SymmetricHashJoinIterator<'_, T> {
    fn poll_active_side(
        &mut self,
        output: &mut Vec<Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>>,
        batch_size: usize,
    ) -> PollState {
        let (iter, values, input, other_values, done) = match self.active_side {
            JoinSide::Left => (
                &mut self.left_iter,
                &mut self.left_values,
                &mut self.left_input,
                &self.right_values,
                &mut self.left_done,
            ),
            JoinSide::Right => (
                &mut self.right_iter,
                &mut self.right_values,
                &mut self.right_input,
                &self.left_values,
                &mut self.right_done,
            ),
        };
        if *done {
            return PollState::Done;
        }

        let output_start = output.len();
        let mut pending_values = Vec::new();
        let mut made_progress = false;
        while output.len() < batch_size {
            if input.is_empty() && iter.next_batch(input, batch_size - output.len()) == 0 {
                *done = true;
                break;
            }

            let keys = input
                .iter()
                .take_while(|item| matches!(item, Ok(StreamingItem::Item(_))))
                .map(|item| match item {
                    Ok(StreamingItem::Item(delta)) => other_values.tuple_key(delta.value()),
                    _ => unreachable!("the delta prefix contains only tuple deltas"),
                })
                .collect::<Vec<_>>();
            let delta_count = keys.len();
            output.reserve(keys.iter().map(|key| other_values.get(*key).len()).sum());
            pending_values.reserve(delta_count);
            for (key, item) in keys.into_iter().zip(input.drain(..delta_count)) {
                let Ok(StreamingItem::Item(delta)) = item else {
                    unreachable!("the delta prefix contains only tuple deltas");
                };
                let kind = delta.kind();
                output.extend(
                    other_values
                        .get(key)
                        .iter()
                        .filter_map(|other| delta.value().combine_with(other))
                        .map(move |joined| Ok(StreamingItem::Item(Delta::with_kind(kind, joined)))),
                );
                pending_values.push((key, delta));
            }
            made_progress |= delta_count != 0;

            if input.is_empty() {
                continue;
            }
            let boundary = input
                .drain(..1)
                .next()
                .expect("the input buffer is non-empty");
            match boundary {
                Ok(StreamingItem::Pending) => break,
                Err(error) => {
                    output.push(Err(error));
                    break;
                }
                Ok(StreamingItem::Item(_)) => {
                    unreachable!("all leading tuple deltas were consumed");
                }
            }
        }

        values.reserve(pending_values.len());
        for (key, delta) in pending_values {
            match delta {
                Delta::Addition(tuple) => values.insert(key, tuple),
                Delta::Deletion(tuple) if !values.remove_one(key, &tuple) => {
                    output.push(Err(non_existing_tuple_deleted()));
                }
                Delta::Deletion(_) => {}
            }
        }

        if made_progress || output.len() != output_start {
            PollState::Item
        } else if *done {
            PollState::Done
        } else {
            PollState::Pending
        }
    }
}

#[derive(Clone, Copy)]
enum JoinSide {
    Left,
    Right,
}

impl JoinSide {
    fn other(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

struct SymmetricNestedLoopJoinIterator<'a, T> {
    left_iter: InternalTupleDeltasIterator<'a, T>,
    right_iter: InternalTupleDeltasIterator<'a, T>,
    left_input: Vec<Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>>,
    right_input: Vec<Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>>,
    left_values: Vec<InternalTuple<T>>,
    right_values: Vec<InternalTuple<T>>,
    buffered_results: Vec<Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>>,
    left_done: bool,
    right_done: bool,
    active_side: JoinSide,
}

impl<T: Clone + Eq> SymmetricNestedLoopJoinIterator<'_, T> {
    fn poll_active_side(&mut self) -> PollState {
        let active_side = self.active_side;
        let (iter, input, values, other_values, done) = match active_side {
            JoinSide::Left => (
                &mut self.left_iter,
                &mut self.left_input,
                &mut self.left_values,
                &self.right_values,
                &mut self.left_done,
            ),
            JoinSide::Right => (
                &mut self.right_iter,
                &mut self.right_input,
                &mut self.right_values,
                &self.left_values,
                &mut self.right_done,
            ),
        };
        if *done {
            return PollState::Done;
        }

        if input.is_empty() {
            iter.next_batch(input, DEFAULT_BATCH_SIZE);
            input.reverse();
        }
        match input.pop() {
            Some(Ok(StreamingItem::Item(delta))) => {
                let (kind, tuple) = delta.into_parts();
                let joined = |other: &InternalTuple<T>| match active_side {
                    JoinSide::Left => tuple.combine_with(other),
                    JoinSide::Right => other.combine_with(&tuple),
                };
                match kind {
                    DeltaKind::Addition => {
                        self.buffered_results
                            .extend(other_values.iter().filter_map(joined).map(move |joined| {
                                Ok(StreamingItem::Item(Delta::with_kind(kind, joined)))
                            }));
                        values.push(tuple);
                    }
                    DeltaKind::Deletion => {
                        if let Some(position) = values.iter().position(|value| value == &tuple) {
                            values.swap_remove(position);
                            self.buffered_results.extend(
                                other_values.iter().filter_map(joined).map(move |joined| {
                                    Ok(StreamingItem::Item(Delta::with_kind(kind, joined)))
                                }),
                            );
                        }
                    }
                }
                PollState::Item
            }
            Some(Ok(StreamingItem::Pending)) => PollState::Pending,
            Some(Err(error)) => {
                self.buffered_results.push(Err(error));
                PollState::Item
            }
            None => {
                *done = true;
                PollState::Done
            }
        }
    }
}

impl<T: Clone + Eq> BatchedIterator for SymmetricNestedLoopJoinIterator<'_, T> {
    type Item = Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>;

    fn next_batch(&mut self, output: &mut Vec<Self::Item>, batch_size: usize) -> usize {
        let initial_len = output.len();
        loop {
            while output.len() - initial_len < batch_size {
                let Some(result) = self.buffered_results.pop() else {
                    break;
                };
                output.push(result);
            }
            if output.len() - initial_len >= batch_size || (self.left_done && self.right_done) {
                break;
            }

            let first_state = self.poll_active_side();
            if !self.buffered_results.is_empty() {
                continue;
            }

            self.active_side = self.active_side.other();
            let second_state = self.poll_active_side();
            if !self.buffered_results.is_empty() {
                continue;
            }

            if self.left_done && self.right_done {
                break;
            }
            if matches!(first_state, PollState::Pending | PollState::Done)
                && matches!(second_state, PollState::Pending | PollState::Done)
            {
                output.push(Ok(StreamingItem::Pending));
                break;
            }
        }
        output.len() - initial_len
    }
}

enum PollState {
    Item,
    Pending,
    Done,
}

struct InternalTupleSet<T> {
    key: Vec<usize>,
    map: FxHashMap<u64, Vec<InternalTuple<T>>>,
    len: usize,
}

impl<T> InternalTupleSet<T> {
    fn new(key: Vec<usize>) -> Self {
        Self {
            key,
            map: FxHashMap::default(),
            len: 0,
        }
    }
}

impl<T: Hash + Eq> InternalTupleSet<T> {
    fn reserve(&mut self, additional: usize) {
        self.map.reserve(additional);
    }

    fn insert(&mut self, key: u64, tuple: InternalTuple<T>) {
        self.map.entry(key).or_default().push(tuple);
        self.len += 1;
    }

    fn get(&self, key: u64) -> &[InternalTuple<T>] {
        self.map.get(&key).map_or(&[], |v| v)
    }

    fn tuple_key(&self, tuple: &InternalTuple<T>) -> u64 {
        let mut hasher = FxHasher::default();
        for v in &self.key {
            if let Some(val) = tuple.get(*v) {
                val.hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    fn remove_one(&mut self, key: u64, tuple: &InternalTuple<T>) -> bool {
        let Some(values) = self.map.get_mut(&key) else {
            return false;
        };
        let Some(position) = values.iter().position(|candidate| candidate == tuple) else {
            return false;
        };

        values.swap_remove(position);
        self.len -= 1;
        if values.is_empty() {
            self.map.remove(&key);
        }
        true
    }
}

struct StatsDeltaIterator<'a, T> {
    inner: InternalTupleDeltasIterator<'a, T>,
    stats: Rc<EvalNodeWithStats>,
}

impl<T> BatchedIterator for StatsDeltaIterator<'_, T> {
    type Item = Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>;

    fn next_batch(&mut self, output: &mut Vec<Self::Item>, batch_size: usize) -> usize {
        let initial_len = output.len();
        let start = Timer::now();
        let count = self.inner.next_batch(output, batch_size);
        if let Some(duration) = start.elapsed() {
            self.stats.exec_duration.set(
                self.stats
                    .exec_duration
                    .get()
                    .and_then(|d| d.checked_add(duration)),
            );
        }
        self.stats.exec_count.set(
            self.stats.exec_count.get()
                + output[initial_len..]
                    .iter()
                    .filter(|result| matches!(result, Ok(StreamingItem::Item(_))))
                    .count(),
        );
        count
    }
}

fn unsupported_incremental_pattern(pattern: &QueryExpression) -> QueryEvaluationError {
    QueryEvaluationError::Unexpected(Box::new(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("incremental SELECT core does not support {pattern:?} yet"),
    )))
}

fn non_existing_tuple_deleted() -> QueryEvaluationError {
    QueryEvaluationError::Unexpected(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "A tuple was deleted before it was added",
    )))
}
