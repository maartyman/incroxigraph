use super::{
    EvalDataset, EvalNodeWithStats, InternalBooleanExpressionEvaluator, InternalTuple,
    SimpleEvaluator, Timer, TupleSelector, are_compatible_and_not_disjointed,
    encode_initial_bindings, encode_projection, encode_variable, eval_node_label,
    project_input_tuple, project_output_tuple, put_pattern_value,
};
use crate::{
    CancellationToken, Delta, DeltaKind, IncrementalQueryableDataset, InternalQuad,
    QueryEvaluationError, QuerySolution,
};
use oxrdf::{Term, Variable};
use oxsdatatypes::DayTimeDuration;
use rustc_hash::FxHashMap;
use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern};
use sparopt::algebra::{
    Expression, JoinAlgorithm, LeftJoinAlgorithm, MinusAlgorithm, QueryExpression,
};
use sparopt::does_contain_exists;
use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
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

type InternalTupleDeltasIterator<'a, T> = DeltaIterator<'a, InternalTuple<T>>;
type DeltaIterator<'a, T> =
    Box<dyn Iterator<Item = Result<StreamingItem<Delta<T>>, QueryEvaluationError>> + 'a>;

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
    ) -> impl Iterator<
        Item = Result<StreamingItem<Delta<InternalQuad<D::InternalTerm>>>, QueryEvaluationError>,
    > + use<'a, D> {
        let cancellation_token = self.cancellation_token.clone();
        self.dataset
            .internal_quad_deltas_for_pattern(subject, predicate, object, graph_name, notifier)
            .map(move |r| {
                cancellation_token.ensure_alive()?;
                r.map_err(|e| QueryEvaluationError::Dataset(Box::new(e)))
            })
    }

    fn internal_quad_deltas_for_pattern(
        &self,
        subject: Option<&D::InternalTerm>,
        predicate: Option<&D::InternalTerm>,
        object: Option<&D::InternalTerm>,
        graph_name: Option<Option<&D::InternalTerm>>,
        notifier: Weak<IncrementalQueryNotifier>,
    ) -> Box<
        dyn Iterator<
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
                    Box::new(empty())
                }
            } else if let Some(default_graph_graphs) = &self.specification.default {
                if default_graph_graphs.len() == 1 {
                    Box::new(
                        self.underlying_internal_quad_deltas_for_pattern(
                            subject,
                            predicate,
                            object,
                            Some(default_graph_graphs[0].as_ref()),
                            notifier,
                        )
                        .map(|quad| {
                            let quad = quad?;
                            Ok(match quad {
                                StreamingItem::Item(mut quad) => {
                                    quad.value_mut().graph_name = None;
                                    StreamingItem::Item(quad)
                                }
                                StreamingItem::Pending => StreamingItem::Pending,
                            })
                        }),
                    )
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
                    Box::new(DeduplicatingDefaultGraphIterator::new(
                        RoundRobinStreamingIterator::new(iters).map(|quad| {
                            let quad = quad?;
                            Ok(match quad {
                                StreamingItem::Item(mut quad) => {
                                    quad.value_mut().graph_name = None;
                                    StreamingItem::Item(quad)
                                }
                                StreamingItem::Pending => StreamingItem::Pending,
                            })
                        }),
                    ))
                }
            } else {
                Box::new(DeduplicatingDefaultGraphIterator::new(
                    self.underlying_internal_quad_deltas_for_pattern(
                        subject, predicate, object, None, notifier,
                    )
                    .map(|quad| {
                        let quad = quad?;
                        Ok(match quad {
                            StreamingItem::Item(mut quad) => {
                                quad.value_mut().graph_name = None;
                                StreamingItem::Item(quad)
                            }
                            StreamingItem::Pending => StreamingItem::Pending,
                        })
                    }),
                ))
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
            Box::new(
                self.underlying_internal_quad_deltas_for_pattern(
                    subject, predicate, object, None, notifier,
                )
                .filter(|q| {
                    !q.as_ref().is_ok_and(|q| {
                        matches!(
                            q,
                            StreamingItem::Item(quad_delta)
                                if quad_delta.value().graph_name.is_none()
                        )
                    })
                }),
            )
        }
    }
}

type InternalQuadDeltaIterator<'a, T> = DeltaIterator<'a, InternalQuad<T>>;

/// Polls each live source once per round, so a pending source cannot starve the others.
struct RoundRobinStreamingIterator<'a, T> {
    iterators: Vec<Option<DeltaIterator<'a, T>>>,
    next: usize,
}

impl<'a, T> RoundRobinStreamingIterator<'a, T> {
    fn new(iterators: Vec<DeltaIterator<'a, T>>) -> Self {
        Self {
            iterators: iterators.into_iter().map(Some).collect(),
            next: 0,
        }
    }
}

impl<T> Iterator for RoundRobinStreamingIterator<'_, T> {
    type Item = Result<StreamingItem<Delta<T>>, QueryEvaluationError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iterators.is_empty() {
            return None;
        }
        let mut live = false;
        for _ in 0..self.iterators.len() {
            let index = self.next;
            self.next = (self.next + 1) % self.iterators.len();
            let Some(iterator) = &mut self.iterators[index] else {
                continue;
            };
            match iterator.next() {
                Some(Ok(StreamingItem::Pending)) => live = true,
                Some(item) => return Some(item),
                None => self.iterators[index] = None,
            }
        }
        live.then_some(Ok(StreamingItem::Pending))
    }
}

/// Turns a union of RDF graphs into an RDF graph by retaining one copy of each triple.
struct DeduplicatingDefaultGraphIterator<I, T> {
    inner: I,
    counts: FxHashMap<InternalQuad<T>, usize>,
}

impl<I, T> DeduplicatingDefaultGraphIterator<I, T> {
    fn new(inner: I) -> Self {
        Self {
            inner,
            counts: FxHashMap::default(),
        }
    }
}

impl<I, T> Iterator for DeduplicatingDefaultGraphIterator<I, T>
where
    I: Iterator<Item = Result<StreamingItem<Delta<InternalQuad<T>>>, QueryEvaluationError>>,
    T: Clone + Eq + Hash,
{
    type Item = Result<StreamingItem<Delta<InternalQuad<T>>>, QueryEvaluationError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.inner.next()?;
            let delta = match item {
                Ok(StreamingItem::Pending) => return Some(Ok(StreamingItem::Pending)),
                Err(error) => return Some(Err(error)),
                Ok(StreamingItem::Item(delta)) => delta,
            };
            match delta {
                Delta::Addition(quad) => {
                    let count = self.counts.entry(quad.clone()).or_default();
                    *count += 1;
                    if *count == 1 {
                        return Some(Ok(StreamingItem::Item(Delta::Addition(quad))));
                    }
                }
                Delta::Deletion(quad) => match self.counts.get_mut(&quad) {
                    Some(count) if *count > 1 => *count -= 1,
                    Some(_) => {
                        self.counts.remove(&quad);
                        return Some(Ok(StreamingItem::Item(Delta::Deletion(quad))));
                    }
                    None => return Some(Err(non_existing_tuple_deleted())),
                },
            }
        }
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

/// Low-level driver for incremental SELECT changes.
///
/// After any polling error, stop using and drop the driver. Polling again has no
/// recovery guarantee, including after cancellation.
pub struct IncrementalSelectDriver<'a, D: IncrementalQueryableDataset<'a>> {
    core: IncrementalDriverCore<'a, D>,
    variables: Arc<[Variable]>,
    iter: InternalTupleDeltasIterator<'a, D::InternalTerm>,
}

pub struct SimpleIncrementalEvaluator<'a, D: IncrementalQueryableDataset<'a>> {
    evaluator: SimpleEvaluator<'a, D>,
    notifier: Arc<IncrementalQueryNotifier>,
}

impl<'a, D: IncrementalQueryableDataset<'a>> SimpleIncrementalEvaluator<'a, D> {
    pub fn new(evaluator: SimpleEvaluator<'a, D>) -> Self {
        Self {
            evaluator,
            notifier: Arc::new(IncrementalQueryNotifier::new()),
        }
    }

    pub fn evaluate_select_driver(
        self,
        query_expression: &QueryExpression,
        substitutions: impl IntoIterator<Item = (Variable, Term)>,
    ) -> (
        Result<IncrementalSelectDriver<'a, D>, QueryEvaluationError>,
        Rc<EvalNodeWithStats>,
    ) {
        let mut variables = Vec::new();
        let (eval, stats) = self.query_expression_evaluator(query_expression, &mut variables);
        let eval = match eval {
            Ok(e) => e,
            Err(e) => return (Err(e), stats),
        };
        let from = match encode_initial_bindings(&self.evaluator.dataset, &variables, substitutions)
        {
            Ok(from) => from,
            Err(e) => return (Err(e), stats),
        };
        (
            Ok(IncrementalSelectDriver {
                core: IncrementalDriverCore {
                    dataset: self.evaluator.dataset,
                    notifier: self.notifier,
                },
                variables: Arc::from(variables),
                iter: eval(from),
            }),
            stats,
        )
    }

    fn query_expression_evaluator(
        &self,
        query_expression: &QueryExpression,
        encoded_variables: &mut Vec<Variable>,
    ) -> (
        Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError>,
        Rc<EvalNodeWithStats>,
    ) {
        let mut stat_children = Vec::new();
        let evaluator = self.build_query_expression_evaluator(
            query_expression,
            encoded_variables,
            &mut stat_children,
        );
        let stats = Rc::new(EvalNodeWithStats {
            label: eval_node_label(query_expression),
            children: stat_children,
            exec_count: Cell::new(0),
            exec_duration: Cell::new(self.evaluator.run_stats.then(DayTimeDuration::default)),
        });
        let mut evaluator = match evaluator {
            Ok(e) => e,
            Err(e) => return (Err(e), stats),
        };
        if self.evaluator.run_stats {
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

    fn build_query_expression_evaluator(
        &self,
        query_expression: &QueryExpression,
        encoded_variables: &mut Vec<Variable>,
        stat_children: &mut Vec<Rc<EvalNodeWithStats>>,
    ) -> Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError> {
        match query_expression {
            QueryExpression::Values {
                variables,
                bindings,
            } => self.values_evaluator(variables, bindings, encoded_variables),
            QueryExpression::QuadPattern {
                subject,
                predicate,
                object,
                graph_name,
            } => self.quad_pattern_evaluator(
                subject,
                predicate,
                object,
                graph_name.as_ref(),
                encoded_variables,
            ),
            QueryExpression::Join {
                left,
                right,
                algorithm,
            } => self.join_evaluator(left, right, algorithm, encoded_variables, stat_children),
            #[cfg(feature = "sep-0006")]
            QueryExpression::Lateral { .. } => {
                Err(unsupported_incremental_pattern(query_expression))
            }
            QueryExpression::Minus {
                left,
                right,
                algorithm,
            } => self.minus_evaluator(left, right, algorithm, encoded_variables, stat_children),
            QueryExpression::Filter { expression, .. }
            | QueryExpression::LeftJoin { expression, .. }
                if does_contain_exists(expression) =>
            {
                Err(unsupported_incremental_pattern(query_expression))
            }
            QueryExpression::LeftJoin {
                left,
                right,
                expression,
                algorithm,
            } => self.left_join_evaluator(
                left,
                right,
                expression,
                algorithm,
                encoded_variables,
                stat_children,
            ),
            QueryExpression::Filter { inner, expression } => {
                self.filter_evaluator(inner, expression, encoded_variables, stat_children)
            }
            QueryExpression::Union { inner } => {
                self.union_evaluator(inner, encoded_variables, stat_children)
            }
            QueryExpression::Project { inner, variables } => {
                self.project_evaluator(inner, variables, encoded_variables, stat_children)
            }
            QueryExpression::Path { .. }
            | QueryExpression::Graph { .. }
            | QueryExpression::Extend { .. }
            | QueryExpression::OrderBy { .. }
            | QueryExpression::Distinct { .. }
            | QueryExpression::Reduced { .. }
            | QueryExpression::Slice { .. }
            | QueryExpression::Group { .. }
            | QueryExpression::Service { .. } => {
                Err(unsupported_incremental_pattern(query_expression))
            }
        }
    }

    fn values_evaluator(
        &self,
        variables: &[Variable],
        bindings: &[Vec<Option<GroundTerm>>],
        encoded_variables: &mut Vec<Variable>,
    ) -> Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError> {
        let evaluator = self
            .evaluator
            .values_evaluator(variables, bindings, encoded_variables)?;
        Ok(Rc::new(move |from| {
            Box::new(
                evaluator(from)
                    .map(|tuple| tuple.map(|tuple| StreamingItem::Item(Delta::addition(tuple)))),
            )
        }))
    }

    fn quad_pattern_evaluator(
        &self,
        subject: &TermPattern,
        predicate: &NamedNodePattern,
        object: &TermPattern,
        graph_name: Option<&NamedNodePattern>,
        encoded_variables: &mut Vec<Variable>,
    ) -> Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError> {
        let dataset = &self.evaluator.dataset;
        let subject_selector =
            TupleSelector::from_term_pattern(subject, encoded_variables, dataset)?;
        let predicate_selector =
            TupleSelector::from_named_node_pattern(predicate, encoded_variables, dataset)?;
        let object_selector = TupleSelector::from_term_pattern(object, encoded_variables, dataset)?;
        let graph_name_selector = if let Some(graph_name) = graph_name {
            Some(TupleSelector::from_named_node_pattern(
                graph_name,
                encoded_variables,
                dataset,
            )?)
        } else {
            None
        };
        let dataset = dataset.clone();
        let notifier = Arc::downgrade(&self.notifier);
        Ok(Rc::new(move |from| {
            let input_subject = match subject_selector.get_pattern_value(
                &from,
                #[cfg(feature = "sparql-12")]
                &dataset,
            ) {
                Ok(value) => value,
                Err(e) => return Box::new(once(Err(e))),
            };
            let input_predicate = match predicate_selector.get_pattern_value(
                &from,
                #[cfg(feature = "sparql-12")]
                &dataset,
            ) {
                Ok(value) => value,
                Err(e) => return Box::new(once(Err(e))),
            };
            let input_object = match object_selector.get_pattern_value(
                &from,
                #[cfg(feature = "sparql-12")]
                &dataset,
            ) {
                Ok(value) => value,
                Err(e) => return Box::new(once(Err(e))),
            };
            let input_graph_name = if let Some(graph_name_selector) = &graph_name_selector {
                match graph_name_selector.get_pattern_value(
                    &from,
                    #[cfg(feature = "sparql-12")]
                    &dataset,
                ) {
                    Ok(value) => value,
                    Err(e) => return Box::new(once(Err(e))),
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
            Box::new(
                iter.map(move |quad| match quad? {
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
                                return Err(QueryEvaluationError::UnexpectedDefaultGraph);
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
                })
                .filter_map(Result::transpose),
            )
        }))
    }

    fn join_evaluator(
        &self,
        left: &QueryExpression,
        right: &QueryExpression,
        algorithm: &JoinAlgorithm,
        encoded_variables: &mut Vec<Variable>,
        stat_children: &mut Vec<Rc<EvalNodeWithStats>>,
    ) -> Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError> {
        let (left, left_stats) = self.query_expression_evaluator(left, encoded_variables);
        stat_children.push(left_stats);
        let (right, right_stats) = self.query_expression_evaluator(right, encoded_variables);
        stat_children.push(right_stats);
        let left = left?;
        let right = right?;
        match algorithm {
            JoinAlgorithm::HashBuildLeftProbeRight { keys } => {
                if keys.is_empty() {
                    Ok(Rc::new(move |from| {
                        Box::new(SymmetricNestedLoopJoinIterator {
                            inputs: JoinInputs::new(left(from.clone()), right(from)),
                            left_values: Vec::new(),
                            right_values: Vec::new(),
                            buffered_results: Vec::new(),
                        })
                    }))
                } else {
                    let keys = keys
                        .iter()
                        .map(|v| encode_variable(encoded_variables, v))
                        .collect::<Vec<_>>();
                    Ok(Rc::new(move |from| {
                        let left_iter = left(from.clone());
                        let right_iter = right(from);
                        let mut left_values = InternalTupleSet::new(keys.clone());
                        let mut right_values = InternalTupleSet::new(keys.clone());
                        left_values.reserve(left_iter.size_hint().0);
                        right_values.reserve(right_iter.size_hint().0);
                        Box::new(SymmetricHashJoinIterator {
                            inputs: JoinInputs::new(left_iter, right_iter),
                            left_values,
                            right_values,
                            buffered_results: Vec::new(),
                        })
                    }))
                }
            }
        }
    }

    fn minus_evaluator(
        &self,
        left: &QueryExpression,
        right: &QueryExpression,
        algorithm: &MinusAlgorithm,
        encoded_variables: &mut Vec<Variable>,
        stat_children: &mut Vec<Rc<EvalNodeWithStats>>,
    ) -> Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError> {
        let (left, left_stats) = self.query_expression_evaluator(left, encoded_variables);
        stat_children.push(left_stats);
        let (right, right_stats) = self.query_expression_evaluator(right, encoded_variables);
        stat_children.push(right_stats);
        let left = left?;
        let right = right?;

        match algorithm {
            MinusAlgorithm::HashBuildRightProbeLeft { keys } => {
                let keys = keys
                    .iter()
                    .map(|v| encode_variable(encoded_variables, v))
                    .collect::<Vec<_>>();
                Ok(Rc::new(move |from| {
                    Box::new(MinusIterator {
                        inputs: JoinInputs::new(left(from.clone()), right(from)),
                        left_values: InternalTupleSet::new(keys.clone()),
                        right_values: InternalTupleSet::new(keys.clone()),
                        buffered_results: Vec::new(),
                    })
                }))
            }
        }
    }

    fn left_join_evaluator(
        &self,
        left: &QueryExpression,
        right: &QueryExpression,
        expression: &Expression,
        algorithm: &LeftJoinAlgorithm,
        encoded_variables: &mut Vec<Variable>,
        stat_children: &mut Vec<Rc<EvalNodeWithStats>>,
    ) -> Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError> {
        let (left, left_stats) = self.query_expression_evaluator(left, encoded_variables);
        stat_children.push(left_stats);
        let (right, right_stats) = self.query_expression_evaluator(right, encoded_variables);
        stat_children.push(right_stats);
        let left = left?;
        let right = right?;
        let expression = self
            .evaluator
            .effective_boolean_value_expression_evaluator(
                expression,
                encoded_variables,
                stat_children,
            )?;

        match algorithm {
            LeftJoinAlgorithm::HashBuildRightProbeLeft { keys } => {
                let keys = keys
                    .iter()
                    .map(|v| encode_variable(encoded_variables, v))
                    .collect::<Vec<_>>();
                Ok(Rc::new(move |from| {
                    Box::new(SymmetricHashLeftJoinIterator {
                        inputs: JoinInputs::new(left(from.clone()), right(from)),
                        expression: Rc::clone(&expression),
                        left_values: InternalTupleSet::new(keys.clone()),
                        right_values: InternalTupleSet::new(keys.clone()),
                        buffered_results: VecDeque::new(),
                    })
                }))
            }
        }
    }

    fn filter_evaluator(
        &self,
        inner: &QueryExpression,
        expression: &Expression,
        encoded_variables: &mut Vec<Variable>,
        stat_children: &mut Vec<Rc<EvalNodeWithStats>>,
    ) -> Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError> {
        let (child, child_stats) = self.query_expression_evaluator(inner, encoded_variables);
        stat_children.push(child_stats);
        let child = child?;
        let expression = self
            .evaluator
            .effective_boolean_value_expression_evaluator(
                expression,
                encoded_variables,
                stat_children,
            )?;
        Ok(Rc::new(move |from| {
            let expression = Rc::clone(&expression);
            Box::new(child(from).filter_map(move |item| match item {
                Ok(StreamingItem::Item(delta)) => match expression(delta.value()) {
                    Ok(Some(true)) => Some(Ok(StreamingItem::Item(delta))),
                    Ok(Some(false) | None) => None,
                    Err(error) => Some(Err(error)),
                },
                other => Some(other),
            }))
        }))
    }

    fn union_evaluator(
        &self,
        inner: &[QueryExpression],
        encoded_variables: &mut Vec<Variable>,
        stat_children: &mut Vec<Rc<EvalNodeWithStats>>,
    ) -> Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError> {
        let children = inner
            .iter()
            .map(|child| {
                let (child, child_stats) =
                    self.query_expression_evaluator(child, encoded_variables);
                stat_children.push(child_stats);
                child
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Rc::new(move |from| {
            Box::new(RoundRobinStreamingIterator::new(
                children.iter().map(|child| child(from.clone())).collect(),
            ))
        }))
    }

    fn project_evaluator(
        &self,
        inner: &QueryExpression,
        variables: &[Variable],
        encoded_variables: &mut Vec<Variable>,
        stat_children: &mut Vec<Rc<EvalNodeWithStats>>,
    ) -> Result<InternalTupleDeltaEvaluator<'a, D::InternalTerm>, QueryEvaluationError> {
        let mut inner_encoded_variables = variables.to_vec();
        let (child, child_stats) =
            self.query_expression_evaluator(inner, &mut inner_encoded_variables);
        stat_children.push(child_stats);
        let child = child?;
        let mapping = encode_projection(variables, encoded_variables);
        Ok(Rc::new(move |from| {
            let mapping = Rc::clone(&mapping);
            let input_tuple = project_input_tuple(&from, &mapping);
            Box::new(child(input_tuple).filter_map(move |item| match item {
                Ok(StreamingItem::Item(delta)) => {
                    let (kind, tuple) = delta.into_parts();
                    let tuple = project_output_tuple(&from, &tuple, &mapping)?;
                    Some(Ok(StreamingItem::Item(Delta::with_kind(kind, tuple))))
                }
                other => Some(other),
            }))
        }))
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

    /// Polls for the next change. Drop the driver if this returns an error.
    pub fn poll_next(
        &mut self,
    ) -> Result<IncrementalDriverState<QuerySolutionChange>, QueryEvaluationError> {
        match self.iter.next() {
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

type JoinInputItem<T> =
    Result<StreamingItem<(JoinSide, Delta<InternalTuple<T>>)>, QueryEvaluationError>;

struct JoinInputs<'a, T> {
    left: InternalTupleDeltasIterator<'a, T>,
    right: InternalTupleDeltasIterator<'a, T>,
    left_done: bool,
    right_done: bool,
    next_side: JoinSide,
    staged: Option<(JoinSide, Delta<InternalTuple<T>>)>,
    right_initialized: bool,
}

impl<'a, T> JoinInputs<'a, T> {
    fn new(
        left: InternalTupleDeltasIterator<'a, T>,
        right: InternalTupleDeltasIterator<'a, T>,
    ) -> Self {
        Self {
            left,
            right,
            left_done: false,
            right_done: false,
            next_side: JoinSide::Left,
            staged: None,
            right_initialized: false,
        }
    }

    fn poll_side(&mut self, side: JoinSide) -> Option<JoinInputItem<T>> {
        let (iter, done) = match side {
            JoinSide::Left => (&mut self.left, &mut self.left_done),
            JoinSide::Right => (&mut self.right, &mut self.right_done),
        };
        if *done {
            return None;
        }
        match iter.next() {
            Some(Ok(StreamingItem::Item(delta))) => Some(Ok(StreamingItem::Item((side, delta)))),
            Some(Ok(StreamingItem::Pending)) => Some(Ok(StreamingItem::Pending)),
            Some(Err(error)) => Some(Err(error)),
            None => {
                *done = true;
                None
            }
        }
    }

    fn next(&mut self, left_empty: bool, right_empty: bool) -> Option<JoinInputItem<T>> {
        if (self.left_done && left_empty)
            || (self.right_done && right_empty)
            || (self.left_done && self.right_done)
        {
            return None;
        }

        if let Some(side) = self.staged.as_ref().map(|(side, _)| *side) {
            // A tuple from the formerly empty side must see all earlier
            // changes on the deferred side before it can produce matches.
            match self.poll_side(side.other()) {
                Some(Ok(StreamingItem::Item(item))) => {
                    return Some(Ok(StreamingItem::Item(item)));
                }
                Some(Err(error)) => return Some(Err(error)),
                Some(Ok(StreamingItem::Pending)) | None => {
                    return Some(Ok(StreamingItem::Item(self.staged.take()?)));
                }
            }
        }

        if left_empty || right_empty {
            let side = if left_empty {
                JoinSide::Left
            } else {
                JoinSide::Right
            };
            match self.poll_side(side) {
                Some(Ok(StreamingItem::Item(item))) => {
                    self.staged = Some(item);
                    return self.next(left_empty, right_empty);
                }
                Some(other) => return Some(other),
                None => return None,
            }
        }

        self.next_ready()
    }

    fn next_left_preserving(&mut self) -> Option<JoinInputItem<T>> {
        // Read the right side up to its first Pending or end before polling the left side.
        // This avoids emitting unmatched left rows that the initial right rows would retract.
        if !self.right_initialized {
            match self.poll_side(JoinSide::Right) {
                Some(Ok(StreamingItem::Pending)) | None => self.right_initialized = true,
                item => return item,
            }
        }
        self.next_ready()
    }

    fn next_ready(&mut self) -> Option<JoinInputItem<T>> {
        let side = self.next_side;
        self.next_side = side.other();
        match self.poll_side(side) {
            Some(Ok(StreamingItem::Item(item))) => return Some(Ok(StreamingItem::Item(item))),
            Some(Err(error)) => return Some(Err(error)),
            Some(Ok(StreamingItem::Pending)) | None => {}
        }
        match self.poll_side(side.other()) {
            Some(Ok(StreamingItem::Item(item))) => Some(Ok(StreamingItem::Item(item))),
            Some(Err(error)) => Some(Err(error)),
            Some(Ok(StreamingItem::Pending)) | None if self.left_done && self.right_done => None,
            Some(Ok(StreamingItem::Pending)) | None => Some(Ok(StreamingItem::Pending)),
        }
    }
}

struct SymmetricHashJoinIterator<'a, T> {
    inputs: JoinInputs<'a, T>,
    left_values: InternalTupleSet<InternalTuple<T>>,
    right_values: InternalTupleSet<InternalTuple<T>>,
    buffered_results: Vec<Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>>,
}

impl<T: Clone + Eq + Hash> Iterator for SymmetricHashJoinIterator<'_, T> {
    type Item = Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(result) = self.buffered_results.pop() {
                return Some(result);
            }
            match self
                .inputs
                .next(self.left_values.len == 0, self.right_values.len == 0)?
            {
                Ok(StreamingItem::Item((side, delta))) => {
                    if let Err(error) = self.process_delta(side, delta) {
                        return Some(Err(error));
                    }
                }
                Ok(StreamingItem::Pending) => return Some(Ok(StreamingItem::Pending)),
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

impl<T: Clone + Eq + Hash> SymmetricHashJoinIterator<'_, T> {
    fn process_delta(
        &mut self,
        side: JoinSide,
        delta: Delta<InternalTuple<T>>,
    ) -> Result<(), QueryEvaluationError> {
        let (values, other_values) = match side {
            JoinSide::Left => (&mut self.left_values, &self.right_values),
            JoinSide::Right => (&mut self.right_values, &self.left_values),
        };
        let key = values.tuple_key(delta.value());
        let kind = delta.kind();
        self.buffered_results.extend(
            other_values
                .get(key)
                .iter()
                .filter_map(|other| delta.value().combine_with(other))
                .map(move |joined| Ok(StreamingItem::Item(Delta::with_kind(kind, joined)))),
        );
        match delta {
            Delta::Addition(tuple) => values.insert(key, tuple),
            Delta::Deletion(tuple) if values.remove_one(key, |value| value == &tuple).is_none() => {
                return Err(non_existing_tuple_deleted());
            }
            Delta::Deletion(_) => {}
        }
        Ok(())
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
    inputs: JoinInputs<'a, T>,
    left_values: Vec<InternalTuple<T>>,
    right_values: Vec<InternalTuple<T>>,
    buffered_results: Vec<Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>>,
}

impl<T: Clone + Eq> SymmetricNestedLoopJoinIterator<'_, T> {
    fn process_delta(&mut self, side: JoinSide, delta: Delta<InternalTuple<T>>) {
        let (values, other_values) = match side {
            JoinSide::Left => (&mut self.left_values, &self.right_values),
            JoinSide::Right => (&mut self.right_values, &self.left_values),
        };
        let (kind, tuple) = delta.into_parts();
        let joined = |other: &InternalTuple<T>| match side {
            JoinSide::Left => tuple.combine_with(other),
            JoinSide::Right => other.combine_with(&tuple),
        };
        match kind {
            DeltaKind::Addition => {
                self.buffered_results.extend(
                    other_values
                        .iter()
                        .filter_map(joined)
                        .map(move |joined| Ok(StreamingItem::Item(Delta::with_kind(kind, joined)))),
                );
                values.push(tuple);
            }
            DeltaKind::Deletion => {
                if let Some(position) = values.iter().position(|value| value == &tuple) {
                    values.swap_remove(position);
                    self.buffered_results
                        .extend(other_values.iter().filter_map(joined).map(move |joined| {
                            Ok(StreamingItem::Item(Delta::with_kind(kind, joined)))
                        }));
                }
            }
        }
    }
}

impl<T: Clone + Eq> Iterator for SymmetricNestedLoopJoinIterator<'_, T> {
    type Item = Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(result) = self.buffered_results.pop() {
                return Some(result);
            }
            match self
                .inputs
                .next(self.left_values.is_empty(), self.right_values.is_empty())?
            {
                Ok(StreamingItem::Item((side, delta))) => self.process_delta(side, delta),
                Ok(StreamingItem::Pending) => return Some(Ok(StreamingItem::Pending)),
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

struct MinusIterator<'a, T> {
    inputs: JoinInputs<'a, T>,
    left_values: InternalTupleSet<(InternalTuple<T>, usize)>,
    right_values: InternalTupleSet<InternalTuple<T>>,
    // Each input delta produces only additions or only deletions, so pop order is immaterial.
    buffered_results: Vec<Delta<InternalTuple<T>>>,
}

impl<T: Clone + Eq + Hash> Iterator for MinusIterator<'_, T> {
    type Item = Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(delta) = self.buffered_results.pop() {
                return Some(Ok(StreamingItem::Item(delta)));
            }
            match self.inputs.next_left_preserving()? {
                Ok(StreamingItem::Item((side, delta))) => {
                    if let Err(error) = self.process_delta(side, delta) {
                        return Some(Err(error));
                    }
                }
                Ok(StreamingItem::Pending) => return Some(Ok(StreamingItem::Pending)),
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

impl<T: Clone + Eq + Hash> MinusIterator<'_, T> {
    fn process_delta(
        &mut self,
        side: JoinSide,
        delta: Delta<InternalTuple<T>>,
    ) -> Result<(), QueryEvaluationError> {
        let key = self.left_values.tuple_key(delta.value());
        match (side, delta) {
            (JoinSide::Left, Delta::Addition(left)) => {
                let matches = self
                    .right_values
                    .get(key)
                    .iter()
                    .filter(|right| are_compatible_and_not_disjointed(&left, right))
                    .count();
                if matches == 0 {
                    self.buffered_results.push(Delta::addition(left.clone()));
                }
                self.left_values.insert(key, (left, matches));
            }
            (JoinSide::Left, Delta::Deletion(left)) => {
                let (_, matches) = self
                    .left_values
                    .remove_one(key, |(tuple, _)| tuple == &left)
                    .ok_or_else(non_existing_tuple_deleted)?;
                if matches == 0 {
                    self.buffered_results.push(Delta::deletion(left));
                }
            }
            (JoinSide::Right, Delta::Addition(right)) => {
                for (left, matches) in self.left_values.get_mut(key) {
                    if are_compatible_and_not_disjointed(left, &right) {
                        if *matches == 0 {
                            self.buffered_results.push(Delta::deletion(left.clone()));
                        }
                        *matches += 1;
                    }
                }
                self.right_values.insert(key, right);
            }
            (JoinSide::Right, Delta::Deletion(right)) => {
                self.right_values
                    .remove_one(key, |tuple| tuple == &right)
                    .ok_or_else(non_existing_tuple_deleted)?;
                for (left, matches) in self.left_values.get_mut(key) {
                    if are_compatible_and_not_disjointed(left, &right) {
                        *matches -= 1;
                        if *matches == 0 {
                            self.buffered_results.push(Delta::addition(left.clone()));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

struct SymmetricHashLeftJoinIterator<'a, T> {
    inputs: JoinInputs<'a, T>,
    expression: InternalBooleanExpressionEvaluator<'a, T>,
    left_values: InternalTupleSet<(InternalTuple<T>, usize)>,
    right_values: InternalTupleSet<InternalTuple<T>>,
    buffered_results: VecDeque<Delta<InternalTuple<T>>>,
}

impl<T: Clone + Eq + Hash> Iterator for SymmetricHashLeftJoinIterator<'_, T> {
    type Item = Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(delta) = self.buffered_results.pop_front() {
                return Some(Ok(StreamingItem::Item(delta)));
            }
            match self.inputs.next_left_preserving()? {
                Ok(StreamingItem::Item((side, delta))) => {
                    if let Err(error) = self.process_delta(side, delta) {
                        return Some(Err(error));
                    }
                }
                Ok(StreamingItem::Pending) => return Some(Ok(StreamingItem::Pending)),
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

impl<T: Clone + Eq + Hash> SymmetricHashLeftJoinIterator<'_, T> {
    fn process_delta(
        &mut self,
        side: JoinSide,
        delta: Delta<InternalTuple<T>>,
    ) -> Result<(), QueryEvaluationError> {
        let key = self.left_values.tuple_key(delta.value());
        match (side, delta) {
            (JoinSide::Left, Delta::Addition(left)) => {
                let matches = self.emit_matches_for_left(key, &left, DeltaKind::Addition)?;
                if matches == 0 {
                    self.buffered_results
                        .push_back(Delta::addition(left.clone()));
                }
                self.left_values.insert(key, (left, matches));
            }
            (JoinSide::Left, Delta::Deletion(left)) => {
                let (_, matches) = self
                    .left_values
                    .remove_one(key, |(tuple, _)| tuple == &left)
                    .ok_or_else(non_existing_tuple_deleted)?;
                if matches == 0 {
                    self.buffered_results.push_back(Delta::deletion(left));
                } else {
                    self.emit_matches_for_left(key, &left, DeltaKind::Deletion)?;
                }
            }
            (JoinSide::Right, Delta::Addition(right)) => {
                for (left, matches) in self.left_values.get_mut(key) {
                    if let Some(joined) = Self::join_if_matches(&self.expression, left, &right)? {
                        let first_match = *matches == 0;
                        *matches += 1;
                        if first_match {
                            // The first match can produce exactly the existing unmatched row.
                            if joined == *left {
                                continue;
                            }
                            self.buffered_results
                                .push_back(Delta::deletion(left.clone()));
                        }
                        self.buffered_results.push_back(Delta::addition(joined));
                    }
                }
                self.right_values.insert(key, right);
            }
            (JoinSide::Right, Delta::Deletion(right)) => {
                self.right_values
                    .remove_one(key, |tuple| tuple == &right)
                    .ok_or_else(non_existing_tuple_deleted)?;
                for (left, matches) in self.left_values.get_mut(key) {
                    if let Some(joined) = Self::join_if_matches(&self.expression, left, &right)? {
                        *matches = matches
                            .checked_sub(1)
                            .ok_or_else(non_existing_tuple_deleted)?;
                        if *matches == 0 && joined == *left {
                            continue;
                        }
                        self.buffered_results.push_back(Delta::deletion(joined));
                        if *matches == 0 {
                            self.buffered_results
                                .push_back(Delta::addition(left.clone()));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn emit_matches_for_left(
        &mut self,
        key: u64,
        left: &InternalTuple<T>,
        kind: DeltaKind,
    ) -> Result<usize, QueryEvaluationError> {
        let mut matches = 0;
        for right in self.right_values.get(key) {
            if let Some(joined) = Self::join_if_matches(&self.expression, left, right)? {
                self.buffered_results
                    .push_back(Delta::with_kind(kind, joined));
                matches += 1;
            }
        }
        Ok(matches)
    }

    fn join_if_matches(
        expression: &InternalBooleanExpressionEvaluator<'_, T>,
        left: &InternalTuple<T>,
        right: &InternalTuple<T>,
    ) -> Result<Option<InternalTuple<T>>, QueryEvaluationError> {
        let Some(joined) = left.combine_with(right) else {
            return Ok(None);
        };
        Ok((expression(&joined)? == Some(true)).then_some(joined))
    }
}

struct InternalTupleSet<V> {
    key: Vec<usize>,
    map: FxHashMap<u64, Vec<V>>,
    len: usize,
}

impl<V> InternalTupleSet<V> {
    fn new(key: Vec<usize>) -> Self {
        Self {
            key,
            map: FxHashMap::default(),
            len: 0,
        }
    }
    fn reserve(&mut self, additional: usize) {
        self.map.reserve(additional);
    }

    fn insert(&mut self, key: u64, value: V) {
        self.map.entry(key).or_default().push(value);
        self.len += 1;
    }

    fn get(&self, key: u64) -> &[V] {
        self.map.get(&key).map_or(&[], |v| v)
    }

    fn get_mut(&mut self, key: u64) -> &mut [V] {
        self.map
            .get_mut(&key)
            .map(Vec::as_mut_slice)
            .unwrap_or_default()
    }

    fn tuple_key<T: Hash>(&self, tuple: &InternalTuple<T>) -> u64 {
        tuple.hash_key(&self.key)
    }

    fn remove_one(&mut self, key: u64, matches: impl FnMut(&V) -> bool) -> Option<V> {
        let values = self.map.get_mut(&key)?;
        let position = values.iter().position(matches)?;
        let value = values.swap_remove(position);
        self.len -= 1;
        if values.is_empty() {
            self.map.remove(&key);
        }
        Some(value)
    }
}

struct StatsDeltaIterator<'a, T> {
    inner: InternalTupleDeltasIterator<'a, T>,
    stats: Rc<EvalNodeWithStats>,
}

impl<T> Iterator for StatsDeltaIterator<'_, T> {
    type Item = Result<StreamingItem<Delta<InternalTuple<T>>>, QueryEvaluationError>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = Timer::now();
        let result = self.inner.next();
        let duration = start.elapsed()?;
        self.stats.exec_duration.set(
            self.stats
                .exec_duration
                .get()
                .and_then(|d| d.checked_add(duration)),
        );
        if let Some(Ok(StreamingItem::Item(_))) = &result {
            self.stats.exec_count.set(self.stats.exec_count.get() + 1);
        }
        result
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
