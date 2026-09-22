use super::{
    EvalDataset, EvalNodeWithStats, InternalTuple, Timer, TupleSelector, encode_initial_bindings,
    encode_variable, eval_node_label, put_pattern_value,
};
#[cfg(feature = "sparql-12")]
use crate::ExpressionTriple;
use crate::expression::CustomFunctionRegistry;
use crate::service::ServiceHandlerRegistry;
use crate::{
    CancellationToken, CustomAggregateFunctionRegistry, Delta, DeltaKind,
    IncrementalQueryableDataset, InternalQuad, QueryDatasetSpecification, QueryEvaluationError,
    QuerySolution,
};
use futures_core::Stream;
use futures_util::stream::{self, StreamExt};
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
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

type InternalTupleDeltaEvaluator<'a, T> =
    Rc<dyn Fn(InternalTuple<T>) -> InternalTupleDeltasStream<'a, T> + 'a>;

type InternalTupleDeltasStream<'a, T> =
    Pin<Box<dyn Stream<Item = Result<Delta<InternalTuple<T>>, QueryEvaluationError>> + 'a>>;

pub type QuerySolutionChange = Delta<QuerySolution>;

pub enum IncrementalDriverState<T> {
    Item(T),
    Pending,
    Done,
}

impl<'a, D: IncrementalQueryableDataset<'a>> EvalDataset<'a, D> {
    fn underlying_internal_quad_deltas_for_pattern(
        &self,
        subject: Option<&D::InternalTerm>,
        predicate: Option<&D::InternalTerm>,
        object: Option<&D::InternalTerm>,
        graph_name: Option<Option<&D::InternalTerm>>,
    ) -> impl Stream<Item = Result<Delta<InternalQuad<D::InternalTerm>>, QueryEvaluationError>>
    + use<'a, D> {
        let cancellation_token = self.cancellation_token.clone();
        self.dataset
            .internal_quad_deltas_for_pattern(subject, predicate, object, graph_name)
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
    ) -> InternalQuadDeltaStream<'a, D::InternalTerm> {
        if let Some(graph_name) = graph_name {
            if let Some(graph_name) = graph_name {
                if self
                    .specification
                    .named
                    .as_ref()
                    .is_none_or(|d| d.contains(graph_name))
                {
                    Box::pin(self.underlying_internal_quad_deltas_for_pattern(
                        subject,
                        predicate,
                        object,
                        Some(Some(graph_name)),
                    ))
                } else {
                    Box::pin(stream::empty())
                }
            } else if let Some(default_graph_graphs) = &self.specification.default {
                if default_graph_graphs.len() == 1 {
                    Box::pin(
                        self.underlying_internal_quad_deltas_for_pattern(
                            subject,
                            predicate,
                            object,
                            Some(default_graph_graphs[0].as_ref()),
                        )
                        .map(|quad| {
                            let mut quad = quad?;
                            quad.value_mut().graph_name = None;
                            Ok(quad)
                        }),
                    )
                } else {
                    let iters = default_graph_graphs
                        .iter()
                        .map(
                            |graph_name| -> InternalQuadDeltaStream<'a, D::InternalTerm> {
                                Box::pin(self.underlying_internal_quad_deltas_for_pattern(
                                    subject,
                                    predicate,
                                    object,
                                    Some(graph_name.as_ref()),
                                ))
                            },
                        )
                        .collect::<Vec<_>>();
                    Box::pin(DeduplicatingDefaultGraphStream::new(Box::pin(
                        RoundRobinStream::new(iters).map(|quad| {
                            let mut quad = quad?;
                            quad.value_mut().graph_name = None;
                            Ok(quad)
                        }),
                    )))
                }
            } else {
                Box::pin(DeduplicatingDefaultGraphStream::new(Box::pin(
                    self.underlying_internal_quad_deltas_for_pattern(
                        subject, predicate, object, None,
                    )
                    .map(|quad| {
                        let mut quad = quad?;
                        quad.value_mut().graph_name = None;
                        Ok(quad)
                    }),
                )))
            }
        } else if let Some(named_graphs) = &self.specification.named {
            let iters = named_graphs
                .iter()
                .map(
                    |graph_name| -> InternalQuadDeltaStream<'a, D::InternalTerm> {
                        Box::pin(self.underlying_internal_quad_deltas_for_pattern(
                            subject,
                            predicate,
                            object,
                            Some(Some(graph_name)),
                        ))
                    },
                )
                .collect::<Vec<_>>();
            Box::pin(RoundRobinStream::new(iters))
        } else {
            Box::pin(
                self.underlying_internal_quad_deltas_for_pattern(subject, predicate, object, None)
                    .filter(|q| {
                        std::future::ready(
                            !q.as_ref()
                                .is_ok_and(|quad_delta| quad_delta.value().graph_name.is_none()),
                        )
                    }),
            )
        }
    }
}

type InternalQuadDeltaStream<'a, T> =
    Pin<Box<dyn Stream<Item = Result<Delta<InternalQuad<T>>, QueryEvaluationError>> + 'a>>;

/// Polls each live source once per round, so a pending source cannot starve the others.
struct RoundRobinStream<'a, T> {
    streams: Vec<Option<InternalQuadDeltaStream<'a, T>>>,
    next: usize,
}

impl<'a, T> RoundRobinStream<'a, T> {
    fn new(streams: Vec<InternalQuadDeltaStream<'a, T>>) -> Self {
        Self {
            streams: streams.into_iter().map(Some).collect(),
            next: 0,
        }
    }
}

impl<T> Stream for RoundRobinStream<'_, T> {
    type Item = Result<Delta<InternalQuad<T>>, QueryEvaluationError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.streams.is_empty() {
            return Poll::Ready(None);
        }
        let mut live = false;
        for _ in 0..this.streams.len() {
            let index = this.next;
            this.next = (this.next + 1) % this.streams.len();
            let Some(stream) = &mut this.streams[index] else {
                continue;
            };
            match stream.as_mut().poll_next(cx) {
                Poll::Pending => live = true,
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => this.streams[index] = None,
            }
        }
        if live {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
}

/// Turns a union of RDF graphs into an RDF graph by retaining one copy of each triple.
struct DeduplicatingDefaultGraphStream<'a, T> {
    inner: InternalQuadDeltaStream<'a, T>,
    counts: FxHashMap<InternalQuad<T>, usize>,
}

impl<T> Unpin for DeduplicatingDefaultGraphStream<'_, T> {}

impl<'a, T> DeduplicatingDefaultGraphStream<'a, T> {
    fn new(inner: InternalQuadDeltaStream<'a, T>) -> Self {
        Self {
            inner,
            counts: FxHashMap::default(),
        }
    }
}

impl<T: Clone + Eq + Hash> Stream for DeduplicatingDefaultGraphStream<'_, T> {
    type Item = Result<Delta<InternalQuad<T>>, QueryEvaluationError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let delta = match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok(delta))) => delta,
            };
            match delta {
                Delta::Addition(quad) => {
                    let count = this.counts.entry(quad.clone()).or_default();
                    *count += 1;
                    if *count == 1 {
                        return Poll::Ready(Some(Ok(Delta::Addition(quad))));
                    }
                }
                Delta::Deletion(quad) => match this.counts.get_mut(&quad) {
                    Some(count) if *count > 1 => *count -= 1,
                    Some(_) => {
                        this.counts.remove(&quad);
                        return Poll::Ready(Some(Ok(Delta::Deletion(quad))));
                    }
                    None => return Poll::Ready(Some(Err(non_existing_tuple_deleted()))),
                },
            }
        }
    }
}

struct IncrementalDriverCore<'a, D: IncrementalQueryableDataset<'a>> {
    dataset: EvalDataset<'a, D>,
}

/// Asynchronous stream of solution changes produced by an incremental SELECT query.
pub struct IncrementalSelectDriver<'a, D: IncrementalQueryableDataset<'a>> {
    core: IncrementalDriverCore<'a, D>,
    variables: Arc<[Variable]>,
    stream: InternalTupleDeltasStream<'a, D::InternalTerm>,
    cancellation_waker: Option<Waker>,
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
        let evaluator = Self {
            dataset: self.dataset.clone(),
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
                },
                variables: Arc::from(variables),
                stream: eval(from),
                cancellation_waker: None,
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
                Box::pin(StatsDeltaStream {
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
                    Box::pin(stream::iter(
                        encoded_tuples
                            .iter()
                            .filter_map(move |t| from.combine_with(t))
                            .map(|tuple| Ok(Delta::addition(tuple)))
                            .collect::<Vec<_>>(),
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
                Rc::new(move |from| {
                    let input_subject = match subject_selector.get_pattern_value(
                        &from,
                        #[cfg(feature = "sparql-12")]
                        &dataset,
                    ) {
                        Ok(value) => value,
                        Err(e) => return Box::pin(stream::iter([Err(e)])),
                    };
                    let input_predicate = match predicate_selector.get_pattern_value(
                        &from,
                        #[cfg(feature = "sparql-12")]
                        &dataset,
                    ) {
                        Ok(value) => value,
                        Err(e) => return Box::pin(stream::iter([Err(e)])),
                    };
                    let input_object = match object_selector.get_pattern_value(
                        &from,
                        #[cfg(feature = "sparql-12")]
                        &dataset,
                    ) {
                        Ok(value) => value,
                        Err(e) => return Box::pin(stream::iter([Err(e)])),
                    };
                    let input_graph_name = if let Some(graph_name_selector) = &graph_name_selector {
                        match graph_name_selector.get_pattern_value(
                            &from,
                            #[cfg(feature = "sparql-12")]
                            &dataset,
                        ) {
                            Ok(value) => value,
                            Err(e) => return Box::pin(stream::iter([Err(e)])),
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
                    );
                    let subject_selector = subject_selector.clone();
                    let predicate_selector = predicate_selector.clone();
                    let object_selector = object_selector.clone();
                    let graph_name_selector = graph_name_selector.clone();
                    #[cfg(feature = "sparql-12")]
                    let dataset = dataset.clone();
                    Box::pin(
                        iter.map(move |quad| {
                            let quad = quad?;
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
                            Ok(Some(Delta::with_kind(kind, new_tuple)))
                        })
                        .filter_map(|result| std::future::ready(result.transpose())),
                    )
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
                                Box::pin(SymmetricNestedLoopJoinStream {
                                    left_iter: left(from.clone()),
                                    right_iter: right(from),
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
                                let left_values = InternalTupleSet::new(keys.clone());
                                let right_values = InternalTupleSet::new(keys.clone());
                                Box::pin(SymmetricHashJoinStream {
                                    left_iter,
                                    right_iter,
                                    left_values,
                                    right_values,
                                    pending_values: Vec::new(),
                                    buffered_results: Vec::new(),
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
                    Box::pin(child(input_tuple).filter_map(move |change| {
                        std::future::ready((|| match change {
                            Ok(delta) => {
                                let (kind, tuple) = delta.into_parts();
                                let mut output_tuple = from.clone();
                                for (input_key, output_key) in &*mapping {
                                    if let Some(value) = tuple.get(*input_key) {
                                        if let Some(existing_value) = output_tuple.get(*output_key)
                                        {
                                            if existing_value != value {
                                                return None; // Conflict
                                            }
                                        } else {
                                            output_tuple.set(*output_key, value.clone());
                                        }
                                    }
                                }
                                Some(Ok(Delta::with_kind(kind, output_tuple)))
                            }
                            Err(error) => Some(Err(error)),
                        })())
                    }))
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

    pub(crate) fn poll_next_ready(
        &mut self,
    ) -> Result<IncrementalDriverState<QuerySolutionChange>, QueryEvaluationError> {
        let waker = futures_util::task::noop_waker_ref();
        let mut cx = Context::from_waker(waker);
        match Pin::new(self).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(delta))) => Ok(IncrementalDriverState::Item(delta)),
            Poll::Ready(Some(Err(error))) => Err(error),
            Poll::Ready(None) => Ok(IncrementalDriverState::Done),
            Poll::Pending => Ok(IncrementalDriverState::Pending),
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
            match self.poll_next_ready()? {
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

impl<'a, D: IncrementalQueryableDataset<'a>> Unpin for IncrementalSelectDriver<'a, D> {}

impl<'a, D: IncrementalQueryableDataset<'a>> Drop for IncrementalSelectDriver<'a, D> {
    fn drop(&mut self) {
        if let Some(waker) = &self.cancellation_waker {
            self.core.dataset.cancellation_token.unregister_waker(waker);
        }
    }
}

impl<'a, D: IncrementalQueryableDataset<'a>> Stream for IncrementalSelectDriver<'a, D> {
    type Item = Result<QuerySolutionChange, QueryEvaluationError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Err(error) = this.core.dataset.cancellation_token.ensure_alive() {
            return Poll::Ready(Some(Err(error)));
        }
        let result = match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(delta))) => {
                let (kind, tuple) = delta.into_parts();
                let mut result = vec![None; this.variables.len()];
                for (index, value) in tuple.iter().enumerate() {
                    if let Some(term) = value {
                        let term = match this.core.dataset.externalize_term(term) {
                            Ok(term) => term,
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        };
                        result[index] = Some(term);
                    }
                }
                Poll::Ready(Some(Ok(Delta::with_kind(
                    kind,
                    (Arc::clone(&this.variables), result).into(),
                ))))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        };
        if result.is_pending() {
            if let Some(old_waker) = &this.cancellation_waker
                && !old_waker.will_wake(cx.waker())
            {
                this.core
                    .dataset
                    .cancellation_token
                    .unregister_waker(old_waker);
            }
            this.core
                .dataset
                .cancellation_token
                .register_waker(cx.waker());
            this.cancellation_waker = Some(cx.waker().clone());
            if let Err(error) = this.core.dataset.cancellation_token.ensure_alive() {
                return Poll::Ready(Some(Err(error)));
            }
        } else if let Some(waker) = this.cancellation_waker.take() {
            this.core
                .dataset
                .cancellation_token
                .unregister_waker(&waker);
        }
        result
    }
}

struct SymmetricHashJoinStream<'a, T> {
    left_iter: InternalTupleDeltasStream<'a, T>,
    right_iter: InternalTupleDeltasStream<'a, T>,
    left_values: InternalTupleSet<T>,
    right_values: InternalTupleSet<T>,
    pending_values: Vec<(u64, Delta<InternalTuple<T>>)>,
    buffered_results: Vec<Result<Delta<InternalTuple<T>>, QueryEvaluationError>>,
    left_done: bool,
    right_done: bool,
    active_side: JoinSide,
}

impl<T> Unpin for SymmetricHashJoinStream<'_, T> {}

impl<T: Clone + Eq + Hash> Stream for SymmetricHashJoinStream<'_, T> {
    type Item = Result<Delta<InternalTuple<T>>, QueryEvaluationError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(result) = this.buffered_results.pop() {
                return Poll::Ready(Some(result));
            }
            if this.left_done && this.right_done {
                return Poll::Ready(None);
            }

            let first_state = this.poll_active_side(cx);
            if let Some(result) = this.buffered_results.pop() {
                return Poll::Ready(Some(result));
            }

            this.active_side = this.active_side.other();
            let second_state = this.poll_active_side(cx);
            if let Some(result) = this.buffered_results.pop() {
                return Poll::Ready(Some(result));
            }

            if this.left_done && this.right_done {
                return Poll::Ready(None);
            }
            if matches!(first_state, PollState::Pending | PollState::Done)
                && matches!(second_state, PollState::Pending | PollState::Done)
            {
                return Poll::Pending;
            }
        }
    }
}

impl<T: Clone + Eq + Hash> SymmetricHashJoinStream<'_, T> {
    fn poll_active_side(&mut self, cx: &mut Context<'_>) -> PollState {
        let (iter, values, other_values, done) = match self.active_side {
            JoinSide::Left => (
                &mut self.left_iter,
                &mut self.left_values,
                &self.right_values,
                &mut self.left_done,
            ),
            JoinSide::Right => (
                &mut self.right_iter,
                &mut self.right_values,
                &self.left_values,
                &mut self.right_done,
            ),
        };
        if *done {
            return PollState::Done;
        }

        while self.buffered_results.is_empty() {
            match iter.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(delta))) => {
                    let key = other_values.tuple_key(delta.value());
                    let kind = delta.kind();
                    self.buffered_results.extend(
                        other_values
                            .get(key)
                            .iter()
                            .filter_map(|other| delta.value().combine_with(other))
                            .map(move |joined| Ok(Delta::with_kind(kind, joined))),
                    );
                    self.pending_values.push((key, delta));
                }
                Poll::Pending => break,
                Poll::Ready(Some(Err(error))) => {
                    self.buffered_results.push(Err(error));
                    break;
                }
                Poll::Ready(None) => {
                    *done = true;
                    break;
                }
            }
        }

        values.reserve(self.pending_values.len());
        for (key, delta) in self.pending_values.drain(..) {
            match delta {
                Delta::Addition(tuple) => values.insert(key, tuple),
                Delta::Deletion(tuple) if !values.remove_one(key, &tuple) => {
                    self.buffered_results
                        .push(Err(non_existing_tuple_deleted()));
                }
                Delta::Deletion(_) => {}
            }
        }

        if self.buffered_results.is_empty() {
            if *done {
                PollState::Done
            } else {
                PollState::Pending
            }
        } else {
            PollState::Item
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

struct SymmetricNestedLoopJoinStream<'a, T> {
    left_iter: InternalTupleDeltasStream<'a, T>,
    right_iter: InternalTupleDeltasStream<'a, T>,
    left_values: Vec<InternalTuple<T>>,
    right_values: Vec<InternalTuple<T>>,
    buffered_results: Vec<Result<Delta<InternalTuple<T>>, QueryEvaluationError>>,
    left_done: bool,
    right_done: bool,
    active_side: JoinSide,
}

impl<T: Clone + Eq> SymmetricNestedLoopJoinStream<'_, T> {
    fn poll_active_side(&mut self, cx: &mut Context<'_>) -> PollState {
        let active_side = self.active_side;
        let (iter, values, other_values, done) = match active_side {
            JoinSide::Left => (
                &mut self.left_iter,
                &mut self.left_values,
                &self.right_values,
                &mut self.left_done,
            ),
            JoinSide::Right => (
                &mut self.right_iter,
                &mut self.right_values,
                &self.left_values,
                &mut self.right_done,
            ),
        };
        if *done {
            return PollState::Done;
        }

        while self.buffered_results.is_empty() {
            match iter.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(delta))) => {
                    let (kind, tuple) = delta.into_parts();
                    let joined = |other: &InternalTuple<T>| match active_side {
                        JoinSide::Left => tuple.combine_with(other),
                        JoinSide::Right => other.combine_with(&tuple),
                    };
                    match kind {
                        DeltaKind::Addition => {
                            self.buffered_results.extend(
                                other_values
                                    .iter()
                                    .filter_map(joined)
                                    .map(move |joined| Ok(Delta::with_kind(kind, joined))),
                            );
                            values.push(tuple);
                        }
                        DeltaKind::Deletion => {
                            if let Some(position) = values.iter().position(|value| value == &tuple)
                            {
                                values.swap_remove(position);
                                self.buffered_results.extend(
                                    other_values
                                        .iter()
                                        .filter_map(joined)
                                        .map(move |joined| Ok(Delta::with_kind(kind, joined))),
                                );
                            }
                        }
                    }
                }
                Poll::Pending => break,
                Poll::Ready(Some(Err(error))) => {
                    self.buffered_results.push(Err(error));
                    break;
                }
                Poll::Ready(None) => {
                    *done = true;
                    break;
                }
            }
        }

        if self.buffered_results.is_empty() {
            if *done {
                PollState::Done
            } else {
                PollState::Pending
            }
        } else {
            PollState::Item
        }
    }
}

impl<T> Unpin for SymmetricNestedLoopJoinStream<'_, T> {}

impl<T: Clone + Eq> Stream for SymmetricNestedLoopJoinStream<'_, T> {
    type Item = Result<Delta<InternalTuple<T>>, QueryEvaluationError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(result) = this.buffered_results.pop() {
                return Poll::Ready(Some(result));
            }
            if this.left_done && this.right_done {
                return Poll::Ready(None);
            }

            let first_state = this.poll_active_side(cx);
            if let Some(result) = this.buffered_results.pop() {
                return Poll::Ready(Some(result));
            }

            this.active_side = this.active_side.other();
            let second_state = this.poll_active_side(cx);
            if let Some(result) = this.buffered_results.pop() {
                return Poll::Ready(Some(result));
            }

            if this.left_done && this.right_done {
                return Poll::Ready(None);
            }
            if matches!(first_state, PollState::Pending | PollState::Done)
                && matches!(second_state, PollState::Pending | PollState::Done)
            {
                return Poll::Pending;
            }
        }
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

struct StatsDeltaStream<'a, T> {
    inner: InternalTupleDeltasStream<'a, T>,
    stats: Rc<EvalNodeWithStats>,
}

impl<T> Unpin for StatsDeltaStream<'_, T> {}

impl<T> Stream for StatsDeltaStream<'_, T> {
    type Item = Result<Delta<InternalTuple<T>>, QueryEvaluationError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let start = Timer::now();
        let result = this.inner.as_mut().poll_next(cx);
        let Some(duration) = start.elapsed() else {
            return Poll::Ready(None);
        };
        this.stats.exec_duration.set(
            this.stats
                .exec_duration
                .get()
                .and_then(|d| d.checked_add(duration)),
        );
        if matches!(&result, Poll::Ready(Some(Ok(_)))) {
            this.stats.exec_count.set(this.stats.exec_count.get() + 1);
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
