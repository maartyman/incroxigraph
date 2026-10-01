use crate::{
    IncrementalDriverState, IncrementalQueryableDataset, IncrementalSelectDriver,
    QueryEvaluationError, QuerySolution, QuerySolutionIter, QueryTripleIter,
};
use oxrdf::{BlankNode, NamedOrBlankNode, Term, Triple, Variable};
use rustc_hash::{FxHashMap, FxHasher};
use sparesults::QuerySolutionRef;
use spargebra::term::{NamedNodePattern, TermTemplate, TripleTemplate};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone)]
pub enum Delta<T> {
    Addition(T),
    Deletion(T),
}

#[derive(Clone, Copy)]
pub enum DeltaKind {
    Addition,
    Deletion,
}

impl<T> Delta<T> {
    pub fn addition(value: T) -> Self {
        Self::Addition(value)
    }

    pub fn deletion(value: T) -> Self {
        Self::Deletion(value)
    }

    pub fn with_kind(kind: DeltaKind, value: T) -> Self {
        match kind {
            DeltaKind::Addition => Self::Addition(value),
            DeltaKind::Deletion => Self::Deletion(value),
        }
    }

    pub fn kind(&self) -> DeltaKind {
        match self {
            Self::Addition(_) => DeltaKind::Addition,
            Self::Deletion(_) => DeltaKind::Deletion,
        }
    }

    pub fn value(&self) -> &T {
        match self {
            Self::Addition(value) | Self::Deletion(value) => value,
        }
    }

    pub fn value_mut(&mut self) -> &mut T {
        match self {
            Self::Addition(value) | Self::Deletion(value) => value,
        }
    }

    pub fn into_value(self) -> T {
        match self {
            Self::Addition(value) | Self::Deletion(value) => value,
        }
    }

    pub fn into_parts(self) -> (DeltaKind, T) {
        match self {
            Self::Addition(value) => (DeltaKind::Addition, value),
            Self::Deletion(value) => (DeltaKind::Deletion, value),
        }
    }

    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Delta<U> {
        match self {
            Self::Addition(value) => Delta::Addition(f(value)),
            Self::Deletion(value) => Delta::Deletion(f(value)),
        }
    }
}

/// An iterator over [`Delta`]s of query solutions produced by one change batch.
pub type QuerySolutionDeltaIter<'a> = QuerySolutionIter<'a, Delta<QuerySolution>>;

/// An iterator over [`Delta`]s of triples produced by one change batch.
pub type QueryTripleDeltaIter<'a> = QueryTripleIter<'a, Delta<Triple>>;

/// An iterator over updated [ASK](https://www.w3.org/TR/sparql11-query/#ask) boolean values.
///
/// The first batch contains the initial boolean result. Later batches contain the new value only
/// when a change flips it.
pub struct QueryBooleanDeltaIter {
    iter: std::vec::IntoIter<bool>,
}

impl QueryBooleanDeltaIter {
    fn new(changes: Vec<bool>) -> Self {
        Self {
            iter: changes.into_iter(),
        }
    }
}

impl Iterator for QueryBooleanDeltaIter {
    type Item = Result<bool, QueryEvaluationError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(Ok)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// The delta counterpart of [`QueryResults`], mirroring its `Solutions`/`Boolean`/`Graph` shape.
pub enum QueryResultsDelta<'a> {
    /// Changes to the solutions of a [SELECT](https://www.w3.org/TR/sparql11-query/#select) query.
    Solutions(QuerySolutionDeltaIter<'a>),
    /// Updated values of an [ASK](https://www.w3.org/TR/sparql11-query/#ask) query.
    Boolean(QueryBooleanDeltaIter),
    /// Changes to the triples of a
    /// [CONSTRUCT](https://www.w3.org/TR/sparql11-query/#construct) or
    /// [DESCRIBE](https://www.w3.org/TR/sparql11-query/#describe) query.
    Graph(QueryTripleDeltaIter<'a>),
}

/// Stateful handle for complete results of an incremental query.
///
/// If [`Self::results`] or the iterator returns an error, stop using and drop this handle.
/// Errors do not mark the handle as done, and later calls have no recovery guarantee.
pub struct IncrementalQueryResultsState<'a, D: IncrementalQueryableDataset<'a>> {
    driver: IncrementalSelectDriver<'a, D>,
    output: IncrementalResultsOutput,
    done: bool,
    emitted_initial: bool,
}

enum IncrementalResultsOutput {
    Solutions {
        variables: Arc<[Variable]>,
        /// One owned solution and its multiplicity per distinct row.
        current: QuerySolutionSet,
    },
    Boolean {
        solution_count: usize,
    },
    Graph(IncrementalGraphOutput),
}

/// A borrowed snapshot of the current results of an incremental query.
pub enum IncrementalQueryResults<'a> {
    /// Solutions of a SELECT query.
    Solutions(IncrementalSelectResults<'a>),
    /// Result of an ASK query.
    Boolean(bool),
    /// Triples of a CONSTRUCT query.
    Graph(IncrementalGraphResults<'a>),
}

/// A borrowed snapshot of the current solutions of an incremental SELECT query.
pub struct IncrementalSelectResults<'a> {
    current: QuerySolutionSetIter<'a>,
}

impl<'a> IncrementalSelectResults<'a> {
    fn new(current: QuerySolutionSetIter<'a>) -> Self {
        Self { current }
    }
}

impl<'a> Iterator for IncrementalSelectResults<'a> {
    type Item = QuerySolutionRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let (solution, _) = self.current.next()?;
        Some(QuerySolutionRef::new(
            solution.variables(),
            solution.values(),
        ))
    }
}

/// A borrowed snapshot of the current triples of an incremental CONSTRUCT query.
pub struct IncrementalGraphResults<'a> {
    current: std::collections::hash_map::Keys<'a, Triple, usize>,
}

impl<'a> Iterator for IncrementalGraphResults<'a> {
    type Item = &'a Triple;

    fn next(&mut self) -> Option<Self::Item> {
        self.current.next()
    }
}

/// A collision-safe multiset of query solutions keyed by the hash of their ordered values.
struct QuerySolutionSet {
    buckets: FxHashMap<u64, Vec<(QuerySolution, usize)>>,
}

impl QuerySolutionSet {
    fn new() -> Self {
        Self {
            buckets: FxHashMap::default(),
        }
    }

    fn insert(&mut self, solution: QuerySolution) {
        let key = Self::key(&solution);
        let bucket = self.buckets.entry(key).or_default();
        if let Some((_, count)) = bucket
            .iter_mut()
            .find(|(stored, _)| stored.values() == solution.values())
        {
            *count += 1;
        } else {
            bucket.push((solution, 1));
        }
    }

    fn remove_one(&mut self, solution: &QuerySolution) {
        let key = Self::key(solution);
        let Some(bucket) = self.buckets.get_mut(&key) else {
            return;
        };
        let Some(position) = bucket
            .iter()
            .position(|(stored, _)| stored.values() == solution.values())
        else {
            return;
        };
        let (_, count) = &mut bucket[position];
        *count -= 1;
        if *count == 0 {
            bucket.swap_remove(position);
        }
        let remove_bucket = bucket.is_empty();
        if remove_bucket {
            self.buckets.remove(&key);
        }
    }

    fn iter(&self) -> QuerySolutionSetIter<'_> {
        QuerySolutionSetIter {
            buckets: self.buckets.values(),
            bucket: None,
            index: 0,
            remaining: 0,
        }
    }

    fn key(solution: &QuerySolution) -> u64 {
        let mut hasher = FxHasher::default();
        solution.values().hash(&mut hasher);
        hasher.finish()
    }
}

struct QuerySolutionSetIter<'a> {
    buckets: std::collections::hash_map::Values<'a, u64, Vec<(QuerySolution, usize)>>,
    bucket: Option<&'a Vec<(QuerySolution, usize)>>,
    index: usize,
    remaining: usize,
}

impl<'a> Iterator for QuerySolutionSetIter<'a> {
    type Item = (&'a QuerySolution, usize);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let bucket = match self.bucket {
                Some(bucket) if self.index < bucket.len() => bucket,
                _ => {
                    self.bucket = Some(self.buckets.next()?);
                    self.index = 0;
                    self.remaining = 0;
                    continue;
                }
            };
            let (solution, count) = &bucket[self.index];
            if self.remaining == 0 {
                self.remaining = *count;
            }
            self.remaining -= 1;
            if self.remaining == 0 {
                self.index += 1;
            }
            return Some((solution, *count));
        }
    }
}

impl<'a, D: IncrementalQueryableDataset<'a>> IncrementalQueryResultsState<'a, D> {
    pub(crate) fn new_select(driver: IncrementalSelectDriver<'a, D>) -> Self {
        let variables = driver.variables();
        Self {
            driver,
            output: IncrementalResultsOutput::Solutions {
                variables,
                current: QuerySolutionSet::new(),
            },
            done: false,
            emitted_initial: false,
        }
    }

    pub(crate) fn new_ask(driver: IncrementalSelectDriver<'a, D>) -> Self {
        Self {
            driver,
            output: IncrementalResultsOutput::Boolean { solution_count: 0 },
            done: false,
            emitted_initial: false,
        }
    }

    pub(crate) fn new_construct(
        driver: IncrementalSelectDriver<'a, D>,
        template: Vec<TripleTemplate>,
    ) -> Self {
        Self {
            driver,
            output: IncrementalResultsOutput::Graph(IncrementalGraphOutput::new(template)),
            done: false,
            emitted_initial: false,
        }
    }

    /// The variables produced by a SELECT query, in projection order.
    ///
    /// Returns `None` for ASK and CONSTRUCT queries.
    pub fn variables(&self) -> Option<&[Variable]> {
        match &self.output {
            IncrementalResultsOutput::Solutions { variables, .. } => Some(variables),
            IncrementalResultsOutput::Boolean { .. } | IncrementalResultsOutput::Graph(_) => None,
        }
    }

    /// Processes all currently pending dataset changes and returns the current complete results.
    ///
    /// This is the pull-based counterpart of a regular query execution: calling it again after the
    /// dataset changed returns the updated results. The returned iterator borrows the state, which
    /// prevents mixing it with the other access modes until it is dropped.
    /// Drop the state if this call returns an error.
    pub fn results(&mut self) -> Result<IncrementalQueryResults<'_>, QueryEvaluationError> {
        self.drain_ready()?;
        Ok(self.snapshot())
    }

    /// Asynchronously iterates over complete result snapshots.
    ///
    /// The first `next().await` yields the initial snapshot. Each later `next().await` drains any
    /// changes that are ready and, if there are some, yields a fresh complete snapshot; otherwise it
    /// awaits the next committed dataset change and then yields the updated snapshot. It resolves to
    /// `None` only once the underlying query is exhausted (e.g. a static dataset).
    /// Drop the iterator and its state if `next()` returns an error.
    pub fn iter_results(&mut self) -> IncrementalQueryResultsIter<'_, 'a, D> {
        IncrementalQueryResultsIter { state: self }
    }

    fn drain_ready(&mut self) -> Result<bool, QueryEvaluationError> {
        if self.done {
            return Ok(false);
        }
        let mut changes = Vec::new();
        loop {
            match self.driver.poll_next() {
                Ok(IncrementalDriverState::Item(change)) => changes.push(change),
                Ok(IncrementalDriverState::Pending) => break,
                Ok(IncrementalDriverState::Done) => {
                    self.done = true;
                    break;
                }
                Err(error) => {
                    self.apply_changes(changes);
                    return Err(error);
                }
            }
        }
        Ok(self.apply_changes(changes))
    }

    fn apply_changes(&mut self, changes: Vec<Delta<QuerySolution>>) -> bool {
        match &mut self.output {
            IncrementalResultsOutput::Solutions { current, .. } => {
                let changed = !changes.is_empty();
                for change in changes {
                    let (kind, solution) = change.into_parts();
                    match kind {
                        DeltaKind::Addition => current.insert(solution),
                        DeltaKind::Deletion => current.remove_one(&solution),
                    }
                }
                changed
            }
            IncrementalResultsOutput::Boolean { solution_count } => {
                let old_value = *solution_count > 0;
                for change in changes {
                    match change.kind() {
                        DeltaKind::Addition => *solution_count += 1,
                        DeltaKind::Deletion => *solution_count = solution_count.saturating_sub(1),
                    }
                }
                old_value != (*solution_count > 0)
            }
            IncrementalResultsOutput::Graph(output) => !output.apply(changes).is_empty(),
        }
    }

    fn snapshot(&self) -> IncrementalQueryResults<'_> {
        match &self.output {
            IncrementalResultsOutput::Solutions { current, .. } => {
                IncrementalQueryResults::Solutions(IncrementalSelectResults::new(current.iter()))
            }
            IncrementalResultsOutput::Boolean { solution_count } => {
                IncrementalQueryResults::Boolean(*solution_count > 0)
            }
            IncrementalResultsOutput::Graph(output) => {
                IncrementalQueryResults::Graph(IncrementalGraphResults {
                    current: output.triple_counts.keys(),
                })
            }
        }
    }

    /// Awaits the next committed change to the underlying dataset.
    ///
    /// Lost-wakeup-safe: the caller captures the generation before draining, and this future
    /// re-checks it before and after registering the task waker.
    async fn wait_for_change(&self, baseline: u64) -> Result<(), QueryEvaluationError> {
        self.driver.wait_for_change(baseline).await
    }

    async fn next_results_snapshot(
        &mut self,
    ) -> Option<Result<IncrementalQueryResults<'_>, QueryEvaluationError>> {
        loop {
            let baseline = self.driver.change_generation();
            let changed = match self.drain_ready() {
                Ok(changed) => changed,
                Err(error) => return Some(Err(error)),
            };
            if !self.emitted_initial {
                self.emitted_initial = true;
                return Some(Ok(self.snapshot()));
            }
            if changed {
                return Some(Ok(self.snapshot()));
            }
            if self.done {
                return None;
            }
            if let Err(error) = self.wait_for_change(baseline).await {
                return Some(Err(error));
            }
        }
    }
}

/// Iterator returned by [`IncrementalQueryResultsState::iter_results`].
pub struct IncrementalQueryResultsIter<'s, 'a, D: IncrementalQueryableDataset<'a>> {
    state: &'s mut IncrementalQueryResultsState<'a, D>,
}

impl<'a, D: IncrementalQueryableDataset<'a>> IncrementalQueryResultsIter<'_, 'a, D> {
    /// Returns the next complete result snapshot, awaiting a dataset change if necessary.
    pub async fn next(
        &mut self,
    ) -> Option<Result<IncrementalQueryResults<'_>, QueryEvaluationError>> {
        self.state.next_results_snapshot().await
    }
}

/// Stateful handle for the deltas of an incremental query.
///
/// If [`Self::deltas`] or the iterator returns an error, stop using and drop this handle.
/// The current batch may have been partly consumed. Later calls have no recovery guarantee.
pub struct IncrementalQueryDeltasState<'a, D: IncrementalQueryableDataset<'a>> {
    driver: IncrementalSelectDriver<'a, D>,
    output: IncrementalQueryOutput,
    done: bool,
}

enum IncrementalQueryOutput {
    Solutions {
        variables: Arc<[Variable]>,
    },
    Boolean {
        solution_count: usize,
        initial: bool,
    },
    Graph(IncrementalGraphOutput),
}

struct IncrementalGraphOutput {
    template: Vec<TripleTemplate>,
    solution_buckets: FxHashMap<u64, Vec<(QuerySolution, Vec<Triple>)>>,
    triple_counts: FxHashMap<Triple, usize>,
}

impl<'a, D: IncrementalQueryableDataset<'a>> IncrementalQueryDeltasState<'a, D> {
    pub(crate) fn new_select(driver: IncrementalSelectDriver<'a, D>) -> Self {
        let variables = driver.variables();
        Self {
            driver,
            output: IncrementalQueryOutput::Solutions { variables },
            done: false,
        }
    }

    pub(crate) fn new_ask(driver: IncrementalSelectDriver<'a, D>) -> Self {
        Self {
            driver,
            output: IncrementalQueryOutput::Boolean {
                solution_count: 0,
                initial: true,
            },
            done: false,
        }
    }

    pub(crate) fn new_construct(
        driver: IncrementalSelectDriver<'a, D>,
        template: Vec<TripleTemplate>,
    ) -> Self {
        Self {
            driver,
            output: IncrementalQueryOutput::Graph(IncrementalGraphOutput::new(template)),
            done: false,
        }
    }

    /// The variables produced by a SELECT query, in projection order.
    ///
    /// Returns `None` for ASK and graph queries.
    pub fn variables(&self) -> Option<&[Variable]> {
        match &self.output {
            IncrementalQueryOutput::Solutions { variables } => Some(variables),
            IncrementalQueryOutput::Boolean { .. } | IncrementalQueryOutput::Graph(_) => None,
        }
    }

    /// Processes all currently pending dataset changes and returns them.
    ///
    /// If an error occurs, the current batch is discarded. Drop the state rather than retrying.
    pub fn deltas(&mut self) -> Result<QueryResultsDelta<'static>, QueryEvaluationError> {
        let batch = self.drain_ready()?;
        Ok(match &mut self.output {
            IncrementalQueryOutput::Solutions { variables } => QueryResultsDelta::Solutions(
                QuerySolutionDeltaIter::new(Arc::clone(variables), batch.into_iter().map(Ok)),
            ),
            IncrementalQueryOutput::Boolean {
                solution_count,
                initial,
            } => {
                let old_value = *solution_count > 0;
                for change in batch {
                    match change.kind() {
                        DeltaKind::Addition => *solution_count += 1,
                        DeltaKind::Deletion => *solution_count = solution_count.saturating_sub(1),
                    }
                }
                let new_value = *solution_count > 0;
                let changes = if std::mem::take(initial) || old_value != new_value {
                    vec![new_value]
                } else {
                    Vec::new()
                };
                QueryResultsDelta::Boolean(QueryBooleanDeltaIter::new(changes))
            }
            IncrementalQueryOutput::Graph(output) => QueryResultsDelta::Graph(
                QueryTripleIter::new(output.apply(batch).into_iter().map(Ok)),
            ),
        })
    }

    /// Asynchronously iterates over non-empty batches of query changes.
    ///
    /// Drop the iterator and its state if `next()` returns an error.
    pub fn iter_deltas(&mut self) -> IncrementalQueryDeltasIter<'_, 'a, D> {
        IncrementalQueryDeltasIter { state: self }
    }

    fn drain_ready(&mut self) -> Result<Vec<Delta<QuerySolution>>, QueryEvaluationError> {
        let mut batch = Vec::new();
        if self.done {
            return Ok(batch);
        }
        loop {
            match self.driver.poll_next()? {
                IncrementalDriverState::Item(change) => batch.push(change),
                IncrementalDriverState::Pending => break,
                IncrementalDriverState::Done => {
                    self.done = true;
                    break;
                }
            }
        }
        Ok(batch)
    }

    async fn next_deltas_batch(
        &mut self,
    ) -> Option<Result<QueryResultsDelta<'static>, QueryEvaluationError>> {
        loop {
            let baseline = self.driver.change_generation();
            let deltas = match self.deltas() {
                Ok(deltas) => deltas,
                Err(error) => return Some(Err(error)),
            };
            if !deltas.is_empty() {
                return Some(Ok(deltas));
            }
            if self.done {
                return None;
            }
            if let Err(error) = self.driver.wait_for_change(baseline).await {
                return Some(Err(error));
            }
        }
    }
}

/// Iterator returned by [`IncrementalQueryDeltasState::iter_deltas`].
pub struct IncrementalQueryDeltasIter<'s, 'a, D: IncrementalQueryableDataset<'a>> {
    state: &'s mut IncrementalQueryDeltasState<'a, D>,
}

impl<'a, D: IncrementalQueryableDataset<'a>> IncrementalQueryDeltasIter<'_, 'a, D> {
    /// Returns the next change batch, awaiting a dataset change if necessary.
    pub async fn next(
        &mut self,
    ) -> Option<Result<QueryResultsDelta<'static>, QueryEvaluationError>> {
        self.state.next_deltas_batch().await
    }
}

impl QueryResultsDelta<'_> {
    fn is_empty(&self) -> bool {
        match self {
            Self::Solutions(values) => values.size_hint().1 == Some(0),
            Self::Boolean(values) => values.size_hint().1 == Some(0),
            Self::Graph(values) => values.size_hint().1 == Some(0),
        }
    }
}

impl IncrementalGraphOutput {
    fn new(template: Vec<TripleTemplate>) -> Self {
        Self {
            template,
            solution_buckets: FxHashMap::default(),
            triple_counts: FxHashMap::default(),
        }
    }

    fn apply(&mut self, changes: Vec<Delta<QuerySolution>>) -> Vec<Delta<Triple>> {
        let mut original_counts = FxHashMap::default();
        for change in changes {
            match change {
                Delta::Addition(solution) => {
                    let triples = instantiate_template(&self.template, &solution);
                    for triple in &triples {
                        original_counts
                            .entry(triple.clone())
                            .or_insert_with(|| *self.triple_counts.get(triple).unwrap_or(&0));
                        let count = self.triple_counts.entry(triple.clone()).or_default();
                        *count += 1;
                    }
                    self.solution_buckets
                        .entry(solution_key(&solution))
                        .or_default()
                        .push((solution, triples));
                }
                Delta::Deletion(solution) => {
                    let key = solution_key(&solution);
                    let Some(bucket) = self.solution_buckets.get_mut(&key) else {
                        continue;
                    };
                    let Some(position) = bucket
                        .iter()
                        .position(|(stored, _)| stored.values() == solution.values())
                    else {
                        continue;
                    };
                    let (_, triples) = bucket.swap_remove(position);
                    let remove_bucket = bucket.is_empty();
                    if remove_bucket {
                        self.solution_buckets.remove(&key);
                    }
                    for triple in triples {
                        original_counts
                            .entry(triple.clone())
                            .or_insert_with(|| *self.triple_counts.get(&triple).unwrap_or(&0));
                        let Some(count) = self.triple_counts.get_mut(&triple) else {
                            continue;
                        };
                        *count -= 1;
                        if *count == 0 {
                            self.triple_counts.remove(&triple);
                        }
                    }
                }
            }
        }
        original_counts
            .into_iter()
            .filter_map(|(triple, old_count)| {
                let new_count = *self.triple_counts.get(&triple).unwrap_or(&0);
                match (old_count == 0, new_count == 0) {
                    (true, false) => Some(Delta::Addition(triple)),
                    (false, true) => Some(Delta::Deletion(triple)),
                    _ => None,
                }
            })
            .collect()
    }
}

fn solution_key(solution: &QuerySolution) -> u64 {
    let mut hasher = FxHasher::default();
    solution.values().hash(&mut hasher);
    hasher.finish()
}

fn instantiate_template(template: &[TripleTemplate], solution: &QuerySolution) -> Vec<Triple> {
    let mut bnodes = FxHashMap::default();
    template
        .iter()
        .filter_map(|template| {
            let subject = instantiate_term(&template.subject, solution, &mut bnodes)?;
            let predicate = match &template.predicate {
                NamedNodePattern::NamedNode(value) => value.clone(),
                NamedNodePattern::Variable(variable) => {
                    solution.get(variable)?.clone().try_into().ok()?
                }
            };
            let object = instantiate_term(&template.object, solution, &mut bnodes)?;
            let subject: NamedOrBlankNode = subject.try_into().ok()?;
            Some(Triple::new(subject, predicate, object))
        })
        .collect()
}

fn instantiate_term(
    template: &TermTemplate,
    solution: &QuerySolution,
    bnodes: &mut FxHashMap<BlankNode, BlankNode>,
) -> Option<Term> {
    Some(match template {
        TermTemplate::NamedNode(value) => value.clone().into(),
        TermTemplate::BlankNode(value) => bnodes.entry(value.clone()).or_default().clone().into(),
        TermTemplate::Literal(value) => value.clone().into(),
        TermTemplate::Variable(variable) => solution.get(variable)?.clone(),
        #[cfg(feature = "sparql-12")]
        TermTemplate::Triple(value) => {
            let subject: NamedOrBlankNode = instantiate_term(&value.subject, solution, bnodes)?
                .try_into()
                .ok()?;
            let predicate = match &value.predicate {
                NamedNodePattern::NamedNode(value) => value.clone(),
                NamedNodePattern::Variable(variable) => {
                    solution.get(variable)?.clone().try_into().ok()?
                }
            };
            let object = instantiate_term(&value.object, solution, bnodes)?;
            Triple::new(subject, predicate, object).into()
        }
    })
}
