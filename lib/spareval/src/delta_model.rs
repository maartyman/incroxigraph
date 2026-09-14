use crate::{
    IncrementalDriverState, IncrementalQueryableDataset, IncrementalSelectDriver,
    QueryEvaluationError, QuerySolution, QuerySolutionIter, QueryTripleIter,
};
use oxrdf::{Triple, Variable};
use rustc_hash::{FxHashMap, FxHasher};
use sparesults::QuerySolutionRef;
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
/// Each item is the query's boolean result after a change flipped it.
pub struct QueryBooleanDeltaIter {
    iter: std::vec::IntoIter<bool>,
}

impl QueryBooleanDeltaIter {
    #[expect(dead_code)]
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

/// Stateful handle for complete results of an incremental SELECT query.
pub struct IncrementalSelectResultsState<'a, D: IncrementalQueryableDataset<'a>> {
    driver: IncrementalSelectDriver<'a, D>,
    variables: Arc<[Variable]>,
    /// Current result multiset, retaining one owned solution and its multiplicity per distinct row.
    current: QuerySolutionSet,
    done: bool,
    emitted_initial: bool,
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

impl<'a, D: IncrementalQueryableDataset<'a>> IncrementalSelectResultsState<'a, D> {
    pub(crate) fn new(driver: IncrementalSelectDriver<'a, D>) -> Self {
        let variables = driver.variables();
        Self {
            driver,
            variables,
            current: QuerySolutionSet::new(),
            done: false,
            emitted_initial: false,
        }
    }

    /// The variables produced by the query, in projection order.
    #[inline]
    pub fn variables(&self) -> &[Variable] {
        &self.variables
    }

    /// Processes all currently pending dataset changes and returns the current complete result set.
    ///
    /// This is the pull-based counterpart of a regular query execution: calling it again after the
    /// dataset changed returns the updated results. The returned iterator borrows the state, which
    /// prevents mixing it with the other access modes until it is dropped.
    pub fn results(&mut self) -> Result<IncrementalSelectResults<'_>, QueryEvaluationError> {
        self.drain_ready()?;
        Ok(IncrementalSelectResults::new(self.current.iter()))
    }

    /// Asynchronously iterates over complete result snapshots.
    ///
    /// The first `next().await` yields the initial snapshot. Each later `next().await` drains any
    /// changes that are ready and, if there are some, yields a fresh complete snapshot; otherwise it
    /// awaits the next committed dataset change and then yields the updated snapshot. It resolves to
    /// `None` only once the underlying query is exhausted (e.g. a static dataset).
    pub fn iter_results(&mut self) -> IncrementalSelectResultsIter<'_, 'a, D> {
        IncrementalSelectResultsIter { state: self }
    }

    fn drain_ready(&mut self) -> Result<bool, QueryEvaluationError> {
        let mut changed = false;
        if self.done {
            return Ok(changed);
        }
        loop {
            match self.driver.poll_next()? {
                IncrementalDriverState::Item(change) => {
                    changed = true;
                    let (kind, solution) = change.into_parts();
                    match kind {
                        DeltaKind::Addition => self.current.insert(solution),
                        DeltaKind::Deletion => self.current.remove_one(&solution),
                    }
                }
                IncrementalDriverState::Pending => break,
                IncrementalDriverState::Done => {
                    self.done = true;
                    break;
                }
            }
        }
        Ok(changed)
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
    ) -> Option<Result<IncrementalSelectResults<'_>, QueryEvaluationError>> {
        loop {
            let baseline = self.driver.change_generation();
            let changed = match self.drain_ready() {
                Ok(changed) => changed,
                Err(error) => return Some(Err(error)),
            };
            if !self.emitted_initial {
                self.emitted_initial = true;
                return Some(Ok(IncrementalSelectResults::new(self.current.iter())));
            }
            if changed {
                return Some(Ok(IncrementalSelectResults::new(self.current.iter())));
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

/// Iterator returned by [`IncrementalSelectResultsState::iter_results`].
pub struct IncrementalSelectResultsIter<'s, 'a, D: IncrementalQueryableDataset<'a>> {
    state: &'s mut IncrementalSelectResultsState<'a, D>,
}

impl<'a, D: IncrementalQueryableDataset<'a>> IncrementalSelectResultsIter<'_, 'a, D> {
    /// Returns the next complete result snapshot, awaiting a dataset change if necessary.
    pub async fn next(
        &mut self,
    ) -> Option<Result<IncrementalSelectResults<'_>, QueryEvaluationError>> {
        self.state.next_results_snapshot().await
    }
}

/// Stateful handle for deltas of an incremental SELECT query.
///
/// Unlike [`IncrementalSelectResultsState`], this state does not retain a result multiset.
pub struct IncrementalSelectDeltasState<'a, D: IncrementalQueryableDataset<'a>> {
    driver: IncrementalSelectDriver<'a, D>,
    variables: Arc<[Variable]>,
    done: bool,
}

impl<'a, D: IncrementalQueryableDataset<'a>> IncrementalSelectDeltasState<'a, D> {
    pub(crate) fn new(driver: IncrementalSelectDriver<'a, D>) -> Self {
        let variables = driver.variables();
        Self {
            driver,
            variables,
            done: false,
        }
    }

    /// The variables produced by the query, in projection order.
    #[inline]
    pub fn variables(&self) -> &[Variable] {
        &self.variables
    }

    /// Processes all currently pending dataset changes and returns them.
    pub fn deltas(&mut self) -> Result<QueryResultsDelta<'static>, QueryEvaluationError> {
        let batch = self.drain_ready()?;
        Ok(QueryResultsDelta::Solutions(QuerySolutionDeltaIter::new(
            Arc::clone(&self.variables),
            batch.into_iter().map(Ok),
        )))
    }

    /// Asynchronously iterates over non-empty batches of query changes.
    pub fn iter_deltas(&mut self) -> IncrementalSelectDeltasIter<'_, 'a, D> {
        IncrementalSelectDeltasIter { state: self }
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
            let batch = match self.drain_ready() {
                Ok(batch) => batch,
                Err(error) => return Some(Err(error)),
            };
            if !batch.is_empty() {
                return Some(Ok(QueryResultsDelta::Solutions(
                    QuerySolutionDeltaIter::new(
                        Arc::clone(&self.variables),
                        batch.into_iter().map(Ok),
                    ),
                )));
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

/// Iterator returned by [`IncrementalSelectDeltasState::iter_deltas`].
pub struct IncrementalSelectDeltasIter<'s, 'a, D: IncrementalQueryableDataset<'a>> {
    state: &'s mut IncrementalSelectDeltasState<'a, D>,
}

impl<'a, D: IncrementalQueryableDataset<'a>> IncrementalSelectDeltasIter<'_, 'a, D> {
    /// Returns the next change batch, awaiting a dataset change if necessary.
    pub async fn next(
        &mut self,
    ) -> Option<Result<QueryResultsDelta<'static>, QueryEvaluationError>> {
        self.state.next_deltas_batch().await
    }
}
