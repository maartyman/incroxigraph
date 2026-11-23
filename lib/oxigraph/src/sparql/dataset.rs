use crate::model::Term;
use crate::storage::numeric_encoder::{
    Decoder, EncodedQuad, EncodedTerm, EncodedTriple, StrHash, StrHashHasher, StrLookup,
    insert_term,
};
use crate::storage::{CorruptionError, DecodingQuadIterator, StorageError, StorageReader};
use oxsdatatypes::Boolean;
use oxstr::OxString;
#[cfg(feature = "rdf-12")]
use spareval::ExpressionTriple;
use spareval::{
    Delta, ExpressionTerm, IncrementalQueryNotifier, IncrementalQueryableDataset, InternalQuad,
    InternalTriple, QueryableDataset, StreamingItem,
};
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::vec_deque::IntoIter;
use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasherDefault;
use std::sync::{Arc, Weak};

pub struct DatasetView<'a> {
    reader: Arc<StorageReader<'a>>,
    extra: RefCell<HashMap<StrHash, OxString, BuildHasherDefault<StrHashHasher>>>,
}

impl<'a> DatasetView<'a> {
    pub fn new(reader: StorageReader<'a>) -> Self {
        Self {
            reader: Arc::new(reader),
            extra: RefCell::new(HashMap::default()),
        }
    }

    pub fn insert_str(&self, key: &StrHash, value: OxString) {
        if let Entry::Vacant(e) = self.extra.borrow_mut().entry(*key) {
            if !matches!(self.reader.contains_str(key), Ok(true)) {
                e.insert(value);
            }
        }
    }
}

impl<'a> QueryableDataset<'a> for DatasetView<'a> {
    type InternalTerm = EncodedTerm;
    type Error = StorageError;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&EncodedTerm>,
        predicate: Option<&EncodedTerm>,
        object: Option<&EncodedTerm>,
        graph_name: Option<Option<&EncodedTerm>>,
    ) -> impl Iterator<Item = Result<InternalQuad<EncodedTerm>, StorageError>> + use<'a> {
        self.reader
            .quads_for_pattern(
                subject,
                predicate,
                object,
                graph_name.map(|graph_name| graph_name.unwrap_or(&EncodedTerm::DefaultGraph)),
            )
            .map(|q| Ok(q?.into()))
    }

    fn internal_triples_for_pattern(
        &self,
        subject: Option<&EncodedTerm>,
        predicate: Option<&EncodedTerm>,
        object: Option<&EncodedTerm>,
        graph_names: Option<&[Option<EncodedTerm>]>,
    ) -> impl Iterator<Item = Result<InternalTriple<EncodedTerm>, StorageError>> + use<'a> {
        self.reader
            .triples_for_pattern(subject, predicate, object, graph_names)
            .map(|q| Ok(q?.into()))
    }

    fn internal_named_graphs(
        &self,
    ) -> impl Iterator<Item = Result<EncodedTerm, StorageError>> + use<'a> {
        self.reader.named_graphs()
    }

    fn contains_internal_graph_name(&self, graph_name: &EncodedTerm) -> Result<bool, StorageError> {
        self.reader.contains_named_graph(graph_name)
    }

    fn internalize_term(&self, term: Term) -> Result<EncodedTerm, StorageError> {
        let encoded = (&term).into();
        insert_term(term, &encoded, &mut |key, value| {
            self.insert_str(key, value)
        });
        Ok(encoded)
    }

    fn externalize_term(&self, term: EncodedTerm) -> Result<Term, StorageError> {
        self.decode_term(&term)
    }

    fn externalize_expression_term(
        &self,
        term: EncodedTerm,
    ) -> Result<ExpressionTerm, StorageError> {
        Ok(match term {
            EncodedTerm::DefaultGraph => {
                return Err(CorruptionError::new("Unexpected default graph").into());
            }
            EncodedTerm::BooleanLiteral(value) => ExpressionTerm::BooleanLiteral(value),
            EncodedTerm::FloatLiteral(value) => ExpressionTerm::FloatLiteral(value),
            EncodedTerm::DoubleLiteral(value) => ExpressionTerm::DoubleLiteral(value),
            EncodedTerm::IntegerLiteral(value) => ExpressionTerm::IntegerLiteral(value),
            EncodedTerm::DecimalLiteral(value) => ExpressionTerm::DecimalLiteral(value),
            EncodedTerm::DateTimeLiteral(value) => ExpressionTerm::DateTimeLiteral(value),
            EncodedTerm::TimeLiteral(value) => ExpressionTerm::TimeLiteral(value),
            EncodedTerm::DateLiteral(value) => ExpressionTerm::DateLiteral(value),
            EncodedTerm::GYearMonthLiteral(value) => ExpressionTerm::GYearMonthLiteral(value),
            EncodedTerm::GYearLiteral(value) => ExpressionTerm::GYearLiteral(value),
            EncodedTerm::GMonthDayLiteral(value) => ExpressionTerm::GMonthDayLiteral(value),
            EncodedTerm::GDayLiteral(value) => ExpressionTerm::GDayLiteral(value),
            EncodedTerm::GMonthLiteral(value) => ExpressionTerm::GMonthLiteral(value),
            EncodedTerm::DurationLiteral(value) => ExpressionTerm::DurationLiteral(value),
            EncodedTerm::YearMonthDurationLiteral(value) => {
                ExpressionTerm::YearMonthDurationLiteral(value)
            }
            EncodedTerm::DayTimeDurationLiteral(value) => {
                ExpressionTerm::DayTimeDurationLiteral(value)
            }
            #[cfg(feature = "rdf-12")]
            EncodedTerm::Triple(t) => ExpressionTriple::new(
                self.externalize_expression_term(t.subject.clone())?,
                self.externalize_expression_term(t.predicate.clone())?,
                self.externalize_expression_term(t.object.clone())?,
            )
            .ok_or_else(|| CorruptionError::msg("Invalid triple term in the storage"))?
            .into(),
            _ => self.decode_term(&term)?.into(), // No escape
        })
    }

    fn internalize_expression_term(
        &self,
        term: ExpressionTerm,
    ) -> Result<EncodedTerm, StorageError> {
        Ok(match term {
            ExpressionTerm::BooleanLiteral(value) => EncodedTerm::BooleanLiteral(value),
            ExpressionTerm::FloatLiteral(value) => EncodedTerm::FloatLiteral(value),
            ExpressionTerm::DoubleLiteral(value) => EncodedTerm::DoubleLiteral(value),
            ExpressionTerm::IntegerLiteral(value) => EncodedTerm::IntegerLiteral(value),
            ExpressionTerm::DecimalLiteral(value) => EncodedTerm::DecimalLiteral(value),
            ExpressionTerm::DateTimeLiteral(value) => EncodedTerm::DateTimeLiteral(value),
            ExpressionTerm::TimeLiteral(value) => EncodedTerm::TimeLiteral(value),
            ExpressionTerm::DateLiteral(value) => EncodedTerm::DateLiteral(value),
            ExpressionTerm::GYearMonthLiteral(value) => EncodedTerm::GYearMonthLiteral(value),
            ExpressionTerm::GYearLiteral(value) => EncodedTerm::GYearLiteral(value),
            ExpressionTerm::GMonthDayLiteral(value) => EncodedTerm::GMonthDayLiteral(value),
            ExpressionTerm::GDayLiteral(value) => EncodedTerm::GDayLiteral(value),
            ExpressionTerm::GMonthLiteral(value) => EncodedTerm::GMonthLiteral(value),
            ExpressionTerm::DurationLiteral(value) => EncodedTerm::DurationLiteral(value),
            ExpressionTerm::YearMonthDurationLiteral(value) => {
                EncodedTerm::YearMonthDurationLiteral(value)
            }
            ExpressionTerm::DayTimeDurationLiteral(value) => {
                EncodedTerm::DayTimeDurationLiteral(value)
            }
            #[cfg(feature = "rdf-12")]
            ExpressionTerm::Triple(t) => EncodedTerm::Triple(Arc::new(EncodedTriple {
                subject: self.internalize_expression_term(t.subject.into())?,
                predicate: self.internalize_expression_term(t.predicate.into())?,
                object: self.internalize_expression_term(t.object)?,
            })),
            _ => self.internalize_term(term.into())?, // No fast path
        })
    }

    fn internal_term_effective_boolean_value(
        &self,
        term: EncodedTerm,
    ) -> Result<Option<bool>, StorageError> {
        Ok(match term {
            EncodedTerm::BooleanLiteral(value) => Some(value.into()),
            EncodedTerm::SmallStringLiteral(value) => Some(!value.is_empty()),
            EncodedTerm::BigStringLiteral { .. } => {
                Some(true) // A big literal can't be empty
            }
            EncodedTerm::FloatLiteral(value) => Some(Boolean::from(value).into()),
            EncodedTerm::DoubleLiteral(value) => Some(Boolean::from(value).into()),
            EncodedTerm::IntegerLiteral(value) => Some(Boolean::from(value).into()),
            EncodedTerm::DecimalLiteral(value) => Some(Boolean::from(value).into()),
            _ => None,
        })
    }
}

impl From<EncodedQuad> for InternalQuad<EncodedTerm> {
    fn from(quad: EncodedQuad) -> Self {
        Self {
            subject: quad.subject,
            predicate: quad.predicate,
            object: quad.object,
            graph_name: if quad.graph_name.is_default_graph() {
                None
            } else {
                Some(quad.graph_name)
            },
        }
    }
}

impl From<EncodedTriple> for InternalTriple<EncodedTerm> {
    fn from(triple: EncodedTriple) -> Self {
        Self {
            subject: triple.subject,
            predicate: triple.predicate,
            object: triple.object,
        }
    }
}

impl<'a> IncrementalQueryableDataset<'a> for DatasetView<'a> {
    fn internal_quad_deltas_for_pattern(
        &self,
        subject: Option<&EncodedTerm>,
        predicate: Option<&EncodedTerm>,
        object: Option<&EncodedTerm>,
        graph_name: Option<Option<&EncodedTerm>>,
        notifier: Weak<IncrementalQueryNotifier>,
    ) -> impl Iterator<Item = Result<StreamingItem<Delta<InternalQuad<EncodedTerm>>>, StorageError>>
    + use<'a> {
        Box::new(LiveQuadDeltaSource::new(
            Arc::clone(&self.reader),
            subject,
            predicate,
            object,
            graph_name,
            notifier,
        ))
    }
}

struct LiveQuadDeltaSource<'a> {
    reader: Arc<StorageReader<'a>>,
    snapshot_iter: DecodingQuadIterator<'a>,
    subscription_id: Option<usize>,
    snapshot_done: bool,
    live_buffer: IntoIter<Delta<EncodedQuad>>,
}

impl<'a> LiveQuadDeltaSource<'a> {
    fn new(
        reader: Arc<StorageReader<'a>>,
        subject: Option<&EncodedTerm>,
        predicate: Option<&EncodedTerm>,
        object: Option<&EncodedTerm>,
        graph_name: Option<Option<&EncodedTerm>>,
        notifier: Weak<IncrementalQueryNotifier>,
    ) -> Self {
        let (subscription_id, reader) = if reader.can_refresh() {
            let (subscription_id, fresh_reader) = reader
                .subscribe_incremental_quads(subject, predicate, object, graph_name, notifier);
            (
                Some(subscription_id),
                fresh_reader.map(Arc::new).unwrap_or(reader),
            )
        } else {
            // A transaction reader is an isolated snapshot. It must not be combined with
            // committed-store notifications, which could duplicate or contradict its rows.
            (None, reader)
        };
        let snapshot_iter = reader.quads_for_pattern(
            subject,
            predicate,
            object,
            graph_name.map(|graph_name| graph_name.unwrap_or(&EncodedTerm::DefaultGraph)),
        );
        Self {
            reader,
            snapshot_iter,
            subscription_id,
            snapshot_done: false,
            live_buffer: VecDeque::new().into_iter(),
        }
    }

    fn map_quad(quad: EncodedQuad) -> InternalQuad<EncodedTerm> {
        InternalQuad {
            subject: quad.subject,
            predicate: quad.predicate,
            object: quad.object,
            graph_name: (!quad.graph_name.is_default_graph()).then_some(quad.graph_name),
        }
    }

    fn next_live_change(&mut self) -> Option<Delta<EncodedQuad>> {
        if let Some(change) = self.live_buffer.next() {
            return Some(change);
        }
        let subscription_id = self.subscription_id?;
        self.live_buffer = self
            .reader
            .incremental_quad_subscription_changes(subscription_id)
            .into_iter();
        self.live_buffer.next()
    }
}

impl Iterator for LiveQuadDeltaSource<'_> {
    type Item = Result<StreamingItem<Delta<InternalQuad<EncodedTerm>>>, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.snapshot_done {
            if let Some(quad) = self.snapshot_iter.next() {
                return Some(
                    quad.map(Self::map_quad)
                        .map(Delta::addition)
                        .map(StreamingItem::Item),
                );
            }
            self.snapshot_done = true;
        }
        self.next_live_change()
            .map(|change| Ok(StreamingItem::Item(change.map(Self::map_quad))))
            .or_else(|| self.subscription_id.map(|_| Ok(StreamingItem::Pending)))
    }
}

impl Drop for LiveQuadDeltaSource<'_> {
    fn drop(&mut self) {
        if let Some(subscription_id) = self.subscription_id {
            self.reader.unsubscribe_incremental_quads(subscription_id);
        }
    }
}

impl StrLookup for DatasetView<'_> {
    fn get_str(&self, key: &StrHash) -> Result<Option<OxString>, StorageError> {
        Ok(if let Some(value) = self.extra.borrow().get(key) {
            Some(value.clone())
        } else {
            self.reader.get_str(key)?
        })
    }
}
