#![cfg(test)]
#![expect(clippy::panic_in_result_fn)]

use incroxigraph::io::RdfFormat;
use incroxigraph::model::vocab::{rdf, xsd};
use incroxigraph::model::*;
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
use incroxigraph::sparql::QueryResults;
use incroxigraph::sparql::{
    Delta, IncrementalQueryResults, QueryResultsDelta, SparqlEvaluator,
    StoreIncrementalQueryDeltasState, StoreIncrementalQueryResultsState,
};
use incroxigraph::store::Store;
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
use incroxigraph::store::StoreOptions;
use std::error::Error;
#[cfg(all(target_os = "linux", feature = "rocksdb"))]
use std::fs::remove_dir_all;
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
use std::fs::{File, create_dir_all, read_dir, remove_dir};
#[cfg(all(
    target_os = "linux",
    target_pointer_width = "64",
    target_endian = "little",
    feature = "rocksdb"
))]
use std::fs::{read, write};
use std::future::Future;
#[cfg(all(
    target_os = "linux",
    target_pointer_width = "64",
    target_endian = "little",
    feature = "rocksdb"
))]
use std::io;
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
use std::io::Write;
use std::iter::empty;
#[cfg(all(target_os = "linux", feature = "rocksdb"))]
use std::iter::once;
#[cfg(all(
    target_os = "linux",
    target_pointer_width = "64",
    target_endian = "little",
    feature = "rocksdb"
))]
use std::path::PathBuf;
use std::task::{Context, Poll, Waker};
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
use tempfile::TempDir;

#[expect(clippy::non_ascii_literal)]
const DATA: &str = r#"
@prefix schema: <http://schema.org/> .
@prefix wd: <http://www.wikidata.org/entity/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

wd:Q90 a schema:City ;
    schema:name "Paris"@fr , "la ville lumière"@fr ;
    schema:country wd:Q142 ;
    schema:population 2000000 ;
    schema:startDate "-300"^^xsd:gYear ;
    schema:url "https://www.paris.fr/"^^xsd:anyURI ;
    schema:postalCode "75001" .
"#;

#[test]
fn incremental_query_deltas_support_all_result_kinds() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;

    let mut select = SparqlEvaluator::new()
        .parse_query("SELECT * WHERE { ?s <urn:p> ?o }")?
        .on_store(&store)
        .execute_incremental_deltas()?;
    assert_store_incremental_state(&select);
    let QueryResultsDelta::Solutions(values) = select.deltas()? else {
        unreachable!("expected solution deltas")
    };
    assert_eq!(values.count(), 0);

    let mut ask = SparqlEvaluator::new()
        .parse_query("ASK { ?s <urn:p> ?o }")?
        .on_store(&store)
        .execute_incremental_deltas()?;
    let QueryResultsDelta::Boolean(values) = ask.deltas()? else {
        unreachable!("expected boolean deltas")
    };
    assert_eq!(values.collect::<Result<Vec<_>, _>>()?, [false]);

    let quad = Quad::new(
        NamedNode::new_unchecked("urn:s"),
        NamedNode::new_unchecked("urn:p"),
        NamedNode::new_unchecked("urn:o"),
        GraphName::DefaultGraph,
    );
    store.insert(quad.clone())?;

    let QueryResultsDelta::Boolean(values) = ask.deltas()? else {
        unreachable!("expected boolean deltas")
    };
    assert_eq!(values.collect::<Result<Vec<_>, _>>()?, [true]);

    let mut construct = SparqlEvaluator::new()
        .parse_query("CONSTRUCT { _:b <urn:q> ?o } WHERE { ?s <urn:p> ?o }")?
        .on_store(&store)
        .execute_incremental_deltas()?;
    let QueryResultsDelta::Graph(values) = construct.deltas()? else {
        unreachable!("expected graph deltas")
    };
    let initial = values.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(initial.len(), 1);
    let Delta::Addition(initial) = &initial[0] else {
        unreachable!("expected a triple addition")
    };
    assert!(initial.subject.is_blank_node());

    store.remove(&quad)?;
    let QueryResultsDelta::Graph(values) = construct.deltas()? else {
        unreachable!("expected graph deltas")
    };
    let removed = values.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(removed.len(), 1);
    let Delta::Deletion(removed) = &removed[0] else {
        unreachable!("expected a triple deletion")
    };
    assert_eq!(removed, initial);
    Ok(())
}

fn assert_store_incremental_state(_: &StoreIncrementalQueryDeltasState) {}

fn solution_deltas(
    state: &mut StoreIncrementalQueryDeltasState,
    variable: &str,
) -> Result<Vec<(bool, Option<Term>)>, Box<dyn Error>> {
    let QueryResultsDelta::Solutions(deltas) = state.deltas()? else {
        unreachable!("expected solution deltas")
    };
    Ok(deltas
        .map(|delta| {
            let delta = delta?;
            Ok((
                matches!(delta, Delta::Addition(_)),
                delta.value().get(variable).cloned(),
            ))
        })
        .collect::<Result<_, incroxigraph::sparql::QueryEvaluationError>>()?)
}

fn incremental_test_quad(predicate: &'static str, object: impl Into<Term>) -> Quad {
    Quad::new(
        NamedNode::new_unchecked("urn:s"),
        NamedNode::new_unchecked(predicate),
        object,
        GraphName::DefaultGraph,
    )
}

#[test]
fn incremental_union_preserves_duplicates_and_polls_live_branches() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let mut state = SparqlEvaluator::new()
        .parse_query(
            "SELECT ?v WHERE {
                { <urn:s> <urn:p> ?v } UNION { VALUES ?v { 1 } } UNION { <urn:s> <urn:q> ?v }
            }",
        )?
        .on_store(&store)
        .execute_incremental_deltas()?;
    let value = Literal::from(1);
    let p = incremental_test_quad("urn:p", value.clone());
    let q = incremental_test_quad("urn:q", value.clone());
    let value = Some(value.into());
    assert_eq!(solution_deltas(&mut state, "v")?, [(true, value.clone())]);
    store.insert(q.clone())?;
    assert_eq!(solution_deltas(&mut state, "v")?, [(true, value.clone())]);
    store.insert(p.clone())?;
    assert_eq!(solution_deltas(&mut state, "v")?, [(true, value.clone())]);
    store.remove(&q)?;
    store.remove(&p)?;
    assert_eq!(solution_deltas(&mut state, "v")?, vec![(false, value); 2]);
    Ok(())
}

#[test]
fn incremental_minus_restores_left_duplicates_after_last_match() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let left = incremental_test_quad("urn:left", Literal::from(1));
    let blocker = incremental_test_quad("urn:blocker", Literal::from(1));
    let second_blocker = incremental_test_quad("urn:second-blocker", Literal::from(1));
    store.insert(left.clone())?;
    store.insert(blocker.clone())?;
    let mut state = SparqlEvaluator::new()
        .parse_query(
            "SELECT ?v WHERE {
                { { ?s <urn:left> ?v } UNION { ?s <urn:left> ?v } }
                MINUS {
                    { ?s <urn:blocker> ?v } UNION { ?s <urn:second-blocker> ?v }
                    UNION { ?other <urn:noise> ?unused }
                }
            }",
        )?
        .on_store(&store)
        .execute_incremental_deltas()?;
    assert!(solution_deltas(&mut state, "v")?.is_empty());
    store.insert(second_blocker.clone())?;
    assert!(solution_deltas(&mut state, "v")?.is_empty());
    store.remove(&blocker)?;
    assert!(solution_deltas(&mut state, "v")?.is_empty());
    store.remove(&second_blocker)?;
    let value = Some(Literal::from(1).into());
    let additions = vec![(true, value.clone()); 2];
    let deletions = vec![(false, value); 2];
    assert_eq!(solution_deltas(&mut state, "v")?, additions);
    store.insert(incremental_test_quad("urn:blocker", Literal::from(2)))?;
    store.insert(incremental_test_quad("urn:noise", Literal::from(1)))?;
    assert!(solution_deltas(&mut state, "v")?.is_empty());
    store.insert(blocker.clone())?;
    assert_eq!(solution_deltas(&mut state, "v")?, deletions);
    store.remove(&left)?;
    assert!(solution_deltas(&mut state, "v")?.is_empty());
    store.remove(&blocker)?;
    assert!(solution_deltas(&mut state, "v")?.is_empty());
    Ok(())
}

#[test]
fn incremental_optional_switches_between_unmatched_and_joined_rows() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let left = incremental_test_quad("urn:left", Literal::from(1));
    let matched = incremental_test_quad("urn:right", Literal::from(2));
    let duplicate = incremental_test_quad("urn:other", Literal::from(2));
    store.insert(left.clone())?;
    store.insert(incremental_test_quad("urn:right", Literal::from(0)))?;
    store.insert(incremental_test_quad("urn:right", Literal::from("error")))?;
    let mut state = SparqlEvaluator::new()
        .parse_query(
            "SELECT ?v WHERE {
                ?s <urn:left> ?limit
                OPTIONAL {
                    { SELECT ?s ?v WHERE { { ?s <urn:right> ?v } UNION { ?s <urn:other> ?v } } }
                    FILTER(?v > ?limit)
                }
            }",
        )?
        .on_store(&store)
        .execute_incremental_deltas()?;
    let value = Some(Literal::from(2).into());
    assert_eq!(solution_deltas(&mut state, "v")?, [(true, None)]);
    store.insert(matched.clone())?;
    assert_eq!(
        solution_deltas(&mut state, "v")?,
        [(false, None), (true, value.clone())]
    );
    store.insert(duplicate.clone())?;
    assert_eq!(solution_deltas(&mut state, "v")?, [(true, value.clone())]);
    store.remove(&left)?;
    assert_eq!(
        solution_deltas(&mut state, "v")?,
        vec![(false, value.clone()); 2]
    );
    store.insert(left)?;
    assert_eq!(
        solution_deltas(&mut state, "v")?,
        vec![(true, value.clone()); 2]
    );
    store.remove(&matched)?;
    assert_eq!(solution_deltas(&mut state, "v")?, [(false, value.clone())]);
    store.remove(&duplicate)?;
    assert_eq!(
        solution_deltas(&mut state, "v")?,
        [(false, value), (true, None)]
    );
    Ok(())
}

#[test]
fn incremental_optional_keeps_a_row_when_first_match_adds_no_bindings() -> Result<(), Box<dyn Error>>
{
    let store = Store::new()?;
    store.insert(incremental_test_quad("urn:left", Literal::from(1)))?;
    let mut state = SparqlEvaluator::new()
        .without_optimizations()
        .parse_query("SELECT ?s WHERE { ?s <urn:left> 1 OPTIONAL { ?s <urn:right> 2 } }")?
        .on_store(&store)
        .execute_incremental_deltas()?;
    assert_eq!(
        solution_deltas(&mut state, "s")?,
        [(true, Some(NamedNode::new_unchecked("urn:s").into()))]
    );
    let right = incremental_test_quad("urn:right", Literal::from(2));
    store.insert(right.clone())?;
    assert!(solution_deltas(&mut state, "s")?.is_empty());
    store.remove(&right)?;
    assert!(solution_deltas(&mut state, "s")?.is_empty());
    Ok(())
}

#[test]
fn incremental_filter_evaluates_additions_and_deletions() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let mut state = SparqlEvaluator::new()
        .parse_query("SELECT ?v WHERE { ?s <urn:p> ?v FILTER(?v > 1) }")?
        .on_store(&store)
        .execute_incremental_deltas()?;
    let quad = incremental_test_quad("urn:p", Literal::from(2));
    let value = Some(Literal::from(2).into());
    store.insert(quad.clone())?;
    assert_eq!(solution_deltas(&mut state, "v")?, [(true, value.clone())]);
    store.remove(&quad)?;
    assert_eq!(solution_deltas(&mut state, "v")?, [(false, value)]);
    for value in [Literal::from(0), Literal::from("error")] {
        let rejected = incremental_test_quad("urn:p", value);
        store.insert(rejected.clone())?;
        assert!(solution_deltas(&mut state, "v")?.is_empty());
        store.remove(&rejected)?;
        assert!(solution_deltas(&mut state, "v")?.is_empty());
    }
    Ok(())
}

#[test]
fn incremental_rejects_existence_filters() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    for query in [
        "SELECT * WHERE { ?s <urn:p> ?v FILTER EXISTS { ?s <urn:q> ?w } }",
        "SELECT * WHERE { ?s <urn:p> ?v FILTER(IF(?v > 1, NOT EXISTS { ?s <urn:q> ?w }, true)) }",
        "SELECT * WHERE {
            ?s <urn:p> ?v OPTIONAL { ?s <urn:q> ?w FILTER EXISTS { ?s <urn:r> ?v } }
        }",
    ] {
        let result = SparqlEvaluator::new()
            .parse_query(query)?
            .on_store(&store)
            .execute_incremental_deltas();
        assert!(result.is_err(), "accepted existence filter in {query}");
    }
    Ok(())
}

#[test]
fn incremental_query_results_support_all_result_kinds() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let mut select = SparqlEvaluator::new()
        .parse_query("SELECT * WHERE { ?s <urn:p> ?o }")?
        .on_store(&store)
        .execute_incremental_results()?;
    let mut ask = SparqlEvaluator::new()
        .parse_query("ASK { ?s <urn:p> ?o }")?
        .on_store(&store)
        .execute_incremental_results()?;
    let mut construct = SparqlEvaluator::new()
        .parse_query("CONSTRUCT { _:b <urn:q> ?o } WHERE { ?s <urn:p> ?o }")?
        .on_store(&store)
        .execute_incremental_results()?;
    assert_store_incremental_results_state(&select);

    assert_eq!(select.variables().map(<[_]>::len), Some(2));
    let IncrementalQueryResults::Solutions(solutions) = select.results()? else {
        unreachable!("expected SELECT results")
    };
    assert_eq!(solutions.count(), 0);
    assert!(matches!(
        ask.results()?,
        IncrementalQueryResults::Boolean(false)
    ));
    let IncrementalQueryResults::Graph(triples) = construct.results()? else {
        unreachable!("expected CONSTRUCT results")
    };
    assert_eq!(triples.count(), 0);

    let quad = Quad::new(
        NamedNode::new_unchecked("urn:s"),
        NamedNode::new_unchecked("urn:p"),
        NamedNode::new_unchecked("urn:o"),
        GraphName::DefaultGraph,
    );
    store.insert(quad.clone())?;
    let IncrementalQueryResults::Solutions(solutions) = select.results()? else {
        unreachable!("expected SELECT results")
    };
    assert_eq!(solutions.count(), 1);
    assert!(matches!(
        ask.results()?,
        IncrementalQueryResults::Boolean(true)
    ));
    let IncrementalQueryResults::Graph(triples) = construct.results()? else {
        unreachable!("expected CONSTRUCT results")
    };
    let initial: Vec<_> = triples.cloned().collect();
    assert_eq!(initial.len(), 1);
    assert!(initial[0].subject.is_blank_node());

    store.remove(&quad)?;
    let IncrementalQueryResults::Solutions(solutions) = select.results()? else {
        unreachable!("expected SELECT results")
    };
    assert_eq!(solutions.count(), 0);
    assert!(matches!(
        ask.results()?,
        IncrementalQueryResults::Boolean(false)
    ));
    let IncrementalQueryResults::Graph(triples) = construct.results()? else {
        unreachable!("expected CONSTRUCT results")
    };
    assert_eq!(triples.count(), 0);
    Ok(())
}

fn assert_store_incremental_results_state(_: &StoreIncrementalQueryResultsState) {}

#[test]
fn incremental_query_results_keep_boolean_and_graph_set_semantics() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let mut ask = SparqlEvaluator::new()
        .parse_query("ASK { ?s <urn:p> ?o }")?
        .on_store(&store)
        .execute_incremental_results()?;
    let mut construct = SparqlEvaluator::new()
        .parse_query("CONSTRUCT { <urn:result> <urn:q> <urn:value> } WHERE { ?s <urn:p> ?o }")?
        .on_store(&store)
        .execute_incremental_results()?;
    let first = Quad::new(
        NamedNode::new_unchecked("urn:s1"),
        NamedNode::new_unchecked("urn:p"),
        NamedNode::new_unchecked("urn:o1"),
        GraphName::DefaultGraph,
    );
    let second = Quad::new(
        NamedNode::new_unchecked("urn:s2"),
        NamedNode::new_unchecked("urn:p"),
        NamedNode::new_unchecked("urn:o2"),
        GraphName::DefaultGraph,
    );

    store.insert(first.clone())?;
    store.insert(second.clone())?;
    assert!(matches!(
        ask.results()?,
        IncrementalQueryResults::Boolean(true)
    ));
    assert_eq!(graph_result_count(construct.results()?), 1);

    store.remove(&first)?;
    assert!(matches!(
        ask.results()?,
        IncrementalQueryResults::Boolean(true)
    ));
    assert_eq!(graph_result_count(construct.results()?), 1);

    store.remove(&second)?;
    assert!(matches!(
        ask.results()?,
        IncrementalQueryResults::Boolean(false)
    ));
    assert_eq!(graph_result_count(construct.results()?), 0);
    Ok(())
}

fn graph_result_count(results: IncrementalQueryResults<'_>) -> usize {
    let IncrementalQueryResults::Graph(triples) = results else {
        unreachable!("expected CONSTRUCT results")
    };
    triples.count()
}

#[test]
fn incremental_query_results_iterator_emits_initial_and_changed_snapshots()
-> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let mut ask = SparqlEvaluator::new()
        .parse_query("ASK { ?s <urn:p> ?o }")?
        .on_store(&store)
        .execute_incremental_results()?;
    let mut snapshots = ask.iter_results();
    assert!(matches!(
        poll_ready(snapshots.next()),
        Some(Ok(IncrementalQueryResults::Boolean(false)))
    ));

    let quad = Quad::new(
        NamedNode::new_unchecked("urn:s"),
        NamedNode::new_unchecked("urn:p"),
        NamedNode::new_unchecked("urn:o"),
        GraphName::DefaultGraph,
    );
    store.insert(quad.clone())?;
    assert!(matches!(
        poll_ready(snapshots.next()),
        Some(Ok(IncrementalQueryResults::Boolean(true)))
    ));
    store.remove(&quad)?;
    assert!(matches!(
        poll_ready(snapshots.next()),
        Some(Ok(IncrementalQueryResults::Boolean(false)))
    ));
    Ok(())
}

fn poll_ready<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => unreachable!("the dataset change should already be ready"),
    }
}

#[test]
fn incremental_construct_deltas_have_graph_set_semantics() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let mut state = SparqlEvaluator::new()
        .parse_query("CONSTRUCT { <urn:result> <urn:q> <urn:value> } WHERE { ?s <urn:p> ?o }")?
        .on_store(&store)
        .execute_incremental_deltas()?;

    let first = Quad::new(
        NamedNode::new_unchecked("urn:s1"),
        NamedNode::new_unchecked("urn:p"),
        NamedNode::new_unchecked("urn:o1"),
        GraphName::DefaultGraph,
    );
    let second = Quad::new(
        NamedNode::new_unchecked("urn:s2"),
        NamedNode::new_unchecked("urn:p"),
        NamedNode::new_unchecked("urn:o2"),
        GraphName::DefaultGraph,
    );

    store.insert(first.clone())?;
    assert_eq!(graph_delta_count(state.deltas()?), 1);
    store.insert(second.clone())?;
    assert_eq!(graph_delta_count(state.deltas()?), 0);
    store.remove(&first)?;
    assert_eq!(graph_delta_count(state.deltas()?), 0);
    store.remove(&second)?;
    assert_eq!(graph_delta_count(state.deltas()?), 1);
    Ok(())
}

fn graph_delta_count(deltas: QueryResultsDelta<'_>) -> usize {
    let QueryResultsDelta::Graph(values) = deltas else {
        unreachable!("expected graph deltas")
    };
    values.count()
}

#[expect(clippy::non_ascii_literal)]
const GRAPH_DATA: &str = r#"
@prefix schema: <http://schema.org/> .
@prefix wd: <http://www.wikidata.org/entity/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

GRAPH <http://www.wikidata.org/wiki/Special:EntityData/Q90> {
    wd:Q90 a schema:City ;
        schema:name "Paris"@fr , "la ville lumière"@fr ;
        schema:country wd:Q142 ;
        schema:population 2000000 ;
        schema:startDate "-300"^^xsd:gYear ;
        schema:url "https://www.paris.fr/"^^xsd:anyURI ;
        schema:postalCode "75001" .
}
"#;
const NUMBER_OF_TRIPLES: usize = 8;

fn quads(graph_name: impl Into<GraphName>) -> Vec<Quad> {
    let graph_name = graph_name.into();
    let paris = NamedNode::new_unchecked("http://www.wikidata.org/entity/Q90");
    let france = NamedNode::new_unchecked("http://www.wikidata.org/entity/Q142");
    let city = NamedNode::new_unchecked("http://schema.org/City");
    let name = NamedNode::new_unchecked("http://schema.org/name");
    let country = NamedNode::new_unchecked("http://schema.org/country");
    let population = NamedNode::new_unchecked("http://schema.org/population");
    let start_date = NamedNode::new_unchecked("http://schema.org/startDate");
    let url = NamedNode::new_unchecked("http://schema.org/url");
    let postal_code = NamedNode::new_unchecked("http://schema.org/postalCode");
    vec![
        Quad::new(paris.clone(), rdf::TYPE, city, graph_name.clone()),
        Quad::new(
            paris.clone(),
            name.clone(),
            Literal::new_language_tagged_literal_unchecked("Paris", "fr"),
            graph_name.clone(),
        ),
        Quad::new(
            paris.clone(),
            name,
            Literal::new_language_tagged_literal_unchecked("la ville lumi\u{E8}re", "fr"),
            graph_name.clone(),
        ),
        Quad::new(paris.clone(), country, france, graph_name.clone()),
        Quad::new(
            paris.clone(),
            population,
            Literal::new_typed_literal("2000000", xsd::INTEGER),
            graph_name.clone(),
        ),
        Quad::new(
            paris.clone(),
            start_date,
            Literal::new_typed_literal("-300", xsd::G_YEAR),
            graph_name.clone(),
        ),
        Quad::new(
            paris.clone(),
            url,
            Literal::new_typed_literal("https://www.paris.fr/", xsd::ANY_URI),
            graph_name.clone(),
        ),
        Quad::new(
            paris.clone(),
            postal_code,
            Literal::new_simple_literal("75001"),
            graph_name,
        ),
    ]
}

#[test]
fn test_load_graph() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    store.load_from_reader(RdfFormat::Turtle, DATA.as_bytes())?;
    for q in quads(GraphName::DefaultGraph) {
        assert!(store.contains(&q)?);
    }
    store.validate()?;
    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_load_graph_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = Store::open(&dir)?;
    store.load_from_reader(RdfFormat::Turtle, DATA.as_bytes())?;
    for q in quads(GraphName::DefaultGraph) {
        assert!(store.contains(&q)?);
    }
    store.validate()?;
    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_merged_default_graph_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = Store::open(&dir)?;
    let s1 = NamedNode::new_unchecked("urn:s1");
    let s2 = NamedNode::new_unchecked("urn:s2");
    let s3 = NamedNode::new_unchecked("urn:s3");
    let p1 = NamedNode::new_unchecked("urn:p1");
    let p2 = NamedNode::new_unchecked("urn:p2");
    let o1 = NamedNode::new_unchecked("urn:o1");
    let o2 = NamedNode::new_unchecked("urn:o2");
    let g1 = NamedNode::new_unchecked("urn:g1");
    let g2 = NamedNode::new_unchecked("urn:g2");
    for quad in [
        Quad::new(s1.clone(), p1.clone(), o1.clone(), g1.clone()),
        Quad::new(s2.clone(), p1.clone(), o1.clone(), g1.clone()),
        Quad::new(s1.clone(), p2.clone(), o2.clone(), g1.clone()),
        Quad::new(s1.clone(), p1.clone(), o1.clone(), g2.clone()),
        Quad::new(s1.clone(), p2.clone(), o2.clone(), g2.clone()),
        Quad::new(s3, p1, o1, g2),
    ] {
        store.insert(quad)?;
    }

    for (pattern, expected_count) in [
        ("?s ?p ?o", 4),
        ("<urn:s1> ?p ?o", 2),
        ("?s <urn:p1> ?o", 3),
        ("?s ?p <urn:o1>", 3),
        ("<urn:s1> ?p <urn:o1>", 1),
        ("<urn:s1> <urn:p1> ?o", 1),
        ("?s <urn:p1> <urn:o1>", 3),
        ("<urn:s1> <urn:p1> <urn:o1>", 1),
    ] {
        let query = format!("SELECT * FROM <urn:g1> FROM <urn:g2> WHERE {{ {pattern} }}");
        let QueryResults::Solutions(solutions) = SparqlEvaluator::new()
            .parse_query(&query)?
            .on_store(&store)
            .execute()?
        else {
            unreachable!()
        };
        assert_eq!(
            solutions.collect::<Result<Vec<_>, _>>()?.len(),
            expected_count,
            "pattern: {pattern}"
        );
    }

    let mut query = SparqlEvaluator::new().parse_query("SELECT * WHERE { ?s ?p ?o }")?;
    query.dataset_mut().set_default_graph_as_union();
    let QueryResults::Solutions(solutions) = query.on_store(&store).execute()? else {
        unreachable!()
    };
    assert_eq!(solutions.collect::<Result<Vec<_>, _>>()?.len(), 4);
    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_load_graph_on_disk_with_options() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = Store::open_with_options(
        &dir,
        StoreOptions::default()
            .with_max_open_files(128)
            .with_fd_reserve(64),
    )?;
    store.load_from_reader(RdfFormat::Turtle, DATA.as_bytes())?;
    for q in quads(GraphName::DefaultGraph) {
        assert!(store.contains(&q)?);
    }
    store.validate()?;
    Ok(())
}

#[test]
fn test_bulk_load_graph() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let mut loader = store.bulk_loader();
    loader.load_from_slice(RdfFormat::Turtle, DATA.as_bytes())?;
    loader.commit()?;
    for q in quads(GraphName::DefaultGraph) {
        assert!(store.contains(&q)?);
    }
    store.validate()?;
    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_bulk_load_graph_on_disk() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = Store::open(&dir)?;
    let mut loader = store.bulk_loader();
    loader.load_from_slice(RdfFormat::Turtle, DATA.as_bytes())?;
    loader.commit()?;
    for q in quads(GraphName::DefaultGraph) {
        assert!(store.contains(&q)?);
    }
    store.validate()?;
    Ok(())
}

#[test]
fn test_bulk_load_graph_lenient() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let mut loader = store.bulk_loader().on_parse_error(|_| Ok(()));
    loader.load_from_slice(
        RdfFormat::NTriples,
        b"<http://example.com> <http://example.com> <http://example.com##> .\n<http://example.com> <http://example.com> <http://example.com> .".as_slice(),
    )?;
    loader.commit()?;
    assert_eq!(store.len()?, 1);
    assert!(store.contains(&Quad::new(
        NamedNode::new_unchecked("http://example.com"),
        NamedNode::new_unchecked("http://example.com"),
        NamedNode::new_unchecked("http://example.com"),
        GraphName::DefaultGraph
    ))?);
    store.validate()?;
    Ok(())
}

#[test]
fn test_bulk_load_empty() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let mut loader = store.bulk_loader();
    loader.load_quads(empty::<Quad>())?;
    loader.commit()?;
    assert!(store.is_empty()?);
    store.validate()?;
    Ok(())
}

#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
#[test]
fn test_bulk_load_rollback() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = Store::open(&dir)?;
    let before_files = read_dir(&dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut loader = store.bulk_loader();
    loader.load_from_slice(RdfFormat::Turtle, DATA.as_bytes())?;
    drop(loader);
    store.validate()?;
    let after_files = read_dir(&dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        before_files, after_files,
        "Files created even if bulk loader got rolled back"
    );
    Ok(())
}

#[test]
fn test_load_dataset() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    store.load_from_reader(RdfFormat::TriG, GRAPH_DATA.as_bytes())?;
    for q in quads(NamedNode::new_unchecked(
        "http://www.wikidata.org/wiki/Special:EntityData/Q90",
    )) {
        assert!(store.contains(&q)?);
    }
    store.validate()?;
    Ok(())
}

#[test]
fn test_bulk_load_dataset() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    let mut loader = store.bulk_loader();
    loader.load_from_slice(RdfFormat::TriG, GRAPH_DATA.as_bytes())?;
    loader.commit()?;
    let graph_name =
        NamedNode::new_unchecked("http://www.wikidata.org/wiki/Special:EntityData/Q90");
    for q in quads(graph_name.clone()) {
        assert!(store.contains(&q)?);
    }
    assert!(store.contains_named_graph(&graph_name.into())?);
    store.validate()?;
    Ok(())
}

#[test]
fn test_load_graph_generates_new_blank_nodes() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    for _ in 0..2 {
        store.load_from_reader(
            RdfFormat::NTriples,
            "_:a <http://example.com/p> <http://example.com/p> .".as_bytes(),
        )?;
    }
    assert_eq!(store.len()?, 2);
    Ok(())
}

#[test]
fn test_dump_graph() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    for q in quads(GraphName::DefaultGraph) {
        store.insert(q)?;
    }

    let mut buffer = Vec::new();
    store.dump_graph_to_writer(&GraphName::DefaultGraph, RdfFormat::NTriples, &mut buffer)?;
    assert_eq!(
        buffer.into_iter().filter(|c| *c == b'\n').count(),
        NUMBER_OF_TRIPLES
    );
    Ok(())
}

#[test]
fn test_dump_dataset() -> Result<(), Box<dyn Error>> {
    let store = Store::new()?;
    for q in quads(GraphName::DefaultGraph) {
        store.insert(q)?;
    }

    let buffer = store.dump_to_writer(RdfFormat::NQuads, Vec::new())?;
    assert_eq!(
        buffer.into_iter().filter(|c| *c == b'\n').count(),
        NUMBER_OF_TRIPLES
    );
    Ok(())
}

#[test]
fn test_snapshot_isolation_iterator() -> Result<(), Box<dyn Error>> {
    let quad = Quad::new(
        NamedNode::new("http://example.com/s")?,
        NamedNode::new("http://example.com/p")?,
        NamedNode::new("http://example.com/o")?,
        NamedNode::new("http://www.wikidata.org/wiki/Special:EntityData/Q90")?,
    );
    let store = Store::new()?;
    store.insert(quad.clone())?;
    let iter = store.iter();
    store.remove(&quad)?;
    assert_eq!(iter.collect::<Result<Vec<_>, _>>()?, vec![quad]);
    store.validate()?;
    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_snapshot_isolation_iterator_on_disk() -> Result<(), Box<dyn Error>> {
    let quad = Quad::new(
        NamedNode::new("http://example.com/s")?,
        NamedNode::new("http://example.com/p")?,
        NamedNode::new("http://example.com/o")?,
        NamedNode::new("http://www.wikidata.org/wiki/Special:EntityData/Q90")?,
    );
    let dir = TempDir::new()?;
    let store = Store::open(&dir)?;
    store.insert(quad.clone())?;
    let iter = store.iter();
    store.remove(&quad)?;
    assert_eq!(iter.collect::<Result<Vec<_>, _>>()?, vec![quad]);
    store.validate()?;
    Ok(())
}

#[test]
fn test_bulk_load_on_existing_delete_overrides_the_delete() -> Result<(), Box<dyn Error>> {
    let quad = Quad::new(
        NamedNode::new_unchecked("http://example.com/s"),
        NamedNode::new_unchecked("http://example.com/p"),
        NamedNode::new_unchecked("http://example.com/o"),
        NamedNode::new_unchecked("http://www.wikidata.org/wiki/Special:EntityData/Q90"),
    );
    let store = Store::new()?;
    store.remove(&quad)?;
    let mut loader = store.bulk_loader();
    loader.load_quads([quad])?;
    loader.commit()?;
    assert_eq!(store.len()?, 1);
    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_bulk_load_on_existing_delete_overrides_the_delete_on_disk() -> Result<(), Box<dyn Error>> {
    let quad = Quad::new(
        NamedNode::new_unchecked("http://example.com/s"),
        NamedNode::new_unchecked("http://example.com/p"),
        NamedNode::new_unchecked("http://example.com/o"),
        NamedNode::new_unchecked("http://www.wikidata.org/wiki/Special:EntityData/Q90"),
    );
    let dir = TempDir::new()?;
    let store = Store::open(&dir)?;
    store.remove(&quad)?;
    let mut loader = store.bulk_loader();
    loader.load_quads([quad])?;
    loader.commit()?;
    assert_eq!(store.len()?, 1);
    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_open_bad_dir() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    create_dir_all(&dir)?;
    {
        File::create(dir.as_ref().join("CURRENT"))?.write_all(b"foo")?;
    }
    assert!(Store::open(&dir).is_err());
    Ok(())
}

#[test]
#[cfg(all(target_os = "linux", feature = "rocksdb"))]
fn test_bad_stt_open() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let store = Store::open(&dir)?;
    remove_dir_all(&dir)?;
    let mut loader = store.bulk_loader();
    loader.load_quads(once(Quad::new(
        NamedNode::new_unchecked("http://example.com/s"),
        NamedNode::new_unchecked("http://example.com/p"),
        NamedNode::new_unchecked("http://example.com/o"),
        GraphName::DefaultGraph,
    )))?;
    loader.commit().unwrap_err();
    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_backup() -> Result<(), Box<dyn Error>> {
    let quad = Quad::new(
        NamedNode::new_unchecked("http://example.com/s"),
        NamedNode::new_unchecked("http://example.com/p"),
        NamedNode::new_unchecked("http://example.com/o"),
        GraphName::DefaultGraph,
    );
    let store_dir = TempDir::new()?;
    let backup_from_rw_dir = TempDir::new()?;
    remove_dir(&backup_from_rw_dir)?;
    let backup_from_ro_dir = TempDir::new()?;
    remove_dir(&backup_from_ro_dir)?;

    let store = Store::open(&store_dir)?;
    store.insert(quad.clone())?;
    store.backup(&backup_from_rw_dir)?;
    store.remove(&quad)?;
    assert!(!store.contains(&quad)?);

    let backup_from_rw = Store::open_read_only(&backup_from_rw_dir)?;
    backup_from_rw.validate()?;
    assert!(backup_from_rw.contains(&quad)?);
    backup_from_rw.backup(&backup_from_ro_dir)?;

    let backup_from_ro = Store::open_read_only(&backup_from_ro_dir)?;
    backup_from_ro.validate()?;
    assert!(backup_from_ro.contains(&quad)?);

    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_bad_backup() -> Result<(), Box<dyn Error>> {
    let store_dir = TempDir::new()?;
    let backup_dir = TempDir::new()?;

    create_dir_all(&backup_dir)?;
    Store::open(&store_dir)?.backup(&backup_dir).unwrap_err();
    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_backup_on_in_memory() -> Result<(), Box<dyn Error>> {
    let backup_dir = TempDir::new()?;
    Store::new()?.backup(&backup_dir).unwrap_err();
    Ok(())
}

#[test]
#[cfg(all(
    target_os = "linux",
    target_pointer_width = "64",
    target_endian = "little",
    feature = "rocksdb"
))]
fn test_backward_compatibility() -> Result<(), Box<dyn Error>> {
    // We run twice to check if data is properly saved and closed
    let _reset = DirSaver::new("tests/rocksdb_bc_data")?;
    for _ in 0..2 {
        let store = Store::open("tests/rocksdb_bc_data")?;
        for q in quads(GraphName::DefaultGraph) {
            assert!(store.contains(&q)?);
        }
        let graph_name =
            NamedNode::new_unchecked("http://www.wikidata.org/wiki/Special:EntityData/Q90");
        for q in quads(graph_name.clone()) {
            assert!(store.contains(&q)?);
        }
        assert!(store.contains_named_graph(&graph_name.clone().into())?);
        assert_eq!(
            vec![NamedOrBlankNode::from(graph_name)],
            store.named_graphs().collect::<Result<Vec<_>, _>>()?
        );
    }
    Ok(())
}

#[test]
#[cfg(all(
    target_os = "linux",
    target_pointer_width = "64",
    target_endian = "little",
    feature = "rocksdb",
    feature = "rdf-12"
))]
fn test_rdf_star_backward_compatibility() -> Result<(), Box<dyn Error>> {
    // We run twice to check if data is properly saved and closed
    let _reset = DirSaver::new("tests/rocksdb_bc_rdf_star_data")?;
    let s = NamedNode::new_unchecked("http://example.com/s");
    let p = NamedNode::new_unchecked("http://example.com/p");
    let o = NamedNode::new_unchecked("http://example.com/o");
    let g = NamedNode::new_unchecked("http://example.com/g");
    let bnode = BlankNode::new_unchecked("f2fef82410957224105241225fd0a648");
    for _ in 0..2 {
        let store = Store::open("tests/rocksdb_bc_rdf_star_data")?;
        assert!(store.contains(&Quad::new(s.clone(), p.clone(), o.clone(), g.clone()))?);
        assert!(store.contains(&Quad::new(
            s.clone(),
            p.clone(),
            o.clone(),
            GraphName::DefaultGraph
        ))?);
        assert!(store.contains(&Quad::new(bnode.clone(), p.clone(), o.clone(), g.clone()))?);
        assert!(store.contains(&Quad::new(
            bnode.clone(),
            p.clone(),
            o.clone(),
            GraphName::DefaultGraph
        ))?);
        assert!(store.contains(&Quad::new(s.clone(), p.clone(), bnode.clone(), g.clone()))?);
        assert!(store.contains(&Quad::new(
            s.clone(),
            p.clone(),
            bnode.clone(),
            GraphName::DefaultGraph
        ))?);
        assert!(store.contains(&Quad::new(
            bnode.clone(),
            rdf::REIFIES,
            Triple::new(s.clone(), p.clone(), o.clone()),
            g.clone()
        ))?);
        assert!(store.contains(&Quad::new(
            bnode.clone(),
            rdf::REIFIES,
            Triple::new(s.clone(), p.clone(), o.clone()),
            GraphName::DefaultGraph
        ))?);
    }
    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_read_only() -> Result<(), Box<dyn Error>> {
    let s = NamedNode::new_unchecked("http://example.com/s");
    let p = NamedNode::new_unchecked("http://example.com/p");
    let first_quad = Quad::new(
        s.clone(),
        p.clone(),
        NamedNode::new_unchecked("http://example.com/o"),
        GraphName::DefaultGraph,
    );
    let second_quad = Quad::new(
        s,
        p,
        NamedNode::new_unchecked("http://example.com/o2"),
        GraphName::DefaultGraph,
    );
    let store_dir = TempDir::new()?;

    // We write to the store and close it
    {
        let read_write = Store::open(&store_dir)?;
        read_write.insert(first_quad.clone())?;
        read_write.flush()?;
    }

    // We open as read-only
    let read_only = Store::open_read_only(&store_dir)?;
    assert!(read_only.contains(&first_quad)?);
    assert_eq!(
        read_only.iter().collect::<Result<Vec<_>, _>>()?,
        vec![first_quad]
    );
    read_only.validate()?;

    // We open as read-write again
    let read_write = Store::open(&store_dir)?;
    read_write.insert(second_quad.clone())?;
    read_write.flush()?;
    read_write.optimize()?; // Makes sure it's well flushed

    // The new quad is in the read-write instance but not the read-only instance
    assert!(read_write.contains(&second_quad)?);
    assert!(!read_only.contains(&second_quad)?);
    read_only.validate()?;

    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_open_read_only_bad_dir() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    create_dir_all(&dir)?;
    {
        File::create(dir.as_ref().join("CURRENT"))?.write_all(b"foo")?;
    }
    assert!(Store::open_read_only(&dir).is_err());
    Ok(())
}

#[test]
#[cfg(all(not(target_family = "wasm"), feature = "rocksdb"))]
fn test_read_your_own_write_transaction() -> Result<(), Box<dyn Error>> {
    let store_dir = TempDir::new()?;

    let quad = Quad::new(
        NamedNode::new_unchecked("http://example.com/s"),
        NamedNode::new_unchecked("http://example.com/p"),
        NamedNode::new_unchecked("http://example.com/o"),
        GraphName::DefaultGraph,
    );

    let store = Store::open(&store_dir)?;
    let mut transaction = store.start_transaction()?;
    transaction.insert(quad.clone());
    assert_eq!(transaction.iter().collect::<Result<Vec<_>, _>>()?, [quad]);

    Ok(())
}

#[cfg(all(
    target_os = "linux",
    target_pointer_width = "64",
    target_endian = "little",
    feature = "rocksdb"
))]
struct DirSaver {
    path: PathBuf,
    elements: Vec<(PathBuf, Vec<u8>)>,
}

#[cfg(all(
    target_os = "linux",
    target_pointer_width = "64",
    target_endian = "little",
    feature = "rocksdb"
))]
impl DirSaver {
    fn new(path: &str) -> io::Result<Self> {
        Ok(Self {
            path: path.into(),
            elements: read_dir(path)?
                .map(|item| {
                    let path = item?.path();
                    let content = read(&path)?;
                    Ok((path, content))
                })
                .collect::<io::Result<Vec<_>>>()?,
        })
    }
}

#[cfg(all(
    target_os = "linux",
    target_pointer_width = "64",
    target_endian = "little",
    feature = "rocksdb"
))]
impl Drop for DirSaver {
    fn drop(&mut self) {
        remove_dir_all(&self.path).unwrap();
        create_dir_all(&self.path).unwrap();
        for (path, content) in &self.elements {
            write(path, content).unwrap();
        }
    }
}

#[test]
fn test_snapshot_is_pinned_to_one_state() -> Result<(), Box<dyn Error>> {
    let ex = NamedNode::new("http://example.com")?;
    let q1 = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
    let q2 = Quad::new(ex.clone(), ex.clone(), ex.clone(), ex.clone());
    let store = Store::new()?;
    store.insert(q1.clone())?;

    let snapshot = store.snapshot();
    assert_eq!(snapshot.len()?, 1);
    assert!(!snapshot.is_empty()?);
    assert!(snapshot.contains(&q1)?);

    // Committed after the snapshot was taken, so the snapshot must not see it
    // while the store must.
    store.insert(q2.clone())?;
    assert_eq!(snapshot.len()?, 1);
    assert_eq!(store.len()?, 2);
    assert!(!snapshot.contains(&q2)?);
    assert!(store.contains(&q2)?);

    // The same state through every accessor, not just len.
    assert_eq!(snapshot.iter().count(), 1);
    assert_eq!(
        snapshot
            .quads_for_pattern(None, Some(&ex), None, None)
            .count(),
        1
    );
    Ok(())
}
