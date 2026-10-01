use oxrdf::{Dataset, GraphName, NamedNode, Quad};
use spareval::{Delta, QueryEvaluator, QueryResultsDelta};
use spargebra::SparqlParser;
use std::error::Error;

#[test]
fn static_dataset_snapshot_flows_through_join() -> Result<(), Box<dyn Error>> {
    let mut dataset = Dataset::new();
    dataset.insert(Quad::new(
        NamedNode::new_unchecked("urn:s"),
        NamedNode::new_unchecked("urn:p"),
        NamedNode::new_unchecked("urn:o"),
        GraphName::DefaultGraph,
    ));
    dataset.insert(Quad::new(
        NamedNode::new_unchecked("urn:o"),
        NamedNode::new_unchecked("urn:q"),
        NamedNode::new_unchecked("urn:x"),
        GraphName::DefaultGraph,
    ));
    let query = SparqlParser::new()
        .parse_query("SELECT ?s WHERE { ?s <urn:p> ?o . ?o <urn:q> <urn:x> }")?;
    let mut state = QueryEvaluator::new()
        .prepare(&query)
        .execute_incremental_deltas(&dataset)?;
    let QueryResultsDelta::Solutions(changes) = state.deltas()? else {
        unreachable!("expected SELECT deltas")
    };
    assert!(matches!(
        changes.collect::<Result<Vec<_>, _>>()?.as_slice(),
        [Delta::Addition(solution)]
            if solution.get("s") == Some(&NamedNode::new_unchecked("urn:s").into())
    ));
    Ok(())
}
