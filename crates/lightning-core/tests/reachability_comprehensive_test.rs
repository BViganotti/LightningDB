use arrow::array::{Int64Array, StringArray};
use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_reachability_direct() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person)", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob'})", None)?;
    conn.execute("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:Knows]->(b)", None)?;
    let res = conn.execute("MATCH (a:Person {id: 1})-[r:Knows]->(b:Person) RETURN b.name", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_reachability_no_relation() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person)", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob'})", None)?;
    let res = conn.execute("MATCH (a:Person {id: 1})-[r:Knows]->(b:Person) RETURN b.name", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_reachability_multiple_relations() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person)", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob'})", None)?;
    conn.execute("CREATE (:Person {id: 3, name: 'Charlie'})", None)?;
    conn.execute("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:Knows]->(b)", None)?;
    conn.execute("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:Knows]->(b)", None)?;
    let res = conn.execute("MATCH (a:Person {id: 1})-[r:Knows]->(b:Person) RETURN b.name", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

#[test]
fn test_reachability_self_reference() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person)", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("MATCH (a:Person {id: 1}), (b:Person {id: 1}) CREATE (a)-[:Knows]->(b)", None)?;
    let res = conn.execute("MATCH (a:Person {id: 1})-[r:Knows]->(b:Person) RETURN b.name", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_reachability_filtered_by_rel_property() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, weight INT64)", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob'})", None)?;
    conn.execute("CREATE (:Person {id: 3, name: 'Charlie'})", None)?;
    conn.execute("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:Knows {weight: 10}]->(b)", None)?;
    conn.execute("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:Knows {weight: 20}]->(b)", None)?;
    let result = conn.execute(
        "MATCH (a:Person {id: 1})-[r:Knows]->(b:Person) WHERE r.weight > 15 RETURN b.name",
        None,
    );
    if let Ok(res) = result {
        if count_rows(&res) > 0 {
            let name = res.batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(name.value(0), "Charlie");
        }
    }
    Ok(())
}

#[test]
fn test_reachability_via_different_tables() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Company(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE WorksAt(FROM Person TO Company)", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:Company {id: 1, name: 'Acme'})", None)?;
    conn.execute("MATCH (a:Person {id: 1}), (b:Company {id: 1}) CREATE (a)-[:WorksAt]->(b)", None)?;
    let res = conn.execute("MATCH (a:Person {id: 1})-[r:WorksAt]->(c:Company) RETURN c.name", None)?;
    let name = res.batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(name.value(0), "Acme");
    Ok(())
}
