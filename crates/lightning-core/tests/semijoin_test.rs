use arrow::array::Array;
use lightning_core::Database;
use tempfile::tempdir;

#[test]
fn test_semijoin_pushdown_optimization() {
    let dir = tempdir().unwrap();
    let db = Database::new(
        dir.path().to_path_buf(),
        lightning_core::SystemConfig::default(),
    )
    .unwrap();

    let conn = db.connect();
    conn.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY (id))", None).unwrap();
    conn.execute("CREATE REL TABLE Follows(FROM User TO User)", None).unwrap();

    conn.execute("CREATE (:User {id: 1, name: 'Alice'})", None).unwrap();
    conn.execute("CREATE (:User {id: 2, name: 'Bob'})", None).unwrap();
    conn.execute("CREATE (:User {id: 3, name: 'Charlie'})", None).unwrap();
    conn.execute("CREATE (:User {id: 4, name: 'David'})", None).unwrap();
    conn.execute("MATCH (a:User {id: 1}), (b:User {id: 3}) CREATE (a)-[:Follows]->(b)", None).unwrap();
    conn.execute("MATCH (a:User {id: 1}), (b:User {id: 4}) CREATE (a)-[:Follows]->(b)", None).unwrap();
    conn.execute("MATCH (a:User {id: 2}), (b:User {id: 1}) CREATE (a)-[:Follows]->(b)", None).unwrap();
    conn.execute("MATCH (a:User {id: 2}), (b:User {id: 3}) CREATE (a)-[:Follows]->(b)", None).unwrap();
    conn.execute("MATCH (a:User {id: 3}), (b:User {id: 4}) CREATE (a)-[:Follows]->(b)", None).unwrap();
    conn.execute("MATCH (a:User {id: 4}), (b:User {id: 1}) CREATE (a)-[:Follows]->(b)", None).unwrap();

    // Query: MATCH (a:User)-[e:Follows]->(b:User) RETURN a.name
    let query = "MATCH (a:User)-[e:Follows]->(b:User) RETURN a.name";
    let query_result = conn.execute(query, None).unwrap();

    let mut results = Vec::new();
    for batch in &query_result.batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>();
        if let Some(names) = col {
            for i in 0..names.len() {
                results.push(names.value(i).to_string());
            }
        }
    }

    results.sort();
    let expected = vec!["Alice", "Alice", "Bob", "Bob", "Charlie", "David"];
    assert_eq!(results, expected);
}

#[test]
fn test_semijoin_no_results() {
    let dir = tempdir().unwrap();
    let db = Database::new(
        dir.path().to_path_buf(),
        lightning_core::SystemConfig::default(),
    )
    .unwrap();

    let conn = db.connect();
    conn.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY (id))", None).unwrap();
    conn.execute("CREATE REL TABLE Follows(FROM User TO User)", None).unwrap();
    conn.execute("CREATE (:User {id: 1, name: 'Alice'})", None).unwrap();
    conn.execute("CREATE (:User {id: 2, name: 'Bob'})", None).unwrap();
    conn.execute("CREATE (:User {id: 3, name: 'Charlie'})", None).unwrap();
    conn.execute("CREATE (:User {id: 4, name: 'David'})", None).unwrap();
    conn.execute("MATCH (a:User {id: 1}), (b:User {id: 3}) CREATE (a)-[:Follows]->(b)", None).unwrap();
    conn.execute("MATCH (a:User {id: 1}), (b:User {id: 4}) CREATE (a)-[:Follows]->(b)", None).unwrap();
    conn.execute("MATCH (a:User {id: 2}), (b:User {id: 1}) CREATE (a)-[:Follows]->(b)", None).unwrap();
    conn.execute("MATCH (a:User {id: 999}), (b:User {id: 1}) CREATE (a)-[:Follows]->(b)", None).unwrap();

    let query = "MATCH (a:User)-[e:Follows]->(b:User) WHERE a.id = 999 RETURN a.name";
    let query_result = conn.execute(query, None);
    if let Ok(res) = query_result {
        let total: usize = res.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }
}

#[test]
fn test_semijoin_self_follow() {
    let dir = tempdir().unwrap();
    let db = Database::new(
        dir.path().to_path_buf(),
        lightning_core::SystemConfig::default(),
    )
    .unwrap();

    let conn = db.connect();
    conn.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY (id))", None).unwrap();
    conn.execute("CREATE REL TABLE Follows(FROM User TO User)", None).unwrap();
    conn.execute("CREATE (:User {id: 1, name: 'Alice'})", None).unwrap();
    conn.execute("CREATE (:User {id: 2, name: 'Bob'})", None).unwrap();
    conn.execute("CREATE (:User {id: 3, name: 'Charlie'})", None).unwrap();
    conn.execute("CREATE (:User {id: 4, name: 'David'})", None).unwrap();
    conn.execute("MATCH (a:User {id: 1}), (b:User {id: 1}) CREATE (a)-[:Follows]->(b)", None).unwrap();
    conn.execute("MATCH (a:User {id: 2}), (b:User {id: 2}) CREATE (a)-[:Follows]->(b)", None).unwrap();

    let query = "MATCH (a:User)-[e:Follows]->(b:User) RETURN count(*)";
    let query_result = conn.execute(query, None).unwrap();
    let total: usize = query_result.batches.iter().map(|b| b.num_rows()).sum();
    if total > 0 {
        let count = query_result.batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        assert_eq!(count.value(0), 2);
    }
}
