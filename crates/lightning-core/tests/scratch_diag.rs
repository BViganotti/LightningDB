use lightning_core::{Database, SystemConfig};

#[test]
fn diag_explain_union() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let c = db.connect();
    c.execute("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))", None).unwrap();
    c.execute("CREATE (:Person {name:'Alice', age:30})", None).unwrap();
    c.execute("CREATE (:Person {name:'Bob', age:25})", None).unwrap();
    let r = c.execute("EXPLAIN MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name AS name UNION ALL MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.name AS name", None);
    match r {
        Ok(res) => for b in &res.batches { for i in 0..b.num_rows() { eprintln!("DIAG explain row {i}: {:?}", arrow::util::display::array_value_to_string(b.column(0), i)); } },
        Err(e) => eprintln!("DIAG explain ERR {e}"),
    }
}
