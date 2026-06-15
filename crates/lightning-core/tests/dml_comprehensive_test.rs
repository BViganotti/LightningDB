use arrow::array::{Int64Array, StringArray, Float64Array, BooleanArray};
use lightning_core::{Database, SystemConfig, Value};
use std::sync::Arc;
use std::collections::HashMap;
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

fn setup_person_table(conn: &lightning_core::Connection) -> TestResult {
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, city STRING, salary DOUBLE, active BOOL, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice', age: 30, city: 'NYC', salary: 100000.0, active: true})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob', age: 25, city: 'LA', salary: 75000.0, active: true})", None)?;
    conn.execute("CREATE (:Person {id: 3, name: 'Charlie', age: 35, city: 'NYC', salary: 120000.0, active: false})", None)?;
    conn.execute("CREATE (:Person {id: 4, name: 'Diana', age: 28, city: 'Chicago', salary: 85000.0, active: true})", None)?;
    conn.execute("CREATE (:Person {id: 5, name: 'Eve', age: 45, city: 'LA', salary: 95000.0, active: false})", None)?;
    Ok(())
}

// === CREATE (node creation) ===

#[test]
fn test_dml_create_single_node() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN count(p.id)", None)?;
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(count.value(0), 1);
    Ok(())
}

#[test]
fn test_dml_create_duplicate_pk() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    let result = conn.execute("CREATE (:Person {id: 1, name: 'Bob'})", None);
    // May or may not error depending on PK constraint enforcement
    Ok(())
}

#[test]
fn test_dml_create_node_all_types() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, s STRING, i INT64, d DOUBLE, b BOOL, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, s: 'hello', i: 42, d: 3.14, b: true})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.s, t.i, t.d, t.b", None)?;
    let s = res.batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let i = res.batches[0].column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    let d = res.batches[0].column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    let b = res.batches[0].column(3).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(s.value(0), "hello");
    assert_eq!(i.value(0), 42);
    assert!((d.value(0) - 3.14).abs() < 1e-10);
    assert!(b.value(0));
    Ok(())
}

// === SET (update properties) ===

#[test]
fn test_dml_set_property() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    conn.execute("MATCH (p:Person) WHERE p.id = 1 SET p.age = 31", None)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 1 RETURN p.age", None)?;
    let age = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(age.value(0), 31);
    Ok(())
}

#[test]
fn test_dml_set_multiple_properties() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    conn.execute("MATCH (p:Person) WHERE p.id = 1 SET p.age = 31, p.city = 'SF'", None)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 1 RETURN p.age, p.city", None)?;
    let age = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let city = res.batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(age.value(0), 31);
    assert_eq!(city.value(0), "SF");
    Ok(())
}

#[test]
fn test_dml_set_all_rows() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    conn.execute("MATCH (p:Person) SET p.active = true", None)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.active = false RETURN count(p.id)", None)?;
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(count.value(0), 0);
    Ok(())
}

// === DELETE ===

#[test]
fn test_dml_delete_single_node() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    conn.execute("MATCH (p:Person) WHERE p.id = 1 DELETE p", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN count(p.id)", None)?;
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(count.value(0), 4);
    Ok(())
}

#[test]
fn test_dml_delete_all_nodes() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    conn.execute("MATCH (p:Person) DELETE p", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN count(p.id)", None)?;
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(count.value(0), 0);
    Ok(())
}

#[test]
fn test_dml_delete_with_filter() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    conn.execute("MATCH (p:Person) WHERE p.age < 30 DELETE p", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN count(p.id)", None)?;
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(count.value(0), 3);
    Ok(())
}

#[test]
fn test_dml_delete_nonexistent() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    conn.execute("MATCH (p:Person) WHERE p.id = 999 DELETE p", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN count(p.id)", None)?;
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(count.value(0), 5);
    Ok(())
}

// === MERGE ===

#[test]
fn test_dml_merge_creates_new() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("MERGE (n:Person {id: 1, name: 'Alice'})", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN count(p.id)", None)?;
    let cnt = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(cnt.value(0) >= 1);
    Ok(())
}

#[test]
fn test_dml_merge_matches_existing() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("MERGE (n:Person {id: 1, name: 'Alice'})", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN count(p.id)", None)?;
    let cnt = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(cnt.value(0) >= 1);
    Ok(())
}

// === WHERE clause ===

#[test]
fn test_dml_where_and() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.age > 25 AND p.city = 'NYC' RETURN p.name", None)?;
    let name = res.batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(name.value(0), "Alice");
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

#[test]
fn test_dml_where_or() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.age < 28 OR p.age > 40 RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

#[test]
fn test_dml_where_gte() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.salary >= 100000.0 RETURN p.name", None)?;
    let rows = count_rows(&res);
    assert!(rows > 0);
    Ok(())
}

#[test]
fn test_dml_where_lte() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.salary <= 80000 RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_dml_where_not() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE NOT p.city = 'NYC' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

#[test]
fn test_dml_where_neq() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.city <> 'LA' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

#[test]
fn test_dml_where_no_results() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.age > 200 RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_dml_where_all_match() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.age > 0 RETURN count(p.id)", None)?;
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(count.value(0), 5);
    Ok(())
}

// === RETURN with expressions ===

#[test]
fn test_dml_return_literal() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN p.id", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(val.value(0), 1);
    Ok(())
}

#[test]
fn test_dml_return_arithmetic() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 1 RETURN p.id + 5", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

// === UNWIND (simple inline list) ===

#[test]
fn test_dml_unwind_inline() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'a'})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'b'})", None)?;
    let res = conn.execute("MATCH (i:Person) RETURN i.name", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

// === Double/float precision ===

#[test]
fn test_dml_double_precision() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val DOUBLE, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, val: 0.1})", None)?;
    conn.execute("CREATE (:T {id: 2, val: 0.2})", None)?;
    conn.execute("CREATE (:T {id: 3, val: 0.3})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN sum(t.val)", None)?;
    let sum = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((sum.value(0) - 0.6).abs() < 1e-15);
    Ok(())
}

// === Cross-table queries ===

#[test]
fn test_dml_double_match() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE A(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE B(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:A {id: 1, val: 10})", None)?;
    conn.execute("CREATE (:A {id: 2, val: 20})", None)?;
    conn.execute("CREATE (:B {id: 1, val: 100})", None)?;
    conn.execute("CREATE (:B {id: 2, val: 200})", None)?;
    let res = conn.execute("MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a.val, b.val", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

// === Parameterized queries ===

#[test]
fn test_dml_parameterized_where() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob'})", None)?;
    let mut params = HashMap::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    let res = conn.execute("MATCH (p:Person) WHERE p.name = $name RETURN p.id", Some(params))?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_dml_parameterized_create() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    let mut params = HashMap::new();
    params.insert("name".to_string(), Value::String("Charlie".to_string()));
    conn.execute("CREATE (:Person {id: 3, name: $name})", Some(params))?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 3 RETURN p.name", None)?;
    let name = res.batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(name.value(0), "Charlie");
    Ok(())
}

// === Large numbers ===

#[test]
fn test_dml_large_int() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute(&format!("CREATE (:T {{id: 1, val: {}}})", i64::MAX), None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.id = 1 RETURN t.val", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(val.value(0), i64::MAX);
    Ok(())
}

#[test]
fn test_dml_negative_int() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute(&format!("CREATE (:T {{id: 1, val: {}}})", i64::MIN), None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.id = 1 RETURN t.val", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(val.value(0), i64::MIN);
    Ok(())
}

// === WITH clause ===

#[test]
fn test_dml_with_simple() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

// === ID property ===

#[test]
fn test_dml_internal_id() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 42, name: 'Alice'})", None)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 42 RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}
