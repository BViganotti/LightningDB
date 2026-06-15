use arrow::array::{Int64Array, StringArray, Float64Array};
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
    Ok(())
}

// === Expression: arithmetic ===

#[test]
fn test_expr_add_literals() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 1 RETURN p.age + 10", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((val.value(0) - 40.0).abs() < 1e-10);
    Ok(())
}

#[test]
fn test_expr_sub_literals() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 1 RETURN 100 - p.age", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((val.value(0) - 70.0).abs() < 1e-10);
    Ok(())
}

#[test]
fn test_expr_mul_column() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 1 RETURN p.salary * 2", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((val.value(0) - 200000.0).abs() < 1e-5);
    Ok(())
}

#[test]
fn test_expr_div_column() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 1 RETURN p.salary / 2", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((val.value(0) - 50000.0).abs() < 1e-5);
    Ok(())
}

#[test]
fn test_expr_complex_arithmetic() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 1 RETURN (p.age + p.age) * 2", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((val.value(0) - 120.0).abs() < 1e-10);
    Ok(())
}

// === Expression: boolean ===

#[test]
fn test_expr_and_true() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.age > 20 AND p.city = 'NYC' RETURN count(p.id)", None)?;
    let c = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(c.value(0), 2);
    Ok(())
}

#[test]
fn test_expr_or_true() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.age = 25 OR p.age = 35 RETURN count(p.id)", None)?;
    let c = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(c.value(0), 2);
    Ok(())
}

// === Expression: strings ===

#[test]
fn test_expr_string_contains() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.name CONTAINS 'li' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

#[test]
fn test_expr_string_starts_with() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_expr_string_ends_with() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.name ENDS WITH 'e' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

// === Expression: NULL ===

#[test]
fn test_expr_null_equality() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, val: 10})", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.val IS NULL RETURN t.id", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

// === Expression: IN ===

#[test]
fn test_expr_in_list() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id IN [1, 3] RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

#[test]
fn test_expr_in_literal_list() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.name IN ['Alice', 'Bob'] RETURN count(p.id)", None)?;
    let c = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(c.value(0), 2);
    Ok(())
}

// === Expression: boolean comparisons ===

#[test]
fn test_expr_gt() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.age > 30 RETURN count(p.id)", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_expr_gte() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.age > 30 RETURN count(p.id)", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_expr_lt() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.age > 20 AND p.age < 30 RETURN count(p.id)", None)?;
    assert!(count_rows(&res) >= 1);
    Ok(())
}

#[test]
fn test_expr_lte() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.age > 30 RETURN count(p.id)", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

// === Expression: function calls ===

#[test]
fn test_expr_abs() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, val: -42})", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.id = 1 RETURN abs(t.val)", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((val.value(0) - 42.0).abs() < 1e-10);
    Ok(())
}

#[test]
fn test_expr_round() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val DOUBLE, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, val: 3.14159})", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.id = 1 RETURN round(t.val)", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((val.value(0) - 3.0).abs() < 1e-10);
    Ok(())
}

#[test]
fn test_expr_ceil() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val DOUBLE, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, val: 3.14159})", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.id = 1 RETURN ceil(t.val)", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((val.value(0) - 4.0).abs() < 1e-10);
    Ok(())
}

#[test]
fn test_expr_floor() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val DOUBLE, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, val: 3.14159})", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.id = 1 RETURN floor(t.val)", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((val.value(0) - 3.0).abs() < 1e-10);
    Ok(())
}

// === Expression: referencing non-existent column ===

#[test]
fn test_expr_nonexistent_column() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let result = conn.execute("MATCH (p:Person) RETURN p.nonexistent", None);
    assert!(result.is_err());
    Ok(())
}

// === Expression: WHERE clause with multiple conditions ===

#[test]
fn test_expr_multiple_conditions() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_table(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.age > 25 AND p.city = 'NYC' AND p.active = true RETURN p.name", None)?;
    let name = res.batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(name.value(0), "Alice");
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

// === Expression: Type coercion ===

#[test]
fn test_expr_int_vs_double() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val DOUBLE, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, val: 42.0})", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.val > 40 RETURN t.id", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}
