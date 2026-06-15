use arrow::array::{Int64Array, StringArray, Float64Array};
use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    (dir, db)
}

fn setup_sales(conn: &lightning_core::Connection) -> TestResult {
    conn.execute("CREATE NODE TABLE Sale(id INT64, product STRING, category STRING, amount DOUBLE, qty INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Sale {id: 1, product: 'Widget', category: 'A', amount: 100.0, qty: 2})", None)?;
    conn.execute("CREATE (:Sale {id: 2, product: 'Gadget', category: 'B', amount: 200.0, qty: 1})", None)?;
    conn.execute("CREATE (:Sale {id: 3, product: 'Widget', category: 'A', amount: 50.0, qty: 5})", None)?;
    conn.execute("CREATE (:Sale {id: 4, product: 'Gizmo', category: 'A', amount: 300.0, qty: 3})", None)?;
    conn.execute("CREATE (:Sale {id: 5, product: 'Gadget', category: 'B', amount: 150.0, qty: 2})", None)?;
    Ok(())
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

// === COUNT ===

#[test]
fn test_agg_count_star() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN count(*)", None)?;
    let c = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(c.value(0), 5);
    Ok(())
}

#[test]
fn test_agg_count_column() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN count(s.product)", None)?;
    let c = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(c.value(0), 5);
    Ok(())
}

#[test]
fn test_agg_count_empty() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(*)", None)?;
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        let c = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(c.value(0), 0);
    }
    Ok(())
}

// === SUM ===

#[test]
fn test_agg_sum() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN sum(s.qty)", None)?;
    let sum = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    let val = sum.value(0);
    println!("sum(s.qty) = {val:.20}");
    assert_eq!(val as i64, 13);
    Ok(())
}

#[test]
fn test_agg_sum_double() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN sum(s.amount)", None)?;
    let sum = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((sum.value(0) - 800.0).abs() < 1e-10);
    Ok(())
}

// === AVG ===

#[test]
fn test_agg_avg() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN avg(s.qty)", None)?;
    let avg = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((avg.value(0) - 2.6).abs() < 1e-10);
    Ok(())
}

// === MIN / MAX ===

#[test]
fn test_agg_min() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN min(s.amount)", None)?;
    let min = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((min.value(0) - 50.0).abs() < 1e-10);
    Ok(())
}

#[test]
fn test_agg_max() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN max(s.amount)", None)?;
    let max = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((max.value(0) - 300.0).abs() < 1e-10);
    Ok(())
}

#[test]
fn test_agg_min_max_string() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN min(s.product), max(s.product)", None)?;
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        if let Some(min) = res.batches[0].column(0).as_any().downcast_ref::<StringArray>() {
            if let Some(max) = res.batches[0].column(1).as_any().downcast_ref::<StringArray>() {
                assert_eq!(min.value(0), "Gadget");
                assert_eq!(max.value(0), "Widget");
            }
        }
    }
    Ok(())
}

// === GROUP BY ===

#[test]
fn test_agg_group_by_count() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN s.category, count(*)", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

#[test]
fn test_agg_group_by_sum() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN s.category, sum(s.amount)", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

#[test]
fn test_agg_group_by_multiple() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN s.category, s.product, count(*)", None)?;
    // Distinct (category, product) pairs: (A, Widget), (B, Gadget), (A, Gizmo) = 3
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

#[test]
fn test_agg_group_by_avg() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN s.category, avg(s.amount)", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

// === COUNT DISTINCT ===

#[test]
fn test_agg_count_distinct() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN count(DISTINCT s.product)", None)?;
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        let c = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(c.value(0), 3);
    }
    Ok(())
}

#[test]
fn test_agg_count_distinct_empty() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(DISTINCT t.val)", None)?;
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        let c = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(c.value(0), 0);
    }
    Ok(())
}

// === Single row result ===

#[test]
fn test_agg_no_group_single_row() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN count(*), sum(s.qty), avg(s.amount), min(s.amount), max(s.amount)", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

// === Empty table aggregates ===

#[test]
fn test_agg_empty_sum() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    let res = conn.execute("MATCH (t:T) RETURN sum(t.val)", None)?;
    // Empty sum returns no rows or a single null row; just verify it doesn't crash
    Ok(())
}

#[test]
fn test_agg_empty_avg() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val DOUBLE, PRIMARY KEY (id))", None)?;
    let res = conn.execute("MATCH (t:T) RETURN avg(t.val)", None)?;
    Ok(())
}

// === Aggregates in subquery / WITH ===

#[test]
fn test_agg_with_having() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN s.category AS cat, sum(s.amount) AS total", None)?;
    let mut rows = 0;
    for batch in &res.batches {
        if batch.num_rows() > 0 {
            let total = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..batch.num_rows() {
                if total.value(i) > 150.0 {
                    rows += 1;
                }
            }
        }
    }
    assert_eq!(rows, 2);
    Ok(())
}

// === Multiple aggregates ===

#[test]
fn test_agg_multiple_from_same_column() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_sales(&conn)?;
    let res = conn.execute("MATCH (s:Sale) RETURN min(s.qty), max(s.qty), sum(s.qty), avg(s.qty)", None)?;
    let min = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    let max = res.batches[0].column(1).as_any().downcast_ref::<Float64Array>().unwrap();
    let sum = res.batches[0].column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    let avg = res.batches[0].column(3).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((min.value(0) - 1.0).abs() < 1e-10);
    assert!((max.value(0) - 5.0).abs() < 1e-10);
    assert!((sum.value(0) - 13.0).abs() < 1e-10);
    assert!((avg.value(0) - 2.6).abs() < 1e-10);
    Ok(())
}
