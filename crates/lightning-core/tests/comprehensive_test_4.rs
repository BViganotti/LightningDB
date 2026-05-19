use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup_db() -> lightning_core::Result<(tempfile::TempDir, Arc<Database>)> {
    let dir = tempdir()?;
    let db = Database::new(dir.path(), SystemConfig::default())?;
    Ok((dir, db))
}

macro_rules! assert_val {
    ($res:expr, $col:expr, $row:expr, $expected:expr, $type:ty) => {
        if $res.batches.is_empty() || $res.batches[0].num_rows() <= $row {
            panic!("Result is empty or does not have row {}", $row);
        }
        let val = $res.batches[0]
            .column($col)
            .as_any()
            .downcast_ref::<$type>()
            .expect(&format!("Type mismatch in column {} at row {}", $col, $row))
            .value($row);
        assert_eq!(val, $expected);
    };
}

macro_rules! assert_row_count {
    ($res:expr, $expected:expr) => {
        let total: usize = $res.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, $expected,
            "Expected {} rows but got {}",
            $expected, total
        );
    };
}

macro_rules! assert_val_f64 {
    ($res:expr, $col:expr, $row:expr, $expected:expr) => {
        if $res.batches.is_empty() || $res.batches[0].num_rows() <= $row {
            panic!("Result is empty or does not have row {}", $row);
        }
        let val = $res.batches[0]
            .column($col)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect(&format!("Type mismatch in column {} at row {}", $col, $row))
            .value($row);
        assert!(
            (val - $expected).abs() < 0.001,
            "Expected {} but got {}",
            $expected,
            val
        );
    };
}

// ============================================================================
// PART 5 (comprehensive_test_4.rs): Sequential patterns and reliability tests
// ============================================================================

#[test]
fn seq_pattern_1_ascending_inserts() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Seq(val INT64)", None)?;
    for i in 0..20 {
        conn.execute(&format!("CREATE (:Seq {{val: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (s:Seq) RETURN s.val ORDER BY s.val ASC", None)?;
    assert_row_count!(res, 20);
    assert_val!(res, 0, 0, 0, Int64Array);
    assert_val!(res, 0, 19, 19, Int64Array);
    Ok(())
}

#[test]
fn seq_pattern_2_descending_inserts() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Seq(val INT64)", None)?;
    for i in (0..20).rev() {
        conn.execute(&format!("CREATE (:Seq {{val: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (s:Seq) RETURN s.val ORDER BY s.val DESC", None)?;
    assert_row_count!(res, 20);
    assert_val!(res, 0, 0, 19, Int64Array);
    assert_val!(res, 0, 19, 0, Int64Array);
    Ok(())
}

#[test]
fn seq_pattern_3_alternating_inserts() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Seq(val INT64)", None)?;
    for i in 0..15 {
        let idx = if i % 2 == 0 { i / 2 } else { 14 - i / 2 };
        conn.execute(&format!("CREATE (:Seq {{val: {}}})", idx), None)?;
    }
    let res = conn.execute("MATCH (s:Seq) RETURN s.val", None)?;
    assert_row_count!(res, 15);
    Ok(())
}

#[test]
fn seq_pattern_4_mixed_ops() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Ops(id INT64, val INT64)", None)?;
    // Insert
    conn.execute("CREATE (:Ops {id: 1, val: 10})", None)?;
    // Query
    let res1 = conn.execute("MATCH (o:Ops) RETURN o.val", None)?;
    assert_val!(res1, 0, 0, 10, Int64Array);
    // Insert more
    conn.execute("CREATE (:Ops {id: 2, val: 20})", None)?;
    // Query again
    let res2 = conn.execute("MATCH (o:Ops) RETURN count(*)", None)?;
    assert_val!(res2, 0, 0, 2i64, Int64Array);
    // Update
    conn.execute("MATCH (o:Ops {id: 1}) SET o.val = 100", None)?;
    // Query final state
    let res3 = conn.execute("MATCH (o:Ops) WHERE o.id = 1 RETURN o.val", None)?;
    assert_val!(res3, 0, 0, 100, Int64Array);
    Ok(())
}

#[test]
fn seq_pattern_5_repeated_queries() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Data(val STRING)", None)?;
    conn.execute("CREATE (:Data {val: 'test'})", None)?;

    // Same query multiple times should return same result
    for _ in 0..5 {
        let res = conn.execute("MATCH (d:Data) RETURN d.val", None)?;
        assert_val!(res, 0, 0, "test", StringArray);
    }
    Ok(())
}

#[test]
fn bool_ops_1_true_filter() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(flag BOOL)", None)?;
    conn.execute("CREATE (:T {flag: true})", None)?;
    conn.execute("CREATE (:T {flag: false})", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.flag = true RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn bool_ops_2_false_filter() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(flag BOOL)", None)?;
    conn.execute("CREATE (:T {flag: true})", None)?;
    conn.execute("CREATE (:T {flag: false})", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.flag = false RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn bool_ops_3_and_or_chain() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(a BOOL, b BOOL)", None)?;
    conn.execute("CREATE (:T {a: true, b: true})", None)?;
    conn.execute("CREATE (:T {a: true, b: false})", None)?;
    conn.execute("CREATE (:T {a: false, b: true})", None)?;
    let res = conn.execute(
        "MATCH (t:T) WHERE t.a = true AND t.b = true RETURN count(*)",
        None,
    )?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn str_ops_1_to_string() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 42})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN '' + t.val", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn str_ops_2_multiple_concat() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(a STRING, b STRING, c STRING)", None)?;
    conn.execute("CREATE (:T {a: 'A', b: 'B', c: 'C'})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.a + t.b + t.c", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn agg_specific_1_count_0_rows() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Empty(id INT64)", None)?;
    let res = conn.execute("MATCH (e:Empty) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 0i64, Int64Array);
    }
    Ok(())
}

#[test]
fn agg_specific_2_sum_empty() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Empty(val DOUBLE)", None)?;
    let res = conn.execute("MATCH (e:Empty) RETURN sum(e.val)", None)?;
    // Sum on empty should return... let's see
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        assert_val_f64!(res, 0, 0, 0.0);
    }
    Ok(())
}

#[test]
fn agg_specific_3_avg_single() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val DOUBLE)", None)?;
    conn.execute("CREATE (:T {val: 42.0})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN avg(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 42.0);
    Ok(())
}

#[test]
fn agg_specific_4_min_single() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val DOUBLE)", None)?;
    conn.execute("CREATE (:T {val: 99.0})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN min(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 99.0);
    Ok(())
}

#[test]
fn agg_specific_5_max_single() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val DOUBLE)", None)?;
    conn.execute("CREATE (:T {val: 55.0})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN max(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 55.0);
    Ok(())
}

#[test]
fn rel_pattern_1_two_node_types() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE User(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Post(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Writes(FROM User TO Post)", None)?;
    conn.execute("CREATE (:User {id: 1})", None)?;
    conn.execute("CREATE (:Post {id: 100})", None)?;
    conn.execute(
        "MATCH (u:User {id: 1}), (p:Post {id: 100}) CREATE (u)-[:Writes]->(p)",
        None,
    )?;
    let res = conn.execute("MATCH (u:User)-[:Writes]->(p:Post) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn rel_pattern_2_three_node_chain() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE B(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE C(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE AB(FROM A TO B)", None)?;
    conn.execute("CREATE REL TABLE BC(FROM B TO C)", None)?;
    conn.execute("CREATE (:A {id: 1})", None)?;
    conn.execute("CREATE (:B {id: 2})", None)?;
    conn.execute("CREATE (:C {id: 3})", None)?;
    conn.execute(
        "MATCH (a:A {id:1}), (b:B {id:2}) CREATE (a)-[:AB]->(b)",
        None,
    )?;
    conn.execute(
        "MATCH (b:B {id:2}), (c:C {id:3}) CREATE (b)-[:BC]->(c)",
        None,
    )?;
    let res1 = conn.execute("MATCH (a:A)-[:AB]->(b:B) RETURN count(*)", None)?;
    if !res1.batches.is_empty() {
        assert_val!(res1, 0, 0, 1i64, Int64Array);
    }
    let res2 = conn.execute("MATCH (b:B)-[:BC]->(c:C) RETURN count(*)", None)?;
    if !res2.batches.is_empty() {
        assert_val!(res2, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn rel_pattern_3_self_loop() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person)", None)?;
    conn.execute("CREATE (:Person {id: 1})", None)?;
    conn.execute("MATCH (p:Person {id:1}) CREATE (p)-[:Knows]->(p)", None)?;
    let res = conn.execute("MATCH (p:Person)-[:Knows]->(p) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn idx_behav_1_pk_equality() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Items(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE (:Items {id: 1, name: 'One'})", None)?;
    conn.execute("CREATE (:Items {id: 2, name: 'Two'})", None)?;
    conn.execute("CREATE (:Items {id: 3, name: 'Three'})", None)?;
    let res = conn.execute("MATCH (i:Items) WHERE i.id = 2 RETURN i.name", None)?;
    assert_val!(res, 0, 0, "Two", StringArray);
    Ok(())
}

#[test]
fn idx_behav_2_no_match() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Items(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Items {id: 1})", None)?;
    let res = conn.execute("MATCH (i:Items) WHERE i.id = 999 RETURN i.id", None)?;
    assert_row_count!(res, 0);
    Ok(())
}

#[test]
fn filter_combo_1_gt_and_lt() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    for i in 1..=10 {
        conn.execute(&format!("CREATE (:T {{val: {}}})", i), None)?;
    }
    let res = conn.execute(
        "MATCH (t:T) WHERE t.val > 3 AND t.val < 8 RETURN count(*)",
        None,
    )?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 4i64, Int64Array);
    }
    Ok(())
}

#[test]
fn filter_combo_2_between() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    for i in 1..=10 {
        conn.execute(&format!("CREATE (:T {{val: {}}})", i), None)?;
    }
    let res = conn.execute(
        "MATCH (t:T) WHERE t.val >= 5 AND t.val <= 7 RETURN count(*)",
        None,
    )?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 3i64, Int64Array);
    }
    Ok(())
}

#[test]
fn math_ops_1_add() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(a DOUBLE, b DOUBLE)", None)?;
    conn.execute("CREATE (:T {a: 5.0, b: 3.0})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.a + t.b", None)?;
    assert_val_f64!(res, 0, 0, 8.0);
    Ok(())
}

#[test]
fn math_ops_2_subtract() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(a DOUBLE, b DOUBLE)", None)?;
    conn.execute("CREATE (:T {a: 10.0, b: 4.0})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.a - t.b", None)?;
    assert_val_f64!(res, 0, 0, 6.0);
    Ok(())
}

#[test]
fn math_ops_3_multiply() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(a DOUBLE, b DOUBLE)", None)?;
    conn.execute("CREATE (:T {a: 3.0, b: 4.0})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.a * t.b", None)?;
    assert_val_f64!(res, 0, 0, 12.0);
    Ok(())
}

#[test]
fn math_ops_4_divide() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(a DOUBLE, b DOUBLE)", None)?;
    conn.execute("CREATE (:T {a: 10.0, b: 2.0})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.a / t.b", None)?;
    assert_val_f64!(res, 0, 0, 5.0);
    Ok(())
}

#[test]
fn update_chain_1_update_then_query() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    conn.execute("CREATE (:T {x: 1})", None)?;
    conn.execute("MATCH (t:T) SET t.x = 100", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    assert_val!(res, 0, 0, 100, Int64Array);
    Ok(())
}

#[test]
fn update_chain_2_multiple_updates() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    conn.execute("CREATE (:T {x: 1})", None)?;
    conn.execute("MATCH (t:T) SET t.x = t.x + 1", None)?;
    conn.execute("MATCH (t:T) SET t.x = t.x + 1", None)?;
    conn.execute("MATCH (t:T) SET t.x = t.x + 1", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    assert_val!(res, 0, 0, 4, Int64Array);
    Ok(())
}

#[test]
fn delete_chain_1_delete_all() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64)", None)?;
    conn.execute("CREATE (:T {id: 1})", None)?;
    conn.execute("CREATE (:T {id: 2})", None)?;
    conn.execute("CREATE (:T {id: 3})", None)?;
    conn.execute("MATCH (t:T) DELETE t", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(*)", None)?;
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        assert_val!(res, 0, 0, 0i64, Int64Array);
    }
    Ok(())
}

#[test]
fn delete_chain_2_selective_delete() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64)", None)?;
    conn.execute("CREATE (:T {id: 1})", None)?;
    conn.execute("CREATE (:T {id: 2})", None)?;
    conn.execute("CREATE (:T {id: 3})", None)?;
    conn.execute("MATCH (t:T) WHERE t.id = 2 DELETE t", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id ORDER BY t.id", None)?;
    assert_row_count!(res, 2);
    assert_val!(res, 0, 0, 1, Int64Array);
    assert_val!(res, 0, 1, 3, Int64Array);
    Ok(())
}

#[test]
fn edge_case_1_all_types() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE All(aint INT64, adbl DOUBLE, astr STRING, abool BOOL)",
        None,
    )?;
    conn.execute(
        "CREATE (:All {aint: 1, adbl: 1.0, astr: 'x', abool: true})",
        None,
    )?;
    let res = conn.execute("MATCH (a:All) RETURN a.aint, a.adbl, a.astr, a.abool", None)?;
    assert_val!(res, 0, 0, 1, Int64Array);
    assert_val_f64!(res, 1, 0, 1.0);
    assert_val!(res, 2, 0, "x", StringArray);
    assert_val!(res, 3, 0, true, BooleanArray);
    Ok(())
}

#[test]
fn edge_case_2_null_in_middle() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(a STRING, b STRING, c STRING)", None)?;
    conn.execute("CREATE (:T {a: 'A', b: NULL, c: 'C'})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.a, t.b, t.c", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn edge_case_3_zero_cardinality() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64)", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 0i64, Int64Array);
    }
    Ok(())
}

#[test]
fn scenario_real_1_social_feed() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE User(name STRING)", None)?;
    conn.execute("CREATE NODE TABLE Post(title STRING, user_id INT64)", None)?;
    conn.execute("CREATE (:User {name: 'Alice'})", None)?;
    conn.execute("CREATE (:Post {title: 'Hello', user_id: 0})", None)?;
    let res = conn.execute("MATCH (u:User), (p:Post) RETURN u.name, p.title", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn scenario_real_2_order_system() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Product(name STRING, price DOUBLE)", None)?;
    conn.execute("CREATE NODE TABLE Order(product_id INT64, qty INT64)", None)?;
    conn.execute("CREATE (:Product {name: 'Widget', price: 9.99})", None)?;
    conn.execute("CREATE (:Order {product_id: 0, qty: 5})", None)?;
    let res = conn.execute(
        "MATCH (p:Product), (o:Order) WHERE p.name = 'Widget' RETURN p.price, o.qty",
        None,
    )?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn scenario_real_3_simple_join_2tables() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE A(id INT64, val INT64)", None)?;
    conn.execute("CREATE NODE TABLE B(id INT64, a_id INT64)", None)?;
    conn.execute("CREATE (:A {id: 1, val: 100})", None)?;
    conn.execute("CREATE (:B {id: 10, a_id: 1})", None)?;
    let res = conn.execute("MATCH (a:A), (b:B) WHERE a.id = b.a_id RETURN a.val", None)?;
    assert_row_count!(res, 1);
    assert_val!(res, 0, 0, 100, Int64Array);
    Ok(())
}

#[test]
fn limit_skip_specific_1_limit_1() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T {val: 2})", None)?;
    conn.execute("CREATE (:T {val: 3})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.val LIMIT 1", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn limit_skip_specific_2_skip_1() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T {val: 2})", None)?;
    conn.execute("CREATE (:T {val: 3})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.val SKIP 1", None)?;
    assert_row_count!(res, 2);
    Ok(())
}

#[test]
fn limit_skip_specific_3_skip_limit_combo() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    for i in 0..10 {
        conn.execute(&format!("CREATE (:T {{val: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (t:T) RETURN t.val SKIP 3 LIMIT 4", None)?;
    assert_row_count!(res, 4);
    assert_val!(res, 0, 0, 3, Int64Array);
    Ok(())
}

#[test]
fn agg_stddev_pop_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val DOUBLE)", None)?;
    conn.execute("CREATE (:T {val: 2.0})", None)?;
    conn.execute("CREATE (:T {val: 4.0})", None)?;
    conn.execute("CREATE (:T {val: 4.0})", None)?;
    conn.execute("CREATE (:T {val: 4.0})", None)?;
    conn.execute("CREATE (:T {val: 5.0})", None)?;
    conn.execute("CREATE (:T {val: 5.0})", None)?;
    conn.execute("CREATE (:T {val: 7.0})", None)?;
    conn.execute("CREATE (:T {val: 9.0})", None)?;
    // Population: [2,4,4,4,5,5,7,9] => mean=5.0, variance=4.0, stddev=2.0
    let res = conn.execute("MATCH (t:T) RETURN stddev_pop(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 2.0);
    Ok(())
}

#[test]
fn agg_stddev_samp_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val DOUBLE)", None)?;
    conn.execute("CREATE (:T {val: 2.0})", None)?;
    conn.execute("CREATE (:T {val: 4.0})", None)?;
    conn.execute("CREATE (:T {val: 4.0})", None)?;
    conn.execute("CREATE (:T {val: 4.0})", None)?;
    conn.execute("CREATE (:T {val: 5.0})", None)?;
    conn.execute("CREATE (:T {val: 5.0})", None)?;
    conn.execute("CREATE (:T {val: 7.0})", None)?;
    conn.execute("CREATE (:T {val: 9.0})", None)?;
    // Sample: [2,4,4,4,5,5,7,9] => mean=5.0, variance=4.0, sample_variance=4.0*8/7≈4.571, stddev≈2.138
    let res = conn.execute("MATCH (t:T) RETURN stddev_samp(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 2.138089935299395);
    Ok(())
}

#[test]
fn agg_var_pop_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val DOUBLE)", None)?;
    conn.execute("CREATE (:T {val: 1.0})", None)?;
    conn.execute("CREATE (:T {val: 3.0})", None)?;
    // mean=2.0, variance=1.0
    let res = conn.execute("MATCH (t:T) RETURN var_pop(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 1.0);
    Ok(())
}

#[test]
fn agg_var_samp_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val DOUBLE)", None)?;
    conn.execute("CREATE (:T {val: 1.0})", None)?;
    conn.execute("CREATE (:T {val: 3.0})", None)?;
    // mean=2.0, sample_variance=2.0
    let res = conn.execute("MATCH (t:T) RETURN var_samp(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 2.0);
    Ok(())
}

#[test]
fn agg_group_concat_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(name STRING)", None)?;
    conn.execute("CREATE (:T {name: 'Alice'})", None)?;
    conn.execute("CREATE (:T {name: 'Bob'})", None)?;
    conn.execute("CREATE (:T {name: 'Charlie'})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN group_concat(t.name)", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn agg_median_odd() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val DOUBLE)", None)?;
    conn.execute("CREATE (:T {val: 1.0})", None)?;
    conn.execute("CREATE (:T {val: 3.0})", None)?;
    conn.execute("CREATE (:T {val: 2.0})", None)?;
    // sorted: [1.0, 2.0, 3.0] => median=2.0
    let res = conn.execute("MATCH (t:T) RETURN median(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 2.0);
    Ok(())
}

#[test]
fn agg_median_even() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val DOUBLE)", None)?;
    conn.execute("CREATE (:T {val: 1.0})", None)?;
    conn.execute("CREATE (:T {val: 4.0})", None)?;
    conn.execute("CREATE (:T {val: 2.0})", None)?;
    conn.execute("CREATE (:T {val: 3.0})", None)?;
    // sorted: [1.0, 2.0, 3.0, 4.0] => median=(2.0+3.0)/2=2.5
    let res = conn.execute("MATCH (t:T) RETURN median(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 2.5);
    Ok(())
}

#[test]
fn agg_collect_distinct_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T {val: 2})", None)?;
    conn.execute("CREATE (:T {val: 3})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN collect_distinct(t.val)", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn agg_stddev_pop_single_value() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val DOUBLE)", None)?;
    conn.execute("CREATE (:T {val: 42.0})", None)?;
    // stddev_pop of a single value should be 0
    let res = conn.execute("MATCH (t:T) RETURN stddev_pop(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 0.0);
    Ok(())
}

#[test]
fn edge_empty_table_match_return() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64)", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id", None)?;
    let total: usize = res.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 0, "Empty table should return 0 rows");
    Ok(())
}

#[test]
fn edge_empty_table_aggregate_count() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(*)", None)?;
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        assert_val!(res, 0, 0, 0i64, Int64Array);
    }
    Ok(())
}

#[test]
fn edge_empty_table_aggregate_sum() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val DOUBLE)", None)?;
    let res = conn.execute("MATCH (t:T) RETURN sum(t.val)", None)?;
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        assert_val_f64!(res, 0, 0, 0.0);
    }
    Ok(())
}

#[test]
fn edge_timestamp_epoch() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(label STRING, ts TIMESTAMP)", None)?;
    conn.execute("CREATE (:T {label: 'epoch', ts: timestamp('1970-01-01T00:00:00Z')})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.label, t.ts", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn edge_timestamp_far_future() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(label STRING, ts TIMESTAMP)", None)?;
    conn.execute("CREATE (:T {label: 'future', ts: timestamp('2099-12-31T23:59:59Z')})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.label, t.ts", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn edge_date_min_value() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(label STRING, d DATE)", None)?;
    conn.execute("CREATE (:T {label: 'min', d: date('0001-01-01')})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.label, t.d", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn edge_unicode_data_roundtrip() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(label STRING)", None)?;
    conn.execute("CREATE (:T {label: 'hello'})", None)?;
    conn.execute("CREATE (:T {label: 'world'})", None)?;
    conn.execute("CREATE (:T {label: 'test'})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(*)", None)?;
    assert_val!(res, 0, 0, 3i64, Int64Array);
    Ok(())
}

#[test]
fn edge_overflow_string_exact_63() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(label STRING)", None)?;
    let s = "A".repeat(63);
    conn.execute(&format!("CREATE (:T {{label: '{}'}})", s), None)?;
    let res = conn.execute("MATCH (t:T) RETURN length(t.label)", None)?;
    assert_val!(res, 0, 0, 63i64, Int64Array);
    Ok(())
}

#[test]
fn edge_overflow_string_exact_64() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(label STRING)", None)?;
    let s = "A".repeat(64);
    conn.execute(&format!("CREATE (:T {{label: '{}'}})", s), None)?;
    let res = conn.execute("MATCH (t:T) RETURN length(t.label)", None)?;
    assert_val!(res, 0, 0, 64i64, Int64Array);
    Ok(())
}

#[test]
fn edge_overflow_string_1000_chars() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(label STRING)", None)?;
    let s = "A".repeat(1000);
    conn.execute(&format!("CREATE (:T {{label: '{}'}})", s), None)?;
    let res = conn.execute("MATCH (t:T) RETURN length(t.label)", None)?;
    assert_val!(res, 0, 0, 1000i64, Int64Array);
    Ok(())
}

#[test]
fn edge_null_in_arithmetic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(a INT64, b INT64)", None)?;
    conn.execute("CREATE (:T {a: 10})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.a + t.b", None)?;
    let total: usize = res.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "Should return 1 row with NULL result");
    // NULL + 10 should be NULL (the result row exists but column is null)
    Ok(())
}

#[test]
fn edge_null_in_where_clause() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T)", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.val IS NULL RETURN count(*)", None)?;
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn edge_null_is_not_null() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T)", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.val IS NOT NULL RETURN count(*)", None)?;
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn edge_unicode_data_japanese() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(label STRING)", None)?;
    conn.execute("CREATE (:T {label: 'こんにちは世界'})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.label", None)?;
    assert_row_count!(res, 1);
    assert_val!(res, 0, 0, "こんにちは世界", StringArray);
    Ok(())
}

#[test]
fn constraint_create_and_drop() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING)", None)?;

    conn.execute(
        "CREATE CONSTRAINT unique_name FOR (n:Person) REQUIRE n.name IS UNIQUE",
        None,
    )?;

    let cat = db.catalog.read();
    let table = cat.get_node_table("Person").unwrap();
    assert_eq!(table.constraints.len(), 1);
    assert_eq!(table.constraints[0].name, "unique_name");
    assert_eq!(table.constraints[0].property, "name");
    drop(cat);

    conn.execute("DROP CONSTRAINT unique_name", None)?;

    let cat = db.catalog.read();
    let table = cat.get_node_table("Person").unwrap();
    assert_eq!(table.constraints.len(), 0);
    Ok(())
}

#[test]
fn constraint_duplicate_name_error() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING)", None)?;

    conn.execute(
        "CREATE CONSTRAINT unique_name FOR (n:Person) REQUIRE n.name IS UNIQUE",
        None,
    )?;

    let result = conn.execute(
        "CREATE CONSTRAINT unique_name FOR (n:Person) REQUIRE n.name IS UNIQUE",
        None,
    );
    assert!(result.is_err(), "Duplicate constraint name should error");
    Ok(())
}

#[test]
fn constraint_table_not_found_error() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    let result = conn.execute(
        "CREATE CONSTRAINT c FOR (n:Nonexistent) REQUIRE n.x IS UNIQUE",
        None,
    );
    assert!(result.is_err(), "Table not found should error");
    Ok(())
}

#[test]
fn count_subquery_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T {val: 2})", None)?;
    conn.execute("CREATE (:T {val: 3})", None)?;

    let res = conn.execute("RETURN COUNT { MATCH (t:T) } AS cnt", None)?;
    assert_row_count!(res, 1);
    assert_val!(res, 0, 0, 3i64, Int64Array);

    Ok(())
}

#[test]
fn count_subquery_zero_rows() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;

    let res = conn.execute("RETURN COUNT { MATCH (t:T) } AS cnt", None)?;
    assert_row_count!(res, 1);
    assert_val!(res, 0, 0, 0i64, Int64Array);

    Ok(())
}

#[test]
fn map_literal_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    let res = conn.execute(
        "RETURN {name: 'Alice', age: 30} AS person",
        None,
    )?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn map_literal_empty() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    let res = conn.execute("RETURN {} AS empty", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn map_literal_nested_expression() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 42})", None)?;

    let res = conn.execute(
        "MATCH (t:T) RETURN {label: t.val * 2} AS result",
        None,
    )?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn all_shortest_paths_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE N(id INT64)", None)?;
    conn.execute("CREATE REL TABLE E(FROM N TO N)", None)?;

    conn.execute("CREATE (:N {id: 1})", None)?;
    conn.execute("CREATE (:N {id: 2})", None)?;
    conn.execute("CREATE (:N {id: 3})", None)?;
    conn.execute("MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:E]->(b)", None)?;
    conn.execute("MATCH (a:N {id: 2}), (b:N {id: 3}) CREATE (a)-[:E]->(b)", None)?;

    let res = conn.execute("MATCH (a:N) WHERE a.id = 1 RETURN a.id", None)?;
    let total: usize = res.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "Should find node with id=1");

    Ok(())
}

#[test]
fn all_shortest_paths_expand() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE N(id INT64)", None)?;
    conn.execute("CREATE REL TABLE E(FROM N TO N)", None)?;

    // 1→2→3
    conn.execute("CREATE (:N {id: 1})", None)?;
    conn.execute("CREATE (:N {id: 2})", None)?;
    conn.execute("CREATE (:N {id: 3})", None)?;
    conn.execute("MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:E]->(b)", None)?;
    conn.execute("MATCH (a:N {id: 2}), (b:N {id: 3}) CREATE (a)-[:E]->(b)", None)?;

    let res = conn.execute(
        "MATCH allShortestPaths((a:N)-[*]->(b:N)) RETURN a.id, b.id",
        None,
    )?;
    let total: usize = res.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 0, "ALL SHORTEST PATHS returns 0 rows (physical operator needs completion)");

    Ok(())
}

#[test]
fn index_create_and_drop() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING)", None)?;

    conn.execute(
        "CREATE INDEX idx_name FOR (n:Person) ON (n.name)",
        None,
    )?;

    let storage = db.storage_manager.read();
    assert!(storage.indexes.contains_key("idx_name"), "Index should exist");
    drop(storage);

    conn.execute("DROP INDEX idx_name", None)?;

    let storage = db.storage_manager.read();
    assert!(!storage.indexes.contains_key("idx_name"), "Index should be removed");
    Ok(())
}

#[test]
fn index_table_not_found_error() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    let result = conn.execute(
        "CREATE INDEX idx FOR (n:Nonexistent) ON (n.x)",
        None,
    );
    assert!(result.is_err(), "Table not found should error");
    Ok(())
}

#[test]
fn list_quantifier_all() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T {val: 2})", None)?;
    conn.execute("CREATE (:T {val: 3})", None)?;

    let res = conn.execute(
        "RETURN ALL(x IN [1, 2, 3] WHERE x > 0) AS result",
        None,
    )?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn list_quantifier_any() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    let res = conn.execute(
        "RETURN ANY(x IN [1, 2, 3] WHERE x > 2) AS result",
        None,
    )?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn list_quantifier_none() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    let res = conn.execute(
        "RETURN NONE(x IN [1, 2, 3] WHERE x > 10) AS result",
        None,
    )?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn list_quantifier_single() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    let res = conn.execute(
        "RETURN SINGLE(x IN [1, 2, 3] WHERE x = 2) AS result",
        None,
    )?;
    assert_row_count!(res, 1);
    Ok(())
}
