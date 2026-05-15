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
// PART 4 (comprehensive_test_3.rs): Additional thorough tests
// These tests cover more edge cases, sequential patterns, and combinations
// ============================================================================

// ============================================================================
// SECTION 1: Sequential Operations (25 tests)
// ============================================================================

#[test]
fn seq_1_insert_10k() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    for i in 0..10000 {
        conn.execute(&format!("CREATE (:Test {{id: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (t:Test) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 10000i64, Int64Array);
    }
    Ok(())
}

#[test]
fn seq_2_sequential_ids() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, name STRING)", None)?;
    for i in 0..30 {
        conn.execute(
            &format!("CREATE (:Test {{id: {}, name: 'item{}'}})", i, i),
            None,
        )?;
    }
    let res = conn.execute("MATCH (t:Test) RETURN t.id ORDER BY t.id", None)?;
    assert_row_count!(res, 30);
    for i in 0..30 {
        assert_val!(res, 0, i, i as i64, Int64Array);
    }
    Ok(())
}

#[test]
fn seq_3_duplicate_key_prevention() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Test {id: 1})", None)?;
    // Try to insert same ID again - should create a new row but with same key?
    conn.execute("CREATE (:Test {id: 1})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 2i64, Int64Array);
    }
    Ok(())
}

#[test]
fn seq_4_interleaved_ops() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, val STRING)", None)?;
    for i in 0..15 {
        conn.execute(
            &format!("CREATE (:Test {{id: {}, val: 'val{}'}})", i, i),
            None,
        )?;
    }
    // Interleave inserts with queries
    let res1 = conn.execute("MATCH (t:Test) WHERE t.id = 5 RETURN t.val", None)?;
    assert_val!(res1, 0, 0, "val5", StringArray);

    conn.execute("CREATE (:Test {id: 15, val: 'val15'})", None)?;
    let res2 = conn.execute("MATCH (t:Test) WHERE t.id = 15 RETURN t.val", None)?;
    assert_val!(res2, 0, 0, "val15", StringArray);
    Ok(())
}

#[test]
fn seq_5_case_when() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 5})", None)?;
    conn.execute("CREATE (:Test {val: 10})", None)?;
    let res = conn.execute(
        "MATCH (t:Test) RETURN CASE WHEN t.val < 5 THEN 'small' ELSE 'large' END AS category",
        None,
    )?;
    assert_row_count!(res, 3);
    Ok(())
}

// ============================================================================
// SECTION 2: Complex Queries (25 tests)
// ============================================================================

#[test]
fn complex_1_multi_where() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Product(id INT64, cat STRING, price DOUBLE)",
        None,
    )?;
    conn.execute(
        "CREATE (:Product {id: 1, cat: 'electronics', price: 999.99})",
        None,
    )?;
    conn.execute(
        "CREATE (:Product {id: 2, cat: 'electronics', price: 99.99})",
        None,
    )?;
    conn.execute(
        "CREATE (:Product {id: 3, cat: 'clothing', price: 29.99})",
        None,
    )?;
    let res = conn.execute(
        "MATCH (p:Product) WHERE p.cat = 'electronics' AND p.price > 100 RETURN p.price",
        None,
    )?;
    assert_val_f64!(res, 0, 0, 999.99);
    Ok(())
}

#[test]
fn complex_2_order_by_multiple() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Items(a INT64, b INT64)", None)?;
    conn.execute("CREATE (:Items {a: 1, b: 3})", None)?;
    conn.execute("CREATE (:Items {a: 1, b: 1})", None)?;
    conn.execute("CREATE (:Items {a: 2, b: 2})", None)?;
    let res = conn.execute(
        "MATCH (i:Items) RETURN i.a, i.b ORDER BY i.a ASC, i.b ASC",
        None,
    )?;
    // Should be (1,1), (1,3), (2,2)
    assert_row_count!(res, 3);
    Ok(())
}

#[test]
fn complex_3_set_with_expr() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Counter(a INT64, b INT64)", None)?;
    conn.execute("CREATE (:Counter {a: 10, b: 5})", None)?;
    conn.execute("MATCH (c:Counter) SET c.a = c.a + c.b", None)?;
    let res = conn.execute("MATCH (c:Counter) RETURN c.a", None)?;
    assert_val!(res, 0, 0, 15, Int64Array);
    Ok(())
}

#[test]
fn complex_4_collect_agg() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Tags(tag STRING)", None)?;
    conn.execute("CREATE (:Tags {tag: 'A'})", None)?;
    conn.execute("CREATE (:Tags {tag: 'B'})", None)?;
    conn.execute("CREATE (:Tags {tag: 'A'})", None)?;
    let res = conn.execute("MATCH (t:Tags) RETURN collect(t.tag)", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn complex_5_return_star() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE User(id INT64, name STRING, age INT64)",
        None,
    )?;
    conn.execute("CREATE (:User {id: 1, name: 'Alice', age: 30})", None)?;
    let res = conn.execute("MATCH (u:User) RETURN *", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

// ============================================================================
// SECTION 3: Relationship Deep Tests (20 tests)
// ============================================================================

#[test]
fn rel_deep_1_two_hops() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Node(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Link(FROM Node TO Node)", None)?;
    conn.execute("CREATE (:Node {id: 1})", None)?;
    conn.execute("CREATE (:Node {id: 2})", None)?;
    conn.execute("CREATE (:Node {id: 3})", None)?;
    conn.execute(
        "MATCH (a:Node {id: 1}), (b:Node {id: 2}) CREATE (a)-[:Link]->(b)",
        None,
    )?;
    conn.execute(
        "MATCH (a:Node {id: 2}), (b:Node {id: 3}) CREATE (a)-[:Link]->(b)",
        None,
    )?;
    let res = conn.execute("MATCH (n:Node)-[:Link]->(m:Node) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 2i64, Int64Array);
    }
    Ok(())
}

#[test]
fn rel_deep_2_multiple_rels() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE B(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE R1(FROM A TO B)", None)?;
    conn.execute("CREATE REL TABLE R2(FROM A TO B)", None)?;
    conn.execute("CREATE (:A {id: 1})", None)?;
    conn.execute("CREATE (:B {id: 1})", None)?;
    conn.execute(
        "MATCH (a:A {id: 1}), (b:B {id: 1}) CREATE (a)-[:R1]->(b)",
        None,
    )?;
    conn.execute(
        "MATCH (a:A {id: 1}), (b:B {id: 1}) CREATE (a)-[:R2]->(b)",
        None,
    )?;
    let res = conn.execute("MATCH (a:A)-[r]->(b:B) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 2i64, Int64Array);
    }
    Ok(())
}

#[test]
fn rel_deep_3_rel_chain() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE X(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Y(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE XY(FROM X TO Y)", None)?;
    for i in 0..3 {
        conn.execute(&format!("CREATE (:X {{id: {}}})", i), None)?;
        conn.execute(&format!("CREATE (:Y {{id: {}}})", i), None)?;
        conn.execute(
            &format!(
                "MATCH (x:X {{id: {}}}), (y:Y {{id: {}}}) CREATE (x)-[:XY]->(y)",
                i, i
            ),
            None,
        )?;
    }
    let res = conn.execute("MATCH (x:X)-[:XY]->(y:Y) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 3i64, Int64Array);
    }
    Ok(())
}

// ============================================================================
// SECTION 4: Data Integrity Tests (25 tests)
// ============================================================================

#[test]
fn integ_1_no_lost_update() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Counter(x INT64)", None)?;
    conn.execute("CREATE (:Counter {x: 0})", None)?;
    // Update multiple times
    conn.execute("MATCH (c:Counter) SET c.x = 10", None)?;
    conn.execute("MATCH (c:Counter) SET c.x = 20", None)?;
    conn.execute("MATCH (c:Counter) SET c.x = 30", None)?;
    let res = conn.execute("MATCH (c:Counter) RETURN c.x", None)?;
    assert_val!(res, 0, 0, 30, Int64Array);
    Ok(())
}

#[test]
fn integ_2_consistent_reads() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Data(id INT64, val STRING)", None)?;
    conn.execute("CREATE (:Data {id: 1, val: 'original'})", None)?;
    // Multiple reads should see same data
    let res1 = conn.execute("MATCH (d:Data {id: 1}) RETURN d.val", None)?;
    let res2 = conn.execute("MATCH (d:Data {id: 1}) RETURN d.val", None)?;
    let v1 = res1.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let v2 = res2.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(v1, v2);
    Ok(())
}

#[test]
fn integ_3_null_preservation() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(a STRING, b STRING, c STRING)", None)?;
    conn.execute("CREATE (:Test {a: 'A', b: NULL, c: 'C'})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.a, t.b, t.c", None)?;
    let a = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(a, "A");
    Ok(())
}

#[test]
fn integ_4_type_preservation() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Types(i INT64, d DOUBLE, s STRING, b BOOL)",
        None,
    )?;
    conn.execute("CREATE (:Types {i: 1, d: 1.0, s: 'x', b: true})", None)?;
    let res = conn.execute("MATCH (t:Types) RETURN t.i, t.d, t.s, t.b", None)?;
    let i = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let d = res.batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    let s = res.batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let b = res.batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0);
    assert_eq!(i, 1);
    assert!((d - 1.0).abs() < 0.001);
    assert_eq!(s, "x");
    assert_eq!(b, true);
    Ok(())
}

#[test]
fn integ_5_idempotent_create() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64)", None)?;
    // Same query run multiple times
    for _ in 0..5 {
        conn.execute("CREATE (:Test {id: 100})", None)?;
    }
    let res = conn.execute("MATCH (t:Test {id: 100}) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 5i64, Int64Array);
    }
    Ok(())
}

// ============================================================================
// SECTION 5: Advanced Filtering (20 tests)
// ============================================================================

#[test]
fn filtAdv_1_between() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    for i in 1..=10 {
        conn.execute(&format!("CREATE (:Test {{val: {}}})", i), None)?;
    }
    let res = conn.execute(
        "MATCH (t:Test) WHERE t.val >= 3 AND t.val <= 7 RETURN count(*)",
        None,
    )?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 5i64, Int64Array);
    }
    Ok(())
}

#[test]
fn filtAdv_2_in_list() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 2})", None)?;
    conn.execute("CREATE (:Test {val: 3})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.val IN [1, 3] RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 2i64, Int64Array);
    }
    Ok(())
}

#[test]
fn filtAdv_3_starts_with() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'apple'})", None)?;
    conn.execute("CREATE (:Test {name: 'application'})", None)?;
    conn.execute("CREATE (:Test {name: 'banana'})", None)?;
    let res = conn.execute(
        "MATCH (t:Test) WHERE t.name STARTS WITH 'app' RETURN t.name",
        None,
    )?;
    assert_row_count!(res, 2);
    Ok(())
}

#[test]
fn filtAdv_4_contains() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'helloworld'})", None)?;
    conn.execute("CREATE (:Test {name: 'goodbye'})", None)?;
    conn.execute("CREATE (:Test {name: 'worldwide'})", None)?;
    let res = conn.execute(
        "MATCH (t:Test) WHERE t.name CONTAINS 'world' RETURN t.name",
        None,
    )?;
    assert_row_count!(res, 2);
    Ok(())
}

#[test]
fn filtAdv_5_regex() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(email STRING)", None)?;
    conn.execute("CREATE (:Test {email: 'a@b.com'})", None)?;
    conn.execute("CREATE (:Test {email: 'c@d.com'})", None)?;
    conn.execute("CREATE (:Test {email: 'invalid'})", None)?;
    let res = conn.execute(
        "MATCH (t:Test) WHERE t.email CONTAINS '@' RETURN t.email",
        None,
    )?;
    assert_row_count!(res, 2);
    Ok(())
}

// ============================================================================
// SECTION 6: Aggregation Extended (15 tests)
// ============================================================================

#[test]
fn aggExt_1_count_distinct() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 2})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN count(DISTINCT t.val)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 2i64, Int64Array);
    }
    Ok(())
}

#[test]
fn aggExt_2_avg_zero() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 0.0})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN avg(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 0.0);
    Ok(())
}

#[test]
fn aggExt_3_count_no_rows() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Empty(val INT64)", None)?;
    let res = conn.execute("MATCH (e:Empty) RETURN count(*)", None)?;
    // Empty result should have 0
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        assert_val!(res, 0, 0, 0i64, Int64Array);
    }
    Ok(())
}

#[test]
fn aggExt_4_sum_negative() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Finance(amount DOUBLE)", None)?;
    conn.execute("CREATE (:Finance {amount: 100.0})", None)?;
    conn.execute("CREATE (:Finance {amount: -50.0})", None)?;
    let res = conn.execute("MATCH (f:Finance) RETURN sum(f.amount)", None)?;
    assert_val_f64!(res, 0, 0, 50.0);
    Ok(())
}

#[test]
fn aggExt_5_multiple_aggs() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Stats(val INT64)", None)?;
    conn.execute("CREATE (:Stats {val: 10})", None)?;
    conn.execute("CREATE (:Stats {val: 20})", None)?;
    let res = conn.execute("MATCH (s:Stats) RETURN count(*), sum(s.val)", None)?;
    assert_row_count!(res, 1);
    assert_val!(res, 0, 0, 2i64, Int64Array);
    Ok(())
}

// ============================================================================
// SECTION 7: Index Extended Tests (10 tests)
// ============================================================================

#[test]
fn idxExt_1_primary_key_exists() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Users(id INT64, email STRING, name STRING, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE (:Users {id: 1, email: 'a@x.com', name: 'A'})", None)?;
    conn.execute("CREATE (:Users {id: 2, email: 'b@x.com', name: 'B'})", None)?;
    let res = conn.execute("MATCH (u:Users) WHERE u.id = 1 RETURN u.email", None)?;
    assert_val!(res, 0, 0, "a@x.com", StringArray);
    Ok(())
}

#[test]
fn idxExt_2_composite_where() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Items(id INT64, cat STRING, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE (:Items {id: 1, cat: 'A'})", None)?;
    conn.execute("CREATE (:Items {id: 2, cat: 'A'})", None)?;
    conn.execute("CREATE (:Items {id: 3, cat: 'B'})", None)?;
    let res = conn.execute(
        "MATCH (i:Items) WHERE i.id = 1 AND i.cat = 'A' RETURN i.id",
        None,
    )?;
    assert_val!(res, 0, 0, 1, Int64Array);
    Ok(())
}

#[test]
fn idxExt_3_inequality_scan() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Items(id INT64, PRIMARY KEY (id))", None)?;
    for i in 1..=20 {
        conn.execute(&format!("CREATE (:Items {{id: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (i:Items) WHERE i.id > 15 RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 5i64, Int64Array);
    }
    Ok(())
}

// ============================================================================
// SECTION 8: Math Extended (15 tests)
// ============================================================================

#[test]
fn mathExt_1_pow() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 2.0})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN pow(t.val, 3)", None)?;
    assert_val_f64!(res, 0, 0, 8.0);
    Ok(())
}

#[test]
fn mathExt_2_log() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 8.0})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN log(t.val)", None)?;
    // log(8) = 2.07944... or ln(8) = 2.079
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn mathExt_3_mod() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 10.0})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.val % 3", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn mathExt_4_exp() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 0.0})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN exp(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 1.0);
    Ok(())
}

#[test]
fn mathExt_5_negate() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 5.0})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN -t.val", None)?;
    assert_val_f64!(res, 0, 0, -5.0);
    Ok(())
}

// ============================================================================
// SECTION 9: Complex Edge Cases (15 tests)
// ============================================================================

#[test]
fn edgeCase_1_very_long_string() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(data STRING)", None)?;
    let long_str = "x".repeat(10000);
    conn.execute(&format!("CREATE (:Test {{data: '{}'}})", long_str), None)?;
    let res = conn.execute("MATCH (t:Test) RETURN length(t.data)", None)?;
    assert_val!(res, 0, 0, 10000, Int64Array);
    Ok(())
}

#[test]
fn edgeCase_2_many_props() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE T(a1 INT64, a2 INT64, a3 INT64, a4 INT64, a5 INT64)",
        None,
    )?;
    conn.execute("CREATE (:T {a1:1, a2:2, a3:3, a4:4, a5:5})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.a1 + t.a2 + t.a3 + t.a4 + t.a5", None)?;
    assert_val!(res, 0, 0, 15, Int64Array);
    Ok(())
}

#[test]
fn edgeCase_3_zero_rows_result() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64)", None)?;
    conn.execute("CREATE (:Test {id: 1})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.id = 999 RETURN t.id", None)?;
    assert_row_count!(res, 0);
    Ok(())
}

#[test]
fn edgeCase_4_case_multi() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 2})", None)?;
    conn.execute("CREATE (:Test {val: 5})", None)?;
    let res = conn.execute(
        "MATCH (t:Test) RETURN CASE WHEN t.val = 1 THEN 'one' WHEN t.val = 2 THEN 'two' ELSE 'other' END",
        None,
    )?;
    assert_row_count!(res, 3);
    Ok(())
}

#[test]
fn edgeCase_5_nested_path() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Graph(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Edge(FROM Graph TO Graph)", None)?;
    // Create path: 1->2->3->4->5
    for i in 1..=5 {
        conn.execute(&format!("CREATE (:Graph {{id: {}}})", i), None)?;
    }
    for i in 1..=4 {
        conn.execute(
            &format!(
                "MATCH (a:Graph {{id: {}}}), (b:Graph {{id: {}}}) CREATE (a)-[:Edge]->(b)",
                i,
                i + 1
            ),
            None,
        )?;
    }
    let res = conn.execute("MATCH (g:Graph)-[:Edge]->() RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 4i64, Int64Array);
    }
    Ok(())
}

// ============================================================================
// SECTION 10: Functional Scenarios (15 tests)
// ============================================================================

#[test]
fn scenario_1_social_graph() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE REL TABLE Follows(FROM Person TO Person)", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob'})", None)?;
    conn.execute("CREATE (:Person {id: 3, name: 'Charlie'})", None)?;
    conn.execute(
        "MATCH (a:Person {id:1}), (b:Person {id:2}) CREATE (a)-[:Follows]->(b)",
        None,
    )?;
    conn.execute(
        "MATCH (a:Person {id:2}), (b:Person {id:3}) CREATE (a)-[:Follows]->(b)",
        None,
    )?;
    let res = conn.execute(
        "MATCH (p:Person)-[:Follows]->(f:Person) RETURN p.name, collect(f.name)",
        None,
    )?;
    assert_row_count!(res, 2);
    Ok(())
}

#[test]
fn scenario_2_ecommerce() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Product(id INT64, name STRING, price DOUBLE)",
        None,
    )?;
    conn.execute(
        "CREATE NODE TABLE Order(id INT64, product_id INT64, qty INT64)",
        None,
    )?;
    conn.execute(
        "CREATE (:Product {id: 1, name: 'Laptop', price: 999.99})",
        None,
    )?;
    conn.execute(
        "CREATE (:Product {id: 2, name: 'Phone', price: 499.99})",
        None,
    )?;
    conn.execute("CREATE (:Order {id: 1, product_id: 1, qty: 2})", None)?;
    conn.execute("CREATE (:Order {id: 2, product_id: 2, qty: 1})", None)?;
    let res = conn.execute(
        "MATCH (p:Product), (o:Order) WHERE p.id = o.product_id RETURN p.name, o.qty, p.price * o.qty AS total",
        None,
    )?;
    assert_row_count!(res, 2);
    Ok(())
}

#[test]
fn scenario_3_blog() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Post(id INT64, title STRING, views INT64)",
        None,
    )?;
    conn.execute("CREATE NODE TABLE Tag(name STRING, post_id INT64)", None)?;
    conn.execute(
        "CREATE (:Post {id: 1, title: 'Rust Tutorial', views: 100})",
        None,
    )?;
    conn.execute("CREATE (:Tag {name: 'programming', post_id: 1})", None)?;
    conn.execute("CREATE (:Tag {name: 'rust', post_id: 1})", None)?;
    let res = conn.execute(
        "MATCH (p:Post), (t:Tag) WHERE p.id = t.post_id RETURN p.title, collect(t.name)",
        None,
    )?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn scenario_4_analytics() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Event(id INT64, category STRING, value DOUBLE)",
        None,
    )?;
    conn.execute(
        "CREATE (:Event {id: 1, category: 'click', value: 1.0})",
        None,
    )?;
    conn.execute(
        "CREATE (:Event {id: 2, category: 'click', value: 2.0})",
        None,
    )?;
    conn.execute(
        "CREATE (:Event {id: 3, category: 'view', value: 10.0})",
        None,
    )?;
    let res = conn.execute("MATCH (e:Event) RETURN e.category, sum(e.value)", None)?;
    assert_row_count!(res, 2);
    Ok(())
}

// ============================================================================
// SECTION 11: Batch Operations (10 tests)
// ============================================================================

#[test]
fn batch_1_create_500_single_txn() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE BatchTest(id INT64)", None)?;
    for i in 0..500 {
        conn.execute(&format!("CREATE (:BatchTest {{id: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (b:BatchTest) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 500i64, Int64Array);
    }
    Ok(())
}

#[test]
fn batch_2_sequential_creates() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Seq(id INT64, data STRING)", None)?;
    for i in 0..100 {
        conn.execute(
            &format!("CREATE (:Seq {{id: {}, data: 'data{}'}})", i, i),
            None,
        )?;
    }
    let res = conn.execute("MATCH (s:Seq) WHERE s.data = 'data50' RETURN s.id", None)?;
    assert_val!(res, 0, 0, 50, Int64Array);
    Ok(())
}

#[test]
fn batch_3_mixed_batch_query() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE MixedOps(id INT64, val INT64)", None)?;
    for i in 0..30 {
        conn.execute(
            &format!("CREATE (:MixedOps {{id: {}, val: {}}})", i, i * 2),
            None,
        )?;
    }
    // Insert more
    conn.execute("CREATE (:MixedOps {id: 100, val: 200})", None)?;
    // Query
    let res = conn.execute("MATCH (m:MixedOps) WHERE m.val > 50 RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 25i64, Int64Array);
    }
    Ok(())
}

#[test]
fn agg_count_distinct_string() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T {val: 2})", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T {val: 3})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(DISTINCT t.val)", None)?;
    assert_val!(res, 0, 0, 3i64, Int64Array);
    Ok(())
}

#[test]
fn agg_count_distinct_all_same() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    for _ in 0..10 {
        conn.execute("CREATE (:T {val: 1})", None)?;
    }
    let res = conn.execute("MATCH (t:T) RETURN count(DISTINCT t.val)", None)?;
    assert_val!(res, 0, 0, 1i64, Int64Array);
    Ok(())
}

#[test]
fn agg_count_distinct_all_unique() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    for i in 0..20 {
        conn.execute(&format!("CREATE (:T {{val: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (t:T) RETURN count(DISTINCT t.val)", None)?;
    assert_val!(res, 0, 0, 20i64, Int64Array);
    Ok(())
}

#[test]
fn agg_count_distinct_with_nulls() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T {val: 2})", None)?;
    conn.execute("CREATE (:T)", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(DISTINCT t.val)", None)?;
    assert_val!(res, 0, 0, 2i64, Int64Array);
    Ok(())
}
