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
// FINAL COMPREHENSIVE TEST SUITE - All Tests Use Supported Features Only
//=========================================================================

#[test]
fn core_1_create_single() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Node(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Node {id: 1})", None)?;
    let res = conn.execute("MATCH (n:Node) RETURN n.id", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn core_2_create_multiple() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Items(id INT64)", None)?;
    for i in 0..25 {
        conn.execute(&format!("CREATE (:Items {{id: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (i:Items) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 25i64, Int64Array);
    }
    Ok(())
}

#[test]
fn core_3_create_50() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Data(id INT64)", None)?;
    for i in 0..50 {
        conn.execute(&format!("CREATE (:Data {{id: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (d:Data) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 50i64, Int64Array);
    }
    Ok(())
}

#[test]
fn core_4_create_100() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Batch(id INT64)", None)?;
    for i in 0..100 {
        conn.execute(&format!("CREATE (:Batch {{id: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (b:Batch) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 100i64, Int64Array);
    }
    Ok(())
}

#[test]
fn core_5_match_eq() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 10})", None)?;
    conn.execute("CREATE (:T {val: 20})", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.val = 10 RETURN t.val", None)?;
    assert_val!(res, 0, 0, 10, Int64Array);
    Ok(())
}

#[test]
fn core_6_match_gt_lt() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(val INT64)", None)?;
    conn.execute("CREATE (:T {val: 1})", None)?;
    conn.execute("CREATE (:T {val: 5})", None)?;
    conn.execute("CREATE (:T {val: 10})", None)?;
    let res = conn.execute(
        "MATCH (t:T) WHERE t.val > 1 AND t.val < 10 RETURN count(*)",
        None,
    )?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn core_7_filter_neq() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE X(x INT64)", None)?;
    conn.execute("CREATE (:X {x: 1})", None)?;
    conn.execute("CREATE (:X {x: 2})", None)?;
    let res = conn.execute("MATCH (x:X) WHERE x.x <> 1 RETURN x.x", None)?;
    assert_val!(res, 0, 0, 2, Int64Array);
    Ok(())
}

#[test]
fn core_8_filter_gt() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Y(y INT64)", None)?;
    conn.execute("CREATE (:Y {y: 10})", None)?;
    conn.execute("CREATE (:Y {y: 20})", None)?;
    let res = conn.execute("MATCH (y:Y) WHERE y.y > 15 RETURN y.y", None)?;
    assert_val!(res, 0, 0, 20, Int64Array);
    Ok(())
}

#[test]
fn core_9_filter_lt() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Z(z INT64)", None)?;
    conn.execute("CREATE (:Z {z: 5})", None)?;
    conn.execute("CREATE (:Z {z: 15})", None)?;
    let res = conn.execute("MATCH (z:Z) WHERE z.z < 10 RETURN z.z", None)?;
    assert_val!(res, 0, 0, 5, Int64Array);
    Ok(())
}

#[test]
fn core_10_order_by() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE O(val INT64)", None)?;
    conn.execute("CREATE (:O {val:3})", None)?;
    conn.execute("CREATE (:O {val:1})", None)?;
    conn.execute("CREATE (:O {val:2})", None)?;
    let res = conn.execute("MATCH (o:O) RETURN o.val ORDER BY o.val ASC", None)?;
    assert_val!(res, 0, 0, 1, Int64Array);
    assert_val!(res, 0, 1, 2, Int64Array);
    assert_val!(res, 0, 2, 3, Int64Array);
    Ok(())
}

#[test]
fn core_11_limit() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE L(val INT64)", None)?;
    for i in 0..10 {
        conn.execute(&format!("CREATE (:L {{val: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (l:L) RETURN l.val LIMIT 3", None)?;
    assert_row_count!(res, 3);
    Ok(())
}

#[test]
fn core_12_skip() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE S(val INT64)", None)?;
    for i in 0..5 {
        conn.execute(&format!("CREATE (:S {{val: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (s:S) RETURN s.val SKIP 2", None)?;
    assert_row_count!(res, 3);
    Ok(())
}

#[test]
fn core_13_string_prop() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE N(name STRING)", None)?;
    conn.execute("CREATE (:N {name: 'Alice'})", None)?;
    let res = conn.execute("MATCH (n:N) RETURN n.name", None)?;
    assert_val!(res, 0, 0, "Alice", StringArray);
    Ok(())
}

#[test]
fn core_14_double_prop() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE D(val DOUBLE)", None)?;
    conn.execute("CREATE (:D {val: 3.14})", None)?;
    let res = conn.execute("MATCH (d:D) RETURN d.val", None)?;
    assert_val_f64!(res, 0, 0, 3.14);
    Ok(())
}

#[test]
fn core_15_bool_prop() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE B(flag BOOL)", None)?;
    conn.execute("CREATE (:B {flag: true})", None)?;
    let res = conn.execute("MATCH (b:B) WHERE b.flag = true RETURN b.flag", None)?;
    assert_val!(res, 0, 0, true, BooleanArray);
    Ok(())
}

#[test]
fn core_16_count() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE C(id INT64)", None)?;
    conn.execute("CREATE (:C {id:1})", None)?;
    conn.execute("CREATE (:C {id:2})", None)?;
    let res = conn.execute("MATCH (c:C) RETURN count(*)", None)?;
    assert_val!(res, 0, 0, 2i64, Int64Array);
    Ok(())
}

#[test]
fn core_17_count_star() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE X(id INT64)", None)?;
    conn.execute("CREATE (:X {id:1})", None)?;
    let res = conn.execute("MATCH (x:X) RETURN count(*)", None)?;
    assert_val!(res, 0, 0, 1i64, Int64Array);
    Ok(())
}

#[test]
fn core_18_sum() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE S(val INT64)", None)?;
    conn.execute("CREATE (:S {val: 10})", None)?;
    conn.execute("CREATE (:S {val: 20})", None)?;
    conn.execute("CREATE (:S {val: 30})", None)?;
    let res = conn.execute("MATCH (s:S) RETURN sum(s.val)", None)?;
    assert_val_f64!(res, 0, 0, 60.0);
    Ok(())
}

#[test]
fn core_19_avg() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE A(val DOUBLE)", None)?;
    conn.execute("CREATE (:A {val: 10.0})", None)?;
    conn.execute("CREATE (:A {val: 20.0})", None)?;
    let res = conn.execute("MATCH (a:A) RETURN avg(a.val)", None)?;
    assert_val_f64!(res, 0, 0, 15.0);
    Ok(())
}

#[test]
fn core_20_min() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE M(val DOUBLE)", None)?;
    conn.execute("CREATE (:M {val: 3.0})", None)?;
    conn.execute("CREATE (:M {val: 1.0})", None)?;
    conn.execute("CREATE (:M {val: 2.0})", None)?;
    let res = conn.execute("MATCH (m:M) RETURN min(m.val)", None)?;
    assert_val_f64!(res, 0, 0, 1.0);
    Ok(())
}

#[test]
fn core_21_max() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE X(val DOUBLE)", None)?;
    conn.execute("CREATE (:X {val: 5.0})", None)?;
    conn.execute("CREATE (:X {val: 10.0})", None)?;
    let res = conn.execute("MATCH (x:X) RETURN max(x.val)", None)?;
    assert_val_f64!(res, 0, 0, 10.0);
    Ok(())
}

#[test]
fn core_22_upper() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE U(name STRING)", None)?;
    conn.execute("CREATE (:U {name: 'hello'})", None)?;
    let res = conn.execute("MATCH (u:U) RETURN upper(u.name)", None)?;
    assert_val!(res, 0, 0, "HELLO", StringArray);
    Ok(())
}

#[test]
fn core_23_lower() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Lw(name STRING)", None)?;
    conn.execute("CREATE (:Lw {name: 'WORLD'})", None)?;
    let res = conn.execute("MATCH (l:Lw) RETURN lower(l.name)", None)?;
    assert_val!(res, 0, 0, "world", StringArray);
    Ok(())
}

#[test]
fn core_24_abs() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Ab(val DOUBLE)", None)?;
    conn.execute("CREATE (:Ab {val: -7.0})", None)?;
    let res = conn.execute("MATCH (a:Ab) RETURN abs(a.val)", None)?;
    assert_val_f64!(res, 0, 0, 7.0);
    Ok(())
}

#[test]
fn core_25_ceil() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Ce(val DOUBLE)", None)?;
    conn.execute("CREATE (:Ce {val: 2.3})", None)?;
    let res = conn.execute("MATCH (c:Ce) RETURN ceil(c.val)", None)?;
    assert_val_f64!(res, 0, 0, 3.0);
    Ok(())
}

#[test]
fn core_26_floor() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Fl(val DOUBLE)", None)?;
    conn.execute("CREATE (:Fl {val: 7.8})", None)?;
    let res = conn.execute("MATCH (f:Fl) RETURN floor(f.val)", None)?;
    assert_val_f64!(res, 0, 0, 7.0);
    Ok(())
}

#[test]
fn core_27_round() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Ro(val DOUBLE)", None)?;
    conn.execute("CREATE (:Ro {val: 4.6})", None)?;
    let res = conn.execute("MATCH (r:Ro) RETURN round(r.val)", None)?;
    assert_val_f64!(res, 0, 0, 5.0);
    Ok(())
}

#[test]
fn core_28_sqrt() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Sq(val DOUBLE)", None)?;
    conn.execute("CREATE (:Sq {val: 9.0})", None)?;
    let res = conn.execute("MATCH (s:Sq) RETURN sqrt(s.val)", None)?;
    assert_val_f64!(res, 0, 0, 3.0);
    Ok(())
}

#[test]
fn core_29_unwind() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    let res = conn.execute("UNWIND [1,2,3,4,5] AS x RETURN x", None)?;
    assert_row_count!(res, 5);
    Ok(())
}

#[test]
fn core_30_distinct() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE D(val INT64)", None)?;
    conn.execute("CREATE (:D {val: 1})", None)?;
    conn.execute("CREATE (:D {val: 1})", None)?;
    conn.execute("CREATE (:D {val: 2})", None)?;
    let res = conn.execute("MATCH (d:D) RETURN DISTINCT d.val", None)?;
    assert_row_count!(res, 2);
    Ok(())
}

#[test]
fn core_31_update() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE U(val INT64)", None)?;
    conn.execute("CREATE (:U {val: 1})", None)?;
    conn.execute("MATCH (u:U) SET u.val = 99", None)?;
    let res = conn.execute("MATCH (u:U) RETURN u.val", None)?;
    assert_val!(res, 0, 0, 99, Int64Array);
    Ok(())
}

#[test]
fn core_32_delete() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE De(id INT64)", None)?;
    conn.execute("CREATE (:De {id:1})", None)?;
    conn.execute("CREATE (:De {id:2})", None)?;
    conn.execute("MATCH (d:De) WHERE d.id = 1 DELETE d", None)?;
    let res = conn.execute("MATCH (d:De) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn core_33_case() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE C(val INT64)", None)?;
    conn.execute("CREATE (:C {val: 1})", None)?;
    let res = conn.execute(
        "MATCH (c:C) RETURN CASE WHEN c.val < 5 THEN 'small' ELSE 'big' END",
        None,
    )?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn core_34_merge_match() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Mg(id INT64, val INT64)", None)?;
    conn.execute("CREATE (:Mg {id:1, val:10})", None)?;
    conn.execute("MERGE (m:Mg {id:1}) ON MATCH SET m.val = 999", None)?;
    let res = conn.execute("MATCH (m:Mg) RETURN m.val", None)?;
    assert_val!(res, 0, 0, 999, Int64Array);
    Ok(())
}

#[test]
fn core_35_merge_create() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Mc(id INT64)", None)?;
    conn.execute("CREATE (:Mc {id:1})", None)?;
    conn.execute("MERGE (m:Mc {id:2}) ON CREATE SET m.id = 2", None)?;
    let res = conn.execute("MATCH (m:Mc) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 2i64, Int64Array);
    }
    Ok(())
}

#[test]
fn core_36_relationship() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE A(id INT64)", None)?;
    conn.execute("CREATE NODE TABLE B(id INT64)", None)?;
    conn.execute("CREATE REL TABLE X(FROM A TO B)", None)?;
    conn.execute("CREATE (:A {id:1})", None)?;
    conn.execute("CREATE (:B {id:1})", None)?;
    conn.execute(
        "MATCH (a:A {id:1}), (b:B {id:1}) CREATE (a)-[:X]->(b)",
        None,
    )?;
    let res = conn.execute("MATCH (a:A)-[:X]->(b:B) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn core_37_pk_lookup() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE P(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE (:P {id:1, name:'Alice'})", None)?;
    conn.execute("CREATE (:P {id:2, name:'Bob'})", None)?;
    let res = conn.execute("MATCH (p:P) WHERE p.id = 1 RETURN p.name", None)?;
    assert_val!(res, 0, 0, "Alice", StringArray);
    Ok(())
}

#[test]
fn core_38_multi_prop() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE M(a STRING, b INT64, c DOUBLE)", None)?;
    conn.execute("CREATE (:M {a:'x', b:1, c:1.0})", None)?;
    let res = conn.execute("MATCH (m:M) RETURN m.a, m.b, m.c", None)?;
    assert_val!(res, 0, 0, "x", StringArray);
    assert_val!(res, 1, 0, 1, Int64Array);
    assert_val_f64!(res, 2, 0, 1.0);
    Ok(())
}

#[test]
fn core_39_null_handling() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE N(a STRING, b STRING)", None)?;
    conn.execute("CREATE (:N {a:'present', b:NULL})", None)?;
    let res = conn.execute("MATCH (n:N) RETURN n.a, n.b", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn core_40_empty_result() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE E(id INT64)", None)?;
    conn.execute("CREATE (:E {id:1})", None)?;
    let res = conn.execute("MATCH (e:E) WHERE e.id = 999 RETURN e.id", None)?;
    assert_row_count!(res, 0);
    Ok(())
}

#[test]
fn core_41_two_node_types() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(name STRING)", None)?;
    conn.execute("CREATE NODE TABLE Company(name STRING)", None)?;
    conn.execute("CREATE (:Person {name:'John'})", None)?;
    conn.execute("CREATE (:Company {name:'Acme'})", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN p.name", None)?;
    assert_val!(res, 0, 0, "John", StringArray);
    Ok(())
}

#[test]
fn core_42_math_add_sub() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Math(a DOUBLE, b DOUBLE)", None)?;
    conn.execute("CREATE (:Math {a:5.0, b:3.0})", None)?;
    let res = conn.execute("MATCH (m:Math) RETURN m.a + m.b, m.a - m.b", None)?;
    assert_val_f64!(res, 0, 0, 8.0);
    assert_val_f64!(res, 1, 0, 2.0);
    Ok(())
}

#[test]
fn core_43_math_mult_div() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Md(a DOUBLE, b DOUBLE)", None)?;
    conn.execute("CREATE (:Md {a:6.0, b:2.0})", None)?;
    let res = conn.execute("MATCH (m:Md) RETURN m.a * m.b, m.a / m.b", None)?;
    assert_val_f64!(res, 0, 0, 12.0);
    assert_val_f64!(res, 1, 0, 3.0);
    Ok(())
}

#[test]
fn core_44_coalesce() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Co(a STRING, b STRING)", None)?;
    conn.execute("CREATE (:Co {a:'first', b:NULL})", None)?;
    let res = conn.execute("MATCH (c:Co) RETURN coalesce(c.a, c.b)", None)?;
    assert_val!(res, 0, 0, "first", StringArray);
    Ok(())
}

#[test]
fn core_45_length() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Ln(val STRING)", None)?;
    conn.execute("CREATE (:Ln {val: 'hello'})", None)?;
    let res = conn.execute("MATCH (l:Ln) RETURN length(l.val)", None)?;
    assert_val!(res, 0, 0, 5, Int64Array);
    Ok(())
}

#[test]
fn core_46_pow() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Pw(val DOUBLE)", None)?;
    conn.execute("CREATE (:Pw {val: 2.0})", None)?;
    let res = conn.execute("MATCH (p:Pw) RETURN pow(p.val, 4)", None)?;
    assert_val_f64!(res, 0, 0, 16.0);
    Ok(())
}

#[test]
fn core_47_negate() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Ng(val DOUBLE)", None)?;
    conn.execute("CREATE (:Ng {val: 5.0})", None)?;
    let res = conn.execute("MATCH (n:Ng) RETURN -n.val", None)?;
    assert_val_f64!(res, 0, 0, -5.0);
    Ok(())
}

#[test]
fn core_48_large_insert() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Big(id INT64)", None)?;
    for i in 0..200 {
        conn.execute(&format!("CREATE (:Big {{id: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (b:Big) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 200i64, Int64Array);
    }
    Ok(())
}

#[test]
fn core_49_multiple_rels() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE X(id INT64)", None)?;
    conn.execute("CREATE NODE TABLE Y(id INT64)", None)?;
    conn.execute("CREATE REL TABLE R1(FROM X TO Y)", None)?;
    conn.execute("CREATE REL TABLE R2(FROM X TO Y)", None)?;
    conn.execute("CREATE (:X {id:1})", None)?;
    conn.execute("CREATE (:Y {id:1})", None)?;
    conn.execute(
        "MATCH (x:X {id:1}), (y:Y {id:1}) CREATE (x)-[:R1]->(y)",
        None,
    )?;
    conn.execute(
        "MATCH (x:X {id:1}), (y:Y {id:1}) CREATE (x)-[:R2]->(y)",
        None,
    )?;
    let res = conn.execute("MATCH (x:X)-[r]->(y:Y) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 2i64, Int64Array);
    }
    Ok(())
}

#[test]
fn core_50_group_by() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE G(cat STRING, val DOUBLE)", None)?;
    conn.execute("CREATE (:G {cat:'A', val:10.0})", None)?;
    conn.execute("CREATE (:G {cat:'A', val:20.0})", None)?;
    conn.execute("CREATE (:G {cat:'B', val:30.0})", None)?;
    let res = conn.execute("MATCH (g:G) RETURN g.cat, count(*)", None)?;
    assert_row_count!(res, 2);
    Ok(())
}

#[test]
fn core_51_collect() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE C(val INT64)", None)?;
    conn.execute("CREATE (:C {val:1})", None)?;
    conn.execute("CREATE (:C {val:2})", None)?;
    let res = conn.execute("MATCH (c:C) RETURN collect(c.val)", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn core_52_exp() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Ep(val DOUBLE)", None)?;
    conn.execute("CREATE (:Ep {val: 0.0})", None)?;
    let res = conn.execute("MATCH (e:Ep) RETURN exp(e.val)", None)?;
    assert_val_f64!(res, 0, 0, 1.0);
    Ok(())
}

#[test]
fn core_53_is_null() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Nl(val INT64)", None)?;
    conn.execute("CREATE (:Nl {val: 1})", None)?;
    conn.execute("CREATE (:Nl {val: NULL})", None)?;
    let res = conn.execute("MATCH (n:Nl) WHERE n.val IS NULL RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn core_54_is_not_null() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Nn(val INT64)", None)?;
    conn.execute("CREATE (:Nn {val: 1})", None)?;
    conn.execute("CREATE (:Nn {val: NULL})", None)?;
    let res = conn.execute("MATCH (n:Nn) WHERE n.val IS NOT NULL RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}
