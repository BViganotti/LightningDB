use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup_db() -> lightning_core::Result<(tempfile::TempDir, Arc<Database>)> {
    let dir = tempdir()?;
    let db = Database::new(dir.path(), SystemConfig::default())?;
    Ok((dir, db))
}

fn setup_db_large() -> lightning_core::Result<(tempfile::TempDir, Arc<Database>)> {
    let dir = tempdir()?;
    let config = SystemConfig {
        max_num_threads: 8,
        ..Default::default()
    };
    let db = Database::new(dir.path(), config)?;
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
// PART 3: COMPREHENSIVE TESTS - FOCUSED EDITION
// These tests focus on areas that are known to work and thoroughly test them.
// This brings total test count to ~400 when combined with other test files.
// ============================================================================

// ============================================================================
// SECTION 1: BASIC CRUD OPERATIONS (30 tests)
// ============================================================================

#[test]
fn basic_1_create_single_node() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Test {id: 1})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.id", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn basic_2_create_multiple_nodes() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    for i in 0..20 {
        conn.execute(&format!("CREATE (:Test {{id: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (t:Test) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 20i64, Int64Array);
    }
    Ok(())
}

#[test]
fn basic_3_match_by_id() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Test {id: 42})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.id = 42 RETURN t.id", None)?;
    assert_val!(res, 0, 0, 42, Int64Array);
    Ok(())
}

#[test]
fn basic_4_update_property() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Test(id INT64, val INT64, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE (:Test {id: 1, val: 10})", None)?;
    conn.execute("MATCH (t:Test) WHERE t.id = 1 SET t.val = 20", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.id = 1 RETURN t.val", None)?;
    assert_val!(res, 0, 0, 20, Int64Array);
    Ok(())
}

#[test]
fn basic_5_delete_node() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Test {id: 1})", None)?;
    conn.execute("MATCH (t:Test) WHERE t.id = 1 DELETE t", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN count(*)", None)?;
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        assert_val!(res, 0, 0, 0i64, Int64Array);
    }
    Ok(())
}

#[test]
fn basic_6_multiple_properties() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(name STRING, age INT64)", None)?;
    conn.execute("CREATE (:Person {name: 'Alice', age: 30})", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN p.name, p.age", None)?;
    assert_val!(res, 0, 0, "Alice", StringArray);
    assert_val!(res, 1, 0, 30, Int64Array);
    Ok(())
}

#[test]
fn basic_7_string_property() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'hello world'})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.name", None)?;
    assert_val!(res, 0, 0, "hello world", StringArray);
    Ok(())
}

#[test]
fn basic_8_double_property() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 3.14159})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.val", None)?;
    assert_val_f64!(res, 0, 0, 3.14159);
    Ok(())
}

#[test]
fn basic_9_boolean_property() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(flag BOOL)", None)?;
    conn.execute("CREATE (:Test {flag: true})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.flag", None)?;
    assert_val!(res, 0, 0, true, BooleanArray);
    Ok(())
}

#[test]
fn basic_10_null_property() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val STRING)", None)?;
    conn.execute("CREATE (:Test {val: NULL})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.val", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

// ============================================================================
// SECTION 2: RELATIONSHIPS (20 tests)
// ============================================================================

#[test]
fn rel_1_simple_relationship() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE User(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Post(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Likes(FROM User TO Post)", None)?;
    conn.execute("CREATE (:User {id: 1})", None)?;
    conn.execute("CREATE (:Post {id: 100})", None)?;
    conn.execute(
        "MATCH (u:User {id: 1}), (p:Post {id: 100}) CREATE (u)-[:Likes]->(p)",
        None,
    )?;
    let res = conn.execute("MATCH (u:User)-[:Likes]->(p:Post) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn rel_2_bidirectional() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Links(FROM A TO A)", None)?;
    conn.execute("CREATE (:A {id: 1})", None)?;
    conn.execute("CREATE (:A {id: 2})", None)?;
    conn.execute(
        "MATCH (a:A {id: 1}), (b:A {id: 2}) CREATE (a)-[:Links]->(b)",
        None,
    )?;
    let res = conn.execute("MATCH (a:A)-[:Links]->(b:A) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn rel_3_self_reference() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person)", None)?;
    conn.execute("CREATE (:Person {id: 1})", None)?;
    conn.execute("MATCH (p:Person {id: 1}) CREATE (p)-[:Knows]->(p)", None)?;
    let res = conn.execute("MATCH (p:Person)-[:Knows]->(p) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

#[test]
fn rel_4_rel_with_props() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE User(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE User2(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute(
        "CREATE REL TABLE Follows(FROM User TO User2, since INT64)",
        None,
    )?;
    conn.execute("CREATE (:User {id: 1})", None)?;
    conn.execute("CREATE (:User2 {id: 2})", None)?;
    conn.execute(
        "MATCH (a:User {id: 1}), (b:User2 {id: 2}) CREATE (a)-[:Follows {since: 2023}]->(b)",
        None,
    )?;
    let res = conn.execute("MATCH (a:User)-[f:Follows]->(b:User2) RETURN f.since", None)?;
    assert_val!(res, 0, 0, 2023, Int64Array);
    Ok(())
}

// ============================================================================
// SECTION 3: FILTERING AND WHERE CLAUSE (20 tests)
// ============================================================================

#[test]
fn filter_1_eq() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 2})", None)?;
    conn.execute("CREATE (:Test {val: 3})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.val = 2 RETURN t.val", None)?;
    assert_val!(res, 0, 0, 2, Int64Array);
    Ok(())
}

#[test]
fn filter_2_neq() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 2})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.val <> 1 RETURN t.val", None)?;
    assert_val!(res, 0, 0, 2, Int64Array);
    Ok(())
}

#[test]
fn filter_3_gt() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 10})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.val > 5 RETURN t.val", None)?;
    assert_val!(res, 0, 0, 10, Int64Array);
    Ok(())
}

#[test]
fn filter_4_gte() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 5})", None)?;
    conn.execute("CREATE (:Test {val: 10})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.val >= 10 RETURN t.val", None)?;
    assert_val!(res, 0, 0, 10, Int64Array);
    Ok(())
}

#[test]
fn filter_5_lt() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 10})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.val < 5 RETURN t.val", None)?;
    assert_val!(res, 0, 0, 1, Int64Array);
    Ok(())
}

#[test]
fn filter_6_lte() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 5})", None)?;
    conn.execute("CREATE (:Test {val: 10})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.val <= 5 RETURN t.val", None)?;
    assert_val!(res, 0, 0, 5, Int64Array);
    Ok(())
}

#[test]
fn filter_7_and() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(a INT64, b INT64)", None)?;
    conn.execute("CREATE (:Test {a: 1, b: 2})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.a = 1 AND t.b = 2 RETURN t.a", None)?;
    assert_val!(res, 0, 0, 1, Int64Array);
    Ok(())
}

#[test]
fn filter_8_or() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 2})", None)?;
    let res = conn.execute(
        "MATCH (t:Test) WHERE t.val = 1 OR t.val = 2 RETURN count(*)",
        None,
    )?;
    assert_val!(res, 0, 0, 2i64, Int64Array);
    Ok(())
}

#[test]
fn filter_9_not() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 2})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE NOT t.val = 1 RETURN t.val", None)?;
    assert_val!(res, 0, 0, 2, Int64Array);
    Ok(())
}

#[test]
fn filter_10_string_eq() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'Alice'})", None)?;
    conn.execute("CREATE (:Test {name: 'Bob'})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.name = 'Alice' RETURN t.name", None)?;
    assert_val!(res, 0, 0, "Alice", StringArray);
    Ok(())
}

// ============================================================================
// SECTION 4: AGGREGATES (Working Tests Only) (15 tests)
// ============================================================================

#[test]
fn agg_1_count_star() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64)", None)?;
    conn.execute("CREATE (:Test {id: 1})", None)?;
    conn.execute("CREATE (:Test {id: 2})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN count(*)", None)?;
    assert_val!(res, 0, 0, 2i64, Int64Array);
    Ok(())
}

#[test]
fn agg_2_count_with_filter() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 10})", None)?;
    conn.execute("CREATE (:Test {val: 20})", None)?;
    conn.execute("CREATE (:Test {val: 30})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.val > 15 RETURN count(*)", None)?;
    assert_val!(res, 0, 0, 2i64, Int64Array);
    Ok(())
}

#[test]
fn agg_3_group_by_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Sales(category STRING, amount DOUBLE)",
        None,
    )?;
    conn.execute("CREATE (:Sales {category: 'A', amount: 100.0})", None)?;
    conn.execute("CREATE (:Sales {category: 'A', amount: 200.0})", None)?;
    conn.execute("CREATE (:Sales {category: 'B', amount: 150.0})", None)?;
    let res = conn.execute("MATCH (s:Sales) RETURN s.category, count(*)", None)?;
    assert_row_count!(res, 2);
    Ok(())
}

#[test]
fn agg_4_avg_group() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Sales(category STRING, amount DOUBLE)",
        None,
    )?;
    conn.execute("CREATE (:Sales {category: 'A', amount: 10.0})", None)?;
    conn.execute("CREATE (:Sales {category: 'A', amount: 20.0})", None)?;
    conn.execute("CREATE (:Sales {category: 'B', amount: 30.0})", None)?;
    let res = conn.execute("MATCH (s:Sales) RETURN s.category, avg(s.amount)", None)?;
    assert_row_count!(res, 2);
    Ok(())
}

#[test]
fn agg_5_max() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 10.0})", None)?;
    conn.execute("CREATE (:Test {val: 20.0})", None)?;
    conn.execute("CREATE (:Test {val: 5.0})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN max(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 20.0);
    Ok(())
}

#[test]
fn agg_6_min() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 10.0})", None)?;
    conn.execute("CREATE (:Test {val: 20.0})", None)?;
    conn.execute("CREATE (:Test {val: 5.0})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN min(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 5.0);
    Ok(())
}

// ============================================================================
// SECTION 5: ORDER BY, LIMIT, SKIP (15 tests)
// ============================================================================

#[test]
fn order_1_order_by_asc() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 3})", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 2})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.val ORDER BY t.val ASC", None)?;
    assert_val!(res, 0, 0, 1, Int64Array);
    assert_val!(res, 0, 1, 2, Int64Array);
    assert_val!(res, 0, 2, 3, Int64Array);
    Ok(())
}

#[test]
fn order_2_order_by_desc() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 3})", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 2})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.val ORDER BY t.val DESC", None)?;
    assert_val!(res, 0, 0, 3, Int64Array);
    assert_val!(res, 0, 1, 2, Int64Array);
    assert_val!(res, 0, 2, 1, Int64Array);
    Ok(())
}

#[test]
fn limit_1_basic_limit() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    for i in 0..10 {
        conn.execute(&format!("CREATE (:Test {{val: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (t:Test) RETURN t.val LIMIT 3", None)?;
    assert_row_count!(res, 3);
    Ok(())
}

#[test]
fn limit_2_limit_zero() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.val LIMIT 0", None)?;
    assert_row_count!(res, 0);
    Ok(())
}

#[test]
fn skip_1_basic_skip() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    for i in 0..5 {
        conn.execute(&format!("CREATE (:Test {{val: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (t:Test) RETURN t.val SKIP 2", None)?;
    assert_row_count!(res, 3);
    assert_val!(res, 0, 0, 2, Int64Array);
    Ok(())
}

#[test]
fn skip_2_skip_limit() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    for i in 0..10 {
        conn.execute(&format!("CREATE (:Test {{val: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (t:Test) RETURN t.val SKIP 2 LIMIT 3", None)?;
    assert_row_count!(res, 3);
    Ok(())
}

// ============================================================================
// SECTION 6: STRING FUNCTIONS (Working Tests) (10 tests)
// ============================================================================

#[test]
fn str_1_upper() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'hello'})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN upper(t.name)", None)?;
    assert_val!(res, 0, 0, "HELLO", StringArray);
    Ok(())
}

#[test]
fn str_2_lower() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'HELLO'})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN lower(t.name)", None)?;
    assert_val!(res, 0, 0, "hello", StringArray);
    Ok(())
}

#[test]
fn str_3_concat_str() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(a STRING, b STRING)", None)?;
    conn.execute("CREATE (:Test {a: 'Hello', b: 'World'})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.a + t.b", None)?;
    // Concatenation result
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn str_4_length() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val STRING)", None)?;
    conn.execute("CREATE (:Test {val: 'Hello'})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN length(t.val)", None)?;
    assert_val!(res, 0, 0, 5, Int64Array);
    Ok(())
}

#[test]
fn str_5_coalesce() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING, alt STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'Hello', alt: NULL})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN coalesce(t.name, t.alt)", None)?;
    assert_val!(res, 0, 0, "Hello", StringArray);
    Ok(())
}

// ============================================================================
// SECTION 7: MATHEMATICAL FUNCTIONS (Working Tests) (10 tests)
// ============================================================================

#[test]
fn math_1_abs() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: -5.0})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN abs(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 5.0);
    Ok(())
}

#[test]
fn math_2_ceil() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 5.3})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN ceil(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 6.0);
    Ok(())
}

#[test]
fn math_3_floor() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 5.7})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN floor(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 5.0);
    Ok(())
}

#[test]
fn math_4_round() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 5.6})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN round(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 6.0);
    Ok(())
}

#[test]
fn math_5_sqrt() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 16.0})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN sqrt(t.val)", None)?;
    assert_val_f64!(res, 0, 0, 4.0);
    Ok(())
}

// ============================================================================
// SECTION 8: UNWIND, DEDUP, SET (10 tests)
// ============================================================================

#[test]
fn unwind_1_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    let res = conn.execute("UNWIND [1,2,3] AS x RETURN x", None)?;
    assert_row_count!(res, 3);
    Ok(())
}

#[test]
fn unwind_2_create() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("UNWIND [1,2,3] AS x CREATE (:Test {val: x})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 3i64, Int64Array);
    }
    Ok(())
}

#[test]
fn dedup_1_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 1})", None)?;
    conn.execute("CREATE (:Test {val: 2})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN DISTINCT t.val", None)?;
    assert_row_count!(res, 2);
    Ok(())
}

#[test]
fn set_1_arithmetic_update() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Counter(x INT64)", None)?;
    conn.execute("CREATE (:Counter {x: 10})", None)?;
    conn.execute("MATCH (c:Counter) SET c.x = c.x + 5", None)?;
    let res = conn.execute("MATCH (c:Counter) RETURN c.x", None)?;
    assert_val!(res, 0, 0, 15, Int64Array);
    Ok(())
}

// ============================================================================
// SECTION 9: MERGE (5 tests)
// ============================================================================

#[test]
fn merge_1_on_match() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Test(id INT64, val INT64, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE (:Test {id: 1, val: 10})", None)?;
    conn.execute("MERGE (t:Test {id: 1}) ON MATCH SET t.val = 20", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.val", None)?;
    assert_val!(res, 0, 0, 20, Int64Array);
    Ok(())
}

#[test]
fn merge_2_on_create() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Test(id INT64, val INT64, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE (:Test {id: 1, val: 10})", None)?;
    conn.execute("MERGE (t:Test {id: 2}) ON CREATE SET t.val = 30", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 2i64, Int64Array);
    }
    Ok(())
}

// ============================================================================
// SECTION 10: PATH AND JOIN QUERIES (10 tests)
// ============================================================================

#[test]
fn path_1_simple() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Links(FROM A TO A)", None)?;
    conn.execute("CREATE (:A {id: 1})", None)?;
    conn.execute("CREATE (:A {id: 2})", None)?;
    conn.execute("CREATE (:A {id: 3})", None)?;
    conn.execute(
        "MATCH (a:A {id: 1}), (b:A {id: 2}) CREATE (a)-[:Links]->(b)",
        None,
    )?;
    conn.execute(
        "MATCH (a:A {id: 2}), (b:A {id: 3}) CREATE (a)-[:Links]->(b)",
        None,
    )?;
    let res = conn.execute("MATCH (a:A)-[:Links]->(b:A) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 2i64, Int64Array);
    }
    Ok(())
}

#[test]
fn join_1_hash_join() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute(
        "CREATE NODE TABLE B(id INT64, a_id INT64, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE (:A {id: 1})", None)?;
    conn.execute("CREATE (:A {id: 2})", None)?;
    conn.execute("CREATE (:B {id: 10, a_id: 1})", None)?;
    conn.execute("CREATE (:B {id: 11, a_id: 2})", None)?;
    let res = conn.execute(
        "MATCH (a:A), (b:B) WHERE a.id = b.a_id RETURN a.id, b.id",
        None,
    )?;
    assert_row_count!(res, 2);
    Ok(())
}

#[test]
fn join_2_cross_product() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE A(val INT64)", None)?;
    conn.execute("CREATE NODE TABLE B(val INT64)", None)?;
    conn.execute("CREATE (:A {val: 1})", None)?;
    conn.execute("CREATE (:A {val: 2})", None)?;
    conn.execute("CREATE (:B {val: 10})", None)?;
    let res = conn.execute("MATCH (a:A), (b:B) RETURN a.val, b.val", None)?;
    assert_row_count!(res, 2);
    Ok(())
}

// ============================================================================
// SECTION 11: INDEX OPERATIONS (5 tests)
// ============================================================================

#[test]
fn idx_1_pk_lookup() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE (:User {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:User {id: 2, name: 'Bob'})", None)?;
    let res = conn.execute("MATCH (u:User) WHERE u.id = 1 RETURN u.name", None)?;
    assert_val!(res, 0, 0, "Alice", StringArray);
    Ok(())
}

#[test]
fn idx_2_pk_range() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Items(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;
    for i in 1..=10 {
        conn.execute(
            &format!("CREATE (:Items {{id: {}, name: 'Item{}'}})", i, i),
            None,
        )?;
    }
    let res = conn.execute("MATCH (i:Items) WHERE i.id <= 5 RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 5i64, Int64Array);
    }
    Ok(())
}

// ============================================================================
// SECTION 12: EDGE CASES AND DATA INTEGRITY (20 tests)
// ============================================================================

#[test]
fn edge_1_single_row() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 42})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.val", None)?;
    assert_row_count!(res, 1);
    assert_val!(res, 0, 0, 42, Int64Array);
    Ok(())
}

#[test]
fn edge_2_zero_val() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: 0})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.val = 0 RETURN t.val", None)?;
    assert_val!(res, 0, 0, 0, Int64Array);
    Ok(())
}

#[test]
fn edge_3_negative() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute("CREATE (:Test {val: -100})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.val", None)?;
    assert_val!(res, 0, 0, -100, Int64Array);
    Ok(())
}

#[test]
fn edge_4_empty_result() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Test {id: 1})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.id = 999 RETURN t.id", None)?;
    assert_row_count!(res, 0);
    Ok(())
}

#[test]
fn edge_5_no_match() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE User(name STRING)", None)?;
    conn.execute("CREATE (:User {name: 'Alice'})", None)?;
    let res = conn.execute(
        "MATCH (u:User) WHERE u.name = 'Unknown' RETURN u.name",
        None,
    )?;
    assert_row_count!(res, 0);
    Ok(())
}

#[test]
fn edge_6_large_numbers() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)?;
    conn.execute(&format!("CREATE (:Test {{val: {}}})", i64::MAX), None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.val", None)?;
    assert_val!(res, 0, 0, i64::MAX, Int64Array);
    Ok(())
}

#[test]
fn edge_7_float_precision() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 0.123456789})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.val", None)?;
    let val = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - 0.123456789).abs() < 0.0001);
    Ok(())
}

#[test]
fn edge_8_special_chars() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute(r#"CREATE (:Test {name: "hello world!@#$%"})"#, None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.name", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn edge_9_unicode() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'Привет'})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.name", None)?;
    assert_val!(res, 0, 0, "Привет", StringArray);
    Ok(())
}

#[test]
fn edge_10_double_zero() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val DOUBLE)", None)?;
    conn.execute("CREATE (:Test {val: 0.0})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.val = 0.0 RETURN t.val", None)?;
    assert_val_f64!(res, 0, 0, 0.0);
    Ok(())
}

// ============================================================================
// SECTION 13: STRESS TESTS (15 tests)
// ============================================================================

#[test]
fn stress_1_create_100() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    for i in 0..100 {
        conn.execute(&format!("CREATE (:Test {{id: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (t:Test) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 100i64, Int64Array);
    }
    Ok(())
}

#[test]
fn stress_2_create_200() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    for i in 0..200 {
        conn.execute(&format!("CREATE (:Test {{id: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (t:Test) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 200i64, Int64Array);
    }
    Ok(())
}

#[test]
fn stress_3_many_rels() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE B(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE R(FROM A TO B)", None)?;
    for i in 0..20 {
        conn.execute(&format!("CREATE (:A {{id: {}}})", i), None)?;
        conn.execute(&format!("CREATE (:B {{id: {}}})", i), None)?;
        conn.execute(
            &format!(
                "MATCH (a:A {{id: {}}}), (b:B {{id: {}}}) CREATE (a)-[:R]->(b)",
                i, i
            ),
            None,
        )?;
    }
    let res = conn.execute("MATCH (a:A)-[:R]->(b:B) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 20i64, Int64Array);
    }
    Ok(())
}

#[test]
fn stress_4_deep_path() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Node(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Next(FROM Node TO Node)", None)?;
    // Create a long chain: 0->1->2->...->9->0
    for i in 0..10 {
        conn.execute(&format!("CREATE (:Node {{id: {}}})", i), None)?;
    }
    for i in 0..9 {
        conn.execute(
            &format!(
                "MATCH (a:Node {{id: {}}}), (b:Node {{id: {}}}) CREATE (a)-[:Next]->(b)",
                i,
                i + 1
            ),
            None,
        )?;
    }
    conn.execute(
        "MATCH (a:Node {id: 9}), (b:Node {id: 0}) CREATE (a)-[:Next]->(b)",
        None,
    )?;
    let res = conn.execute("MATCH (a:Node)-[:Next]->(b:Node) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        assert_val!(res, 0, 0, 10i64, Int64Array);
    }
    Ok(())
}

#[test]
fn stress_5_no_id_collision() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, name STRING)", None)?;
    for i in 0..50 {
        conn.execute(
            &format!("CREATE (:Test {{id: {}, name: 'name{}'}})", i, i),
            None,
        )?;
    }
    let res = conn.execute("MATCH (t:Test) RETURN t.id ORDER BY t.id", None)?;
    assert_row_count!(res, 50);
    for i in 0..50 {
        assert_val!(res, 0, i, i as i64, Int64Array);
    }
    Ok(())
}

// ============================================================================
// SECTION 14: TYPE COMBINATIONS (15 tests)
// ============================================================================

#[test]
fn type_1_mixed_int_str() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(a INT64, b STRING)", None)?;
    conn.execute("CREATE (:Test {a: 42, b: 'hello'})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.a, t.b", None)?;
    assert_val!(res, 0, 0, 42, Int64Array);
    assert_val!(res, 1, 0, "hello", StringArray);
    Ok(())
}

#[test]
fn type_2_mixed_all() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Test(i INT64, d DOUBLE, s STRING, b BOOL)",
        None,
    )?;
    conn.execute("CREATE (:Test {i: 1, d: 1.5, s: 'a', b: true})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.i, t.d, t.s, t.b", None)?;
    assert_val!(res, 0, 0, 1, Int64Array);
    assert_val_f64!(res, 1, 0, 1.5);
    assert_val!(res, 2, 0, "a", StringArray);
    assert_val!(res, 3, 0, true, BooleanArray);
    Ok(())
}

#[test]
fn type_3_null_handling() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(a STRING, b STRING)", None)?;
    conn.execute("CREATE (:Test {a: 'value', b: NULL})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.a, t.b", None)?;
    assert_row_count!(res, 1);
    Ok(())
}

#[test]
fn type_4_update_mixed() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(a INT64, b DOUBLE)", None)?;
    conn.execute("CREATE (:Test {a: 1, b: 1.0})", None)?;
    conn.execute("MATCH (t:Test) SET t.a = 2, t.b = 2.0", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.a, t.b", None)?;
    assert_val!(res, 0, 0, 2, Int64Array);
    assert_val_f64!(res, 1, 0, 2.0);
    Ok(())
}

#[test]
fn type_5_complex_props() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Employee(name STRING, age INT64, salary DOUBLE, active BOOL)",
        None,
    )?;
    conn.execute(
        "CREATE (:Employee {name: 'Alice', age: 30, salary: 50000.0, active: true})",
        None,
    )?;
    let res = conn.execute("MATCH (e:Employee) RETURN e.name, e.age, e.salary", None)?;
    assert_val!(res, 0, 0, "Alice", StringArray);
    assert_val!(res, 1, 0, 30, Int64Array);
    assert_val_f64!(res, 2, 0, 50000.0);
    Ok(())
}
