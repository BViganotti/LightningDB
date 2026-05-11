use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;
use arrow::array::{Array, StringArray, BooleanArray, Int64Array};

type TestResult = lightning_core::Result<()>;

fn setup_db() -> lightning_core::Result<(tempfile::TempDir, Arc<Database>)> {
    let dir = tempdir()?;
    let db = Database::new(dir.path(), SystemConfig::default())?;
    Ok((dir, db))
}

macro_rules! row_count {
    ($res:expr) => {{
        let total: usize = $res.batches.iter().map(|b| b.num_rows()).sum();
        total
    }};
}

/// Test 1: Basic infix CONTAINS syntax (Cypher standard)
#[test]
fn contains_infix_basic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE CodeNode(name STRING, file_path STRING)", None)?;
    conn.execute("CREATE (:CodeNode {name: 'compression_utils', file_path: '/src/compression.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {name: 'parser_module', file_path: '/src/parser.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {name: 'decompressor', file_path: '/src/decompress.rs'})", None)?;

    let res = conn.execute("MATCH (n:CodeNode) WHERE n.name CONTAINS 'compress' RETURN n.name", None)?;
    let total = row_count!(res);
    assert_eq!(total, 2, "Should find 'compression_utils' and 'decompressor'");
    Ok(())
}

/// Test 2: Function-call CONTAINS syntax (what search.rs uses)
#[test]
fn contains_function_call_syntax() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE CodeNode(name STRING, file_path STRING)", None)?;
    conn.execute("CREATE (:CodeNode {name: 'compression_utils', file_path: '/src/compression.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {name: 'parser_module', file_path: '/src/parser.rs'})", None)?;

    let res = conn.execute("MATCH (n:CodeNode) WHERE CONTAINS(n.name, 'compress') RETURN n.name", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should find 'compression_utils'");
    Ok(())
}

/// Test 3: CONTAINS with AND (used in search.rs find_test_nodes_for_results)
#[test]
fn contains_with_and() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE CodeNode(id STRING, name STRING, file_path STRING)", None)?;
    conn.execute("CREATE (:CodeNode {id: '1', name: 'test_parser', file_path: '/tests/parser_test.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {id: '2', name: 'main_parser', file_path: '/src/parser.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {id: '3', name: 'test_helper', file_path: '/tests/helper.rs'})", None)?;

    let res = conn.execute(
        "MATCH (n:CodeNode) WHERE CONTAINS(n.name, 'test') AND CONTAINS(n.file_path, 'test') RETURN n.name",
        None,
    )?;
    let total = row_count!(res);
    assert_eq!(total, 2, "Should find 'test_parser' and 'test_helper'");
    Ok(())
}

/// Test 4: CONTAINS with OR (used in search.rs)
#[test]
fn contains_with_or() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE CodeNode(name STRING, file_path STRING)", None)?;
    conn.execute("CREATE (:CodeNode {name: 'test_parser', file_path: '/tests/parser_test.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {name: 'main_parser', file_path: '/src/parser.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {name: 'helper', file_path: '/tests/helper.rs'})", None)?;

    let res = conn.execute(
        "MATCH (n:CodeNode) WHERE CONTAINS(n.name, 'test') OR CONTAINS(n.file_path, 'test') RETURN n.name",
        None,
    )?;
    let total = row_count!(res);
    assert_eq!(total, 2, "Should find 'test_parser' and 'helper'");
    Ok(())
}

/// Test 5: CONTAINS with special characters (@ symbol, used for email search)
#[test]
fn contains_special_chars() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(email STRING)", None)?;
    conn.execute("CREATE (:Test {email: 'user@example.com'})", None)?;
    conn.execute("CREATE (:Test {email: 'invalid_email'})", None)?;

    let res = conn.execute("MATCH (t:Test) WHERE t.email CONTAINS '@' RETURN t.email", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should find email with @");
    Ok(())
}

/// Test 6: CONTAINS case sensitivity
#[test]
fn contains_case_sensitive() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'HelloWorld'})", None)?;
    conn.execute("CREATE (:Test {name: 'helloworld'})", None)?;

    let res = conn.execute("MATCH (t:Test) WHERE t.name CONTAINS 'Hello' RETURN t.name", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1, "CONTAINS should be case-sensitive");
    Ok(())
}

/// Test 7: CONTAINS with empty pattern
#[test]
fn contains_empty_pattern() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'hello'})", None)?;
    conn.execute("CREATE (:Test {name: 'world'})", None)?;

    // CONTAINS with empty string via function call syntax
    let res = conn.execute("MATCH (t:Test) WHERE CONTAINS(t.name, '') RETURN t.name", None)?;
    let total = row_count!(res);
    // Empty string contains should match all (Rust's str::contains("") returns true)
    assert_eq!(total, 2, "CONTAINS('', '') should match everything");
    Ok(())
}

/// Test 8: Multiple CONTAINS in complex WHERE (real production pattern from search.rs:304)
#[test]
fn contains_complex_where() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE CodeNode(id STRING, name STRING, file_path STRING)", None)?;
    conn.execute("CREATE (:CodeNode {id: '1', name: 'test_compress', file_path: '/tests/compress.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {id: '2', name: 'main_module', file_path: '/src/main.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {id: '3', name: 'utils', file_path: '/tests/utils_test.rs'})", None)?;

    // Pattern from search.rs - mixed function call CONTAINS with parenthesized OR
    let res = conn.execute(
        "MATCH (n:CodeNode) WHERE (CONTAINS(n.name, 'test') OR CONTAINS(n.file_path, 'test')) RETURN n.name",
        None,
    )?;
    let total = row_count!(res);
    assert_eq!(total, 2, "Should find nodes with 'test' in name or file_path");
    Ok(())
}

/// Test 9: NOT CONTAINS (negation)
#[test]
fn contains_not() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'hello_world'})", None)?;
    conn.execute("CREATE (:Test {name: 'goodbye'})", None)?;
    conn.execute("CREATE (:Test {name: 'worldwide'})", None)?;

    let res = conn.execute("MATCH (t:Test) WHERE NOT t.name CONTAINS 'world' RETURN t.name", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should find only 'goodbye'");
    Ok(())
}

/// Test 10: CONTAINS with LIMIT (production pattern from search.rs:130-133 and verification.rs:55)
#[test]
fn contains_with_limit() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE CodeNode(name STRING)", None)?;
    for i in 0..20 {
        conn.execute(&format!("CREATE (:CodeNode {{name: 'test_func_{}'}})", i), None)?;
    }
    conn.execute("CREATE (:CodeNode {name: 'other_func'})", None)?;

    let res = conn.execute(
        "MATCH (n:CodeNode) WHERE CONTAINS(n.name, 'test') RETURN n.name LIMIT 5",
        None,
    )?;
    let total = row_count!(res);
    assert_eq!(total, 5, "LIMIT should cap results at 5");
    Ok(())
}

/// Test 11: Infix CONTAINS with underscore property (e.g. file_path)
#[test]
fn contains_infix_underscore_property() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE CodeNode(name STRING, file_path STRING)", None)?;
    conn.execute("CREATE (:CodeNode {name: 'foo', file_path: '/src/test_module.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {name: 'bar', file_path: '/src/main.rs'})", None)?;

    let res = conn.execute("MATCH (n:CodeNode) WHERE n.file_path CONTAINS 'test' RETURN n.name", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should match file_path containing 'test'");
    Ok(())
}

/// Test 12: CONTAINS with pattern containing spaces
#[test]
fn contains_pattern_with_spaces() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(desc STRING)", None)?;
    conn.execute("CREATE (:Test {desc: 'hello beautiful world'})", None)?;
    conn.execute("CREATE (:Test {desc: 'goodbye'})", None)?;

    let res = conn.execute("MATCH (t:Test) WHERE t.desc CONTAINS 'beautiful world' RETURN t.desc", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should match substring with spaces");
    Ok(())
}

/// Test 13: CONTAINS where no match exists
#[test]
fn contains_no_match() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'hello'})", None)?;

    let res = conn.execute("MATCH (t:Test) WHERE t.name CONTAINS 'xyz' RETURN t.name", None)?;
    let total = row_count!(res);
    assert_eq!(total, 0, "No matches expected");
    Ok(())
}

/// Test 14: CONTAINS with NULL values
#[test]
fn contains_with_nulls() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING, desc STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'hello', desc: NULL})", None)?;
    conn.execute("CREATE (:Test {name: 'world', desc: 'some description'})", None)?;

    // CONTAINS on NULL should not crash, should return no match
    let res = conn.execute("MATCH (t:Test) WHERE CONTAINS(t.desc, 'some') RETURN t.name", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should only match non-null desc");
    Ok(())
}

/// Test 15: CONTAINS with variable pattern (not literal) - likely broken
#[test]
fn contains_with_variable_pattern() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING, pattern STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'hello_world', pattern: 'world'})", None)?;
    conn.execute("CREATE (:Test {name: 'goodbye', pattern: 'bye'})", None)?;

    // This uses function-call syntax with a variable as the pattern
    let res = conn.execute("MATCH (t:Test) WHERE CONTAINS(t.name, t.pattern) RETURN t.name", None)?;
    let total = row_count!(res);
    assert_eq!(total, 2, "Should match when pattern is a property reference");
    Ok(())
}

/// Test 16: CONTAINS infix with double-quoted pattern - edge case  
#[test]
fn contains_pattern_with_dots() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(path STRING)", None)?;
    conn.execute("CREATE (:Test {path: '/src/main.rs'})", None)?;
    conn.execute("CREATE (:Test {path: '/src/librs'})", None)?;

    let res = conn.execute("MATCH (t:Test) WHERE t.path CONTAINS '.rs' RETURN t.path", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should find path with '.rs'");
    Ok(())
}

/// Test 17: CONTAINS infix with forward slash (common in file paths)
#[test]
fn contains_pattern_with_slash() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(path STRING)", None)?;
    conn.execute("CREATE (:Test {path: '/src/utils/helper.rs'})", None)?;
    conn.execute("CREATE (:Test {path: '/tests/main.rs'})", None)?;

    let res = conn.execute("MATCH (t:Test) WHERE t.path CONTAINS '/utils/' RETURN t.path", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should find path with '/utils/'");
    Ok(())
}

/// Test 18: CONTAINS combined with equality check (common production pattern)
#[test]
fn contains_with_equality() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE CodeNode(name STRING, node_type STRING)", None)?;
    conn.execute("CREATE (:CodeNode {name: 'test_parser', node_type: 'Function'})", None)?;
    conn.execute("CREATE (:CodeNode {name: 'TestStruct', node_type: 'Struct'})", None)?;
    conn.execute("CREATE (:CodeNode {name: 'main', node_type: 'Function'})", None)?;

    let res = conn.execute(
        "MATCH (n:CodeNode) WHERE n.node_type = 'Function' AND CONTAINS(n.name, 'test') RETURN n.name",
        None,
    )?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should find test_parser (Function with 'test' in name)");
    Ok(())
}
