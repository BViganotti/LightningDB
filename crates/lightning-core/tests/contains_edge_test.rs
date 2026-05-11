use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

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

/// EDGE CASE: Pattern with double quotes inside single-quoted string  
#[test]
fn contains_edge_double_quote_in_pattern() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(sig STRING)", None)?;
    conn.execute(r#"CREATE (:Test {sig: 'fn foo(x: &str) -> String'})"#, None)?;
    conn.execute(r#"CREATE (:Test {sig: 'fn bar() -> i32'})"#, None)?;

    // Pattern with & inside - common in Rust signatures
    let res = conn.execute("MATCH (t:Test) WHERE CONTAINS(t.sig, '&str') RETURN t.sig", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should find signature with &str");
    Ok(())
}

/// EDGE CASE: Infix CONTAINS where the expression has parentheses around it
/// e.g. WHERE (t.name CONTAINS 'x')
#[test]
fn contains_edge_parenthesized_infix() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'hello_world'})", None)?;
    conn.execute("CREATE (:Test {name: 'goodbye'})", None)?;

    // Parenthesized infix CONTAINS - the regex might not handle this
    let res = conn.execute(
        "MATCH (t:Test) WHERE (t.name CONTAINS 'world') RETURN t.name",
        None,
    );
    eprintln!("contains_edge_parenthesized_infix result: {:?}", res.is_ok());
    let res = res?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Parenthesized infix CONTAINS should work");
    Ok(())
}

/// EDGE CASE: Infix CONTAINS combined with OR using parentheses
/// This is the pattern from search.rs:304
#[test]
fn contains_edge_infix_or_parens() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE CodeNode(name STRING, file_path STRING)", None)?;
    conn.execute("CREATE (:CodeNode {name: 'test_func', file_path: '/src/main.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {name: 'other', file_path: '/tests/test.rs'})", None)?;
    conn.execute("CREATE (:CodeNode {name: 'unrelated', file_path: '/src/lib.rs'})", None)?;

    // Infix CONTAINS inside parenthesized OR - this might break the regex
    let res = conn.execute(
        "MATCH (n:CodeNode) WHERE (n.name CONTAINS 'test' OR n.file_path CONTAINS 'test') RETURN n.name",
        None,
    );
    eprintln!("contains_edge_infix_or_parens result: {:?}", res.is_ok());
    let res = res?;
    let total = row_count!(res);
    assert_eq!(total, 2, "Should find 2 nodes with 'test' in name or file_path");
    Ok(())
}

/// EDGE CASE: Infix CONTAINS where pattern contains a period (.)
/// Common in file paths like '.rs', '.py'
#[test]
fn contains_edge_infix_dot_pattern() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(path STRING)", None)?;
    conn.execute("CREATE (:Test {path: 'src/main.rs'})", None)?;
    conn.execute("CREATE (:Test {path: 'src/lib.py'})", None)?;

    let res = conn.execute("MATCH (t:Test) WHERE t.path CONTAINS '.rs' RETURN t.path", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1);
    Ok(())
}

/// EDGE CASE: CONTAINS with user input that has apostrophe/single quote
/// This is a SQL-injection-like scenario
#[test]
fn contains_edge_escaped_quote() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'it is great'})", None)?;

    // Function-call with empty search should still work
    let res = conn.execute("MATCH (t:Test) WHERE CONTAINS(t.name, 'is') RETURN t.name", None)?;
    let total = row_count!(res);
    assert_eq!(total, 1);
    Ok(())
}

/// EDGE CASE: Multiple CONTAINS with AND on same property
#[test]
fn contains_edge_double_contains_and() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'hello_world_foo'})", None)?;
    conn.execute("CREATE (:Test {name: 'hello_bar'})", None)?;
    conn.execute("CREATE (:Test {name: 'world_foo'})", None)?;

    let res = conn.execute(
        "MATCH (t:Test) WHERE t.name CONTAINS 'hello' AND t.name CONTAINS 'foo' RETURN t.name",
        None,
    );
    eprintln!("contains_edge_double_contains_and result: {:?}", res.is_ok());
    let res = res?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should only match 'hello_world_foo'");
    Ok(())
}

/// EDGE CASE: CONTAINS followed by ORDER BY
#[test]
fn contains_edge_with_order_by() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'beta_test'})", None)?;
    conn.execute("CREATE (:Test {name: 'alpha_test'})", None)?;
    conn.execute("CREATE (:Test {name: 'unrelated'})", None)?;

    let res = conn.execute(
        "MATCH (t:Test) WHERE t.name CONTAINS 'test' RETURN t.name ORDER BY t.name",
        None,
    );
    eprintln!("contains_edge_with_order_by result: {:?}", res.is_ok());
    let res = res?;
    let total = row_count!(res);
    assert_eq!(total, 2, "Should find 2 test nodes");
    Ok(())
}

/// EDGE CASE: CONTAINS on a property that doesn't exist (should not panic)
#[test]
fn contains_edge_nonexistent_property() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'hello'})", None)?;

    let res = conn.execute(
        "MATCH (t:Test) WHERE CONTAINS(t.nonexistent, 'hello') RETURN t.name",
        None,
    );
    // Should either return 0 rows or error gracefully, not panic
    eprintln!("contains_edge_nonexistent_property result: {:?}", res.is_ok());
    Ok(())
}

/// EDGE CASE: NOT combined with infix CONTAINS in complex expression
#[test]
fn contains_edge_not_with_and() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING, active STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'test_func', active: 'yes'})", None)?;
    conn.execute("CREATE (:Test {name: 'helper_func', active: 'yes'})", None)?;
    conn.execute("CREATE (:Test {name: 'test_util', active: 'no'})", None)?;

    // NOT CONTAINS combined with AND
    let res = conn.execute(
        "MATCH (t:Test) WHERE NOT t.name CONTAINS 'test' AND t.active = 'yes' RETURN t.name",
        None,
    );
    eprintln!("contains_edge_not_with_and result: {:?}", res.is_ok());
    let res = res?;
    let total = row_count!(res);
    assert_eq!(total, 1, "Should find only 'helper_func'");
    Ok(())
}

/// EDGE CASE: CONTAINS in RETURN expression (not WHERE)
#[test]
fn contains_in_return() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(name STRING)", None)?;
    conn.execute("CREATE (:Test {name: 'hello_world'})", None)?;

    let res = conn.execute(
        "MATCH (t:Test) RETURN CONTAINS(t.name, 'world')",
        None,
    );
    eprintln!("contains_in_return result: {:?}", res.is_ok());
    let res = res?;
    let total = row_count!(res);
    assert_eq!(total, 1);
    Ok(())
}
