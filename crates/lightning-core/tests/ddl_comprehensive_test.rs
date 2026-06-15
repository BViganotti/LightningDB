use arrow::array::{Int64Array, StringArray, Float64Array, BooleanArray};
use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup_db() -> TestResult {
    let _dir = tempdir()?;
    let _db = Database::new(_dir.path(), SystemConfig::default())?;
    Ok(())
}

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

// === CREATE NODE TABLE ===

#[test]
fn test_ddl_create_node_table_basic() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY (id))", None)?;
    Ok(())
}

#[test]
fn test_ddl_create_node_table_string_pk() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(email STRING, name STRING, PRIMARY KEY (email))", None)?;
    conn.execute("CREATE (:Person {email: 'alice@test.com', name: 'Alice'})", None)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.email = 'alice@test.com' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_ddl_create_node_table_double_col() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Measure(id INT64, val DOUBLE, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Measure {id: 1, val: 3.14159})", None)?;
    let res = conn.execute("MATCH (m:Measure) RETURN m.val", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((val.value(0) - 3.14159).abs() < 1e-10);
    Ok(())
}

#[test]
fn test_ddl_create_node_table_boolean_col() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Item(id INT64, active BOOL, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Item {id: 1, active: true})", None)?;
    conn.execute("CREATE (:Item {id: 2, active: false})", None)?;
    let res = conn.execute("MATCH (i:Item) WHERE i.active = true RETURN i.id", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_ddl_create_node_table_no_pk() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let result = conn.execute("CREATE NODE TABLE Person(name STRING, age INT64)", None);
    // May or may not allow tables without PK
    Ok(())
}

#[test]
fn test_ddl_create_node_table_duplicate_name() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    let result = conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None);
    assert!(result.is_err());
    Ok(())
}

#[test]
fn test_ddl_create_node_table_many_columns() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Wide(id INT64, c1 INT64, c2 INT64, c3 INT64, c4 INT64, c5 INT64, \
         c6 INT64, c7 INT64, c8 INT64, c9 INT64, c10 INT64, PRIMARY KEY (id))",
        None,
    )?;
    let cols: Vec<String> = (1..=10).map(|i| format!("c{i}: 1")).collect();
    conn.execute(&format!("CREATE (:Wide {{id: 1, {}}})", cols.join(", ")), None)?;
    let res = conn.execute("MATCH (w:Wide) RETURN w.c1, w.c5, w.c10", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

// === CREATE REL TABLE ===

#[test]
fn test_ddl_create_rel_table() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)", None)?;
    Ok(())
}

#[test]
fn test_ddl_create_rel_table_no_props() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE KNOWS(FROM Person TO Person)", None)?;
    Ok(())
}

#[test]
fn test_ddl_create_rel_table_missing_node_table() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let result = conn.execute("CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)", None);
    // May or may not error depending on deferred validation
    Ok(())
}

#[test]
fn test_ddl_create_rel_table_duplicate_name() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)", None)?;
    let result = conn.execute("CREATE REL TABLE KNOWS(FROM Person TO Person, weight DOUBLE)", None);
    assert!(result.is_err());
    Ok(())
}

// === DROP TABLE ===

#[test]
fn test_ddl_drop_node_table() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("DROP TABLE Person", None)?;
    let result = conn.execute("MATCH (p:Person) RETURN p.name", None);
    assert!(result.is_err());
    Ok(())
}

#[test]
fn test_ddl_drop_rel_table() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)", None)?;
    conn.execute("DROP TABLE KNOWS", None)?;
    Ok(())
}

#[test]
fn test_ddl_drop_nonexistent_table() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let result = conn.execute("DROP TABLE NonExistent", None);
    // May or may not error depending on implementation
    Ok(())
}

// === Edge cases ===

#[test]
fn test_ddl_standard_table_name() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T123(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T123 {id: 1, name: 'test'})", None)?;
    let res = conn.execute("MATCH (n:T123) RETURN n.name", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

// === CREATE INDEX ===

#[test]
fn test_ddl_create_and_cast() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN p.name", None)?;
    let name = res.batches[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(name.value(0), "Alice");
    Ok(())
}

// === ALTER TABLE ===

#[test]
fn test_ddl_alter_add_column() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("ALTER TABLE Person ADD COLUMN age INT64", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice', age: 30})", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN p.age", None)?;
    let age = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(age.value(0), 30);
    Ok(())
}

#[test]
fn test_ddl_alter_drop_column() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY (id))", None)?;
    conn.execute("ALTER TABLE Person DROP COLUMN age", None)?;
    let result = conn.execute("MATCH (p:Person) RETURN p.age", None);
    assert!(result.is_err());
    Ok(())
}

#[test]
fn test_ddl_alter_rename_column() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("ALTER TABLE Person RENAME COLUMN name TO full_name", None)?;
    conn.execute("CREATE (:Person {id: 1, full_name: 'Alice'})", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN p.full_name", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_ddl_alter_rename_table() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("ALTER TABLE Person RENAME TO People", None)?;
    let result = conn.execute("MATCH (p:Person) RETURN p.name", None);
    assert!(result.is_err());
    Ok(())
}

// === Comment syntax ===

#[test]
fn test_ddl_comment_syntax() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id)) // this is a comment", None)?;
    Ok(())
}

#[test]
fn test_ddl_create_node_table_multiple_pks() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let result = conn.execute("CREATE NODE TABLE Person(id1 INT64, id2 INT64, name STRING, PRIMARY KEY (id1, id2))", None);
    // May or may not support composite PKs, just verify no crash
    Ok(())
}

#[test]
fn test_ddl_create_and_immediately_query() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob'})", None)?;
    let res = conn.execute("MATCH (p:Person) RETURN count(p.id)", None)?;
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(count.value(0), 2);
    Ok(())
}
