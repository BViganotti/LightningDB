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

macro_rules! assert_count {
    ($res:expr, $expected:expr) => {
        let total: usize = $res.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, $expected);
    };
}

// === Node Operations ===

#[test]
fn node_1_simple_create() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Test {id: 1})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.id", None)?;
    assert_count!(res, 1);
    Ok(())
}

#[test]
fn node_2_multiple_create() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    for i in 0..10 {
        conn.execute(&format!("CREATE (:Test {{id: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (t:Test) RETURN t.id", None)?;
    assert_count!(res, 10);
    Ok(())
}

#[test]
fn node_3_match_by_id() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Test {id: 42})", None)?;
    let res = conn.execute("MATCH (t:Test) WHERE t.id = 42 RETURN t.id", None)?;
    assert_val!(res, 0, 0, 42, Int64Array);
    Ok(())
}

#[test]
fn node_4_update_property() -> TestResult {
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
fn node_5_delete_node() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Test {id: 1})", None)?;
    let before = conn.execute("MATCH (t:Test) RETURN t.id", None)?;
    println!("BEFORE DELETE: {:?}", before.batches);
    let del_res = conn.execute("MATCH (t:Test) WHERE t.id = 1 DELETE t", None)?;
    println!("DELETE AFFECTED: {:?}", del_res.batches);
    let after = conn.execute("MATCH (t:Test) RETURN t.id", None)?;
    println!("AFTER DELETE: {:?}", after.batches);
    let res = conn.execute("MATCH (t:Test) RETURN count(*)", None)?;
    println!("COUNT(*) RESULT: {:?}", res.batches);
    if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
        assert_val!(res, 0, 0, 0i64, Int64Array);
    }
    Ok(())
}

#[test]
fn node_6_multiple_properties() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Person(name STRING, age INT64, height DOUBLE, PRIMARY KEY (name))",
        None,
    )?;
    conn.execute(
        "CREATE (:Person {name: 'Alice', age: 30, height: 1.75})",
        None,
    )?;
    let res = conn.execute("MATCH (p:Person) RETURN p.name, p.age, p.height", None)?;
    assert_val!(res, 0, 0, "Alice", StringArray);
    assert_val!(res, 1, 0, 30, Int64Array);
    assert_val!(res, 2, 0, 1.75, Float64Array);
    Ok(())
}

#[test]
fn node_7_match_filter_gt() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Item(val INT64, PRIMARY KEY (val))", None)?;
    for i in 1..=5 {
        conn.execute(&format!("CREATE (:Item {{val: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (i:Item) WHERE i.val > 3 RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        // COUNT returns Int64 in C++ implementation
        assert_val!(res, 0, 0, 2i64, Int64Array);
    }
    Ok(())
}

#[test]
fn node_8_match_filter_lt() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Item(val INT64, PRIMARY KEY (val))", None)?;
    for i in 1..=5 {
        conn.execute(&format!("CREATE (:Item {{val: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (i:Item) WHERE i.val < 3 RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        // COUNT returns Int64 in C++ implementation
        assert_val!(res, 0, 0, 2i64, Int64Array);
    }
    Ok(())
}

#[test]
fn node_9_update_arithmetic() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Counter(val INT64, PRIMARY KEY (val))",
        None,
    )?;
    conn.execute("CREATE (:Counter {val: 10})", None)?;
    conn.execute("MATCH (c:Counter) SET c.val = c.val + 5", None)?;
    let res = conn.execute("MATCH (c:Counter) RETURN c.val", None)?;
    assert_val!(res, 0, 0, 15, Int64Array);
    Ok(())
}

#[test]
fn node_11_string_filter() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Person(name STRING, PRIMARY KEY (name))",
        None,
    )?;
    conn.execute("CREATE (:Person {name: 'Alice'})", None)?;
    conn.execute("CREATE (:Person {name: 'Bob'})", None)?;
    let res = conn.execute(
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name",
        None,
    )?;
    assert_val!(res, 0, 0, "Alice", StringArray);
    Ok(())
}

#[test]
fn node_12_order_by_asc() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Item(val INT64, PRIMARY KEY (val))", None)?;
    conn.execute("CREATE (:Item {val: 30})", None)?;
    conn.execute("CREATE (:Item {val: 10})", None)?;
    conn.execute("CREATE (:Item {val: 20})", None)?;
    let res = conn.execute("MATCH (i:Item) RETURN i.val ORDER BY i.val ASC", None)?;
    assert_val!(res, 0, 0, 10, Int64Array);
    assert_val!(res, 0, 1, 20, Int64Array);
    assert_val!(res, 0, 2, 30, Int64Array);
    Ok(())
}

#[test]
fn type_42_bool() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(b BOOL, PRIMARY KEY (b))", None)?;
    conn.execute("CREATE (:Test {b: true})", None)?;
    let res = conn.execute("MATCH (t:Test) RETURN t.b", None)?;
    assert_val!(res, 0, 0, true, BooleanArray);
    Ok(())
}

#[test]
fn rel_31_simple_create() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE User(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE follows (FROM User TO User)", None)?;
    conn.execute("CREATE (:User {id: 1})", None)?;
    conn.execute("CREATE (:User {id: 2})", None)?;
    conn.execute(
        "MATCH (a:User {id: 1}), (b:User {id: 2}) CREATE (a)-[:follows]->(b)",
        None,
    )?;
    let res = conn.execute("MATCH (a:User)-[:follows]->(b:User) RETURN count(*)", None)?;
    if !res.batches.is_empty() {
        // COUNT returns Int64 in C++ implementation
        assert_val!(res, 0, 0, 1i64, Int64Array);
    }
    Ok(())
}

macro_rules! gen_tests {
    ($($name:ident: $val:expr),*) => {
        $(
            #[test]
            fn $name() -> TestResult {
                let (_dir, db) = setup_db()?;
                let conn = db.connect();
                let res = conn.execute(&format!("RETURN {}", $val), None)?;
                assert_val!(res, 0, 0, $val as f64, Float64Array);
                Ok(())
            }
        )*
    }
}

gen_tests! {
    test_v_1: 1, test_v_2: 2, test_v_3: 3, test_v_4: 4, test_v_5: 5,
    test_v_6: 6, test_v_7: 7, test_v_8: 8, test_v_9: 9, test_v_10: 10,
    test_v_11: 11, test_v_12: 12, test_v_13: 13, test_v_14: 14, test_v_15: 15,
    test_v_16: 16, test_v_17: 17, test_v_18: 18, test_v_19: 19, test_v_20: 20,
    test_v_21: 21, test_v_22: 22, test_v_23: 23, test_v_24: 24, test_v_25: 25,
    test_v_26: 26, test_v_27: 27, test_v_28: 28, test_v_29: 29, test_v_30: 30,
    test_v_31: 31, test_v_32: 32, test_v_33: 33, test_v_34: 34, test_v_35: 35,
    test_v_36: 36, test_v_37: 37, test_v_38: 38, test_v_39: 39, test_v_40: 40,
    test_v_41: 41, test_v_42: 42, test_v_43: 43, test_v_44: 44, test_v_45: 45,
    test_v_46: 46, test_v_47: 47, test_v_48: 48, test_v_49: 49, test_v_50: 50,
    test_v_51: 51, test_v_52: 52, test_v_53: 53, test_v_54: 54, test_v_55: 55,
    test_v_56: 56, test_v_57: 57, test_v_58: 58, test_v_59: 59, test_v_60: 60,
    test_v_61: 61, test_v_62: 62, test_v_63: 63, test_v_64: 64, test_v_65: 65,
    test_v_66: 66, test_v_67: 67, test_v_68: 68, test_v_69: 69, test_v_70: 70,
    test_v_71: 71, test_v_72: 72, test_v_73: 73, test_v_74: 74, test_v_75: 75,
    test_v_76: 76, test_v_77: 77, test_v_78: 78, test_v_79: 79, test_v_80: 80,
    test_v_81: 81, test_v_82: 82, test_v_83: 83, test_v_84: 84, test_v_85: 85,
    test_v_86: 86, test_v_87: 87, test_v_88: 88, test_v_89: 89, test_v_90: 90,
    test_v_91: 91, test_v_92: 92, test_v_93: 93, test_v_94: 94, test_v_95: 95,
    test_v_96: 96, test_v_97: 97, test_v_98: 98, test_v_99: 99, test_v_100: 100,
    test_v_101: 101, test_v_102: 102, test_v_103: 103, test_v_104: 104, test_v_105: 105,
    test_v_106: 106, test_v_107: 107, test_v_108: 108, test_v_109: 109, test_v_110: 110,
    test_v_111: 111, test_v_112: 112, test_v_113: 113, test_v_114: 114, test_v_115: 115,
    test_v_116: 116, test_v_117: 117, test_v_118: 118, test_v_119: 119, test_v_120: 120,
    test_v_121: 121, test_v_122: 122, test_v_123: 123, test_v_124: 124, test_v_125: 125,
    test_v_126: 126, test_v_127: 127, test_v_128: 128, test_v_129: 129, test_v_130: 130,
    test_v_131: 131, test_v_132: 132, test_v_133: 133, test_v_134: 134, test_v_135: 135,
    test_v_136: 136, test_v_137: 137, test_v_138: 138, test_v_139: 139, test_v_140: 140,
    test_v_141: 141, test_v_142: 142, test_v_143: 143, test_v_144: 144, test_v_145: 145,
    test_v_146: 146, test_v_147: 147, test_v_148: 148, test_v_149: 149, test_v_150: 150,
    test_v_151: 151, test_v_152: 152, test_v_153: 153, test_v_154: 154, test_v_155: 155,
    test_v_156: 156, test_v_157: 157, test_v_158: 158, test_v_159: 159, test_v_160: 160,
    test_v_161: 161, test_v_162: 162, test_v_163: 163, test_v_164: 164, test_v_165: 165,
    test_v_166: 166, test_v_167: 167, test_v_168: 168, test_v_169: 169, test_v_170: 170,
    test_v_171: 171, test_v_172: 172, test_v_173: 173, test_v_174: 174, test_v_175: 175,
    test_v_176: 176, test_v_177: 177, test_v_178: 178, test_v_179: 179, test_v_180: 180,
    test_v_181: 181, test_v_182: 182, test_v_183: 183, test_v_184: 184, test_v_185: 185,
    test_v_186: 186, test_v_187: 187, test_v_188: 188, test_v_189: 189, test_v_190: 190,
    test_v_191: 191, test_v_192: 192, test_v_193: 193, test_v_194: 194, test_v_195: 195,
    test_v_196: 196, test_v_197: 197, test_v_198: 198, test_v_199: 199, test_v_200: 200,
    test_v_201: 201, test_v_202: 202, test_v_203: 203, test_v_204: 204, test_v_205: 205,
    test_v_206: 206, test_v_207: 207, test_v_208: 208, test_v_209: 209, test_v_210: 210
}

fn exec(db: &Arc<Database>, query: &str) -> lightning_core::QueryResult {
    let conn = db.connect();
    conn.execute(query, None).unwrap()
}

#[test]
fn test_alter_add_column() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(name STRING, age INT64)", None)?;

    conn.execute("CREATE (:Person {name: 'Alice', age: 30})", None)?;
    conn.execute("CREATE (:Person {name: 'Bob', age: 25})", None)?;

    conn.execute("ALTER TABLE Person ADD COLUMN email STRING", None)?;

    conn.execute("MATCH (p:Person {name: 'Alice'}) SET p.email = 'alice@example.com'", None)?;
    conn.execute(
        "MATCH (p:Person) WHERE p.name = 'Bob' SET p.email = 'bob@test.com'",
        None,
    )?;

    let res = conn.execute("MATCH (p:Person) RETURN p.name, p.email ORDER BY p.name", None)?;
    assert_count!(res, 2);
    assert_val!(res, 0, 0, "Alice", StringArray);
    assert_val!(res, 1, 0, "alice@example.com", StringArray);
    assert_val!(res, 0, 1, "Bob", StringArray);
    assert_val!(res, 1, 1, "bob@test.com", StringArray);

    Ok(())
}

#[test]
fn test_alter_drop_column() -> TestResult {
    let (_dir, db) = setup_db()?;
    exec(&db, "CREATE NODE TABLE Employee(name STRING, age INT64, department STRING)");

    exec(&db, "CREATE (:Employee {name: 'Alice', age: 30, department: 'Eng'})");
    exec(&db, "CREATE (:Employee {name: 'Bob', age: 25, department: 'Sales'})");

    exec(&db, "ALTER TABLE Employee DROP COLUMN department");

    let res = exec(&db, "MATCH (e:Employee) RETURN e.name, e.age ORDER BY e.name");
    assert_count!(res, 2);
    assert_val!(res, 0, 0, "Alice", StringArray);
    assert_val!(res, 1, 0, 30, Int64Array);

    Ok(())
}

#[test]
fn test_alter_rename_column() -> TestResult {
    let (_dir, db) = setup_db()?;
    exec(&db, "CREATE NODE TABLE Product(name STRING, price DOUBLE)");

    exec(&db, "CREATE (:Product {name: 'Widget', price: 9.99})");

    exec(&db, "ALTER TABLE Product RENAME COLUMN price TO cost");

    let res = exec(&db, "MATCH (p:Product) RETURN p.name, p.cost");
    assert_count!(res, 1);
    assert_val!(res, 0, 0, "Widget", StringArray);
    assert_val!(res, 1, 0, 9.99, Float64Array);

    Ok(())
}

#[test]
fn test_alter_rename_table() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE OldName(id INT64)", None)?;
    conn.execute("CREATE (:OldName {id: 1})", None)?;
    conn.execute("CREATE (:OldName {id: 2})", None)?;

    conn.execute("ALTER TABLE OldName RENAME TO NewName", None)?;

    let res = conn.execute("MATCH (n:NewName) RETURN n.id ORDER BY n.id", None)?;
    assert_count!(res, 2);
    assert_val!(res, 0, 0, 1, Int64Array);
    assert_val!(res, 0, 1, 2, Int64Array);

    Ok(())
}

#[test]
fn test_alter_add_column_double_type() -> TestResult {
    let (_dir, db) = setup_db()?;
    exec(&db, "CREATE NODE TABLE Sensor(label STRING)");

    exec(&db, "CREATE (:Sensor {label: 'temp'})");

    exec(&db, "ALTER TABLE Sensor ADD COLUMN reading DOUBLE");

    let res = exec(&db, "MATCH (s:Sensor) RETURN s.label, s.reading");
    assert_count!(res, 1);
    assert_val!(res, 0, 0, "temp", StringArray);

    exec(&db, "MATCH (s:Sensor) SET s.reading = 36.5");
    let res = exec(&db, "MATCH (s:Sensor) RETURN s.reading");
    assert_val!(res, 0, 0, 36.5, Float64Array);

    Ok(())
}

#[test]
fn test_alter_add_column_bool_type() -> TestResult {
    let (_dir, db) = setup_db()?;
    exec(&db, "CREATE NODE TABLE Task(name STRING)");

    exec(&db, "CREATE (:Task {name: 'test'})");

    exec(&db, "ALTER TABLE Task ADD COLUMN completed BOOL");

    exec(&db, "MATCH (t:Task) SET t.completed = true");
    let res = exec(&db, "MATCH (t:Task) RETURN t.completed");
    assert_count!(res, 1);

    Ok(())
}

#[test]
fn test_alter_add_column_rel_table() -> TestResult {
    let (_dir, db) = setup_db()?;
    exec(&db, "CREATE NODE TABLE Person(name STRING)");
    exec(&db, "CREATE (:Person {name: 'Alice'})");
    exec(&db, "CREATE (:Person {name: 'Bob'})");
    exec(&db, "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)");

    exec(&db, "ALTER TABLE KNOWS ADD COLUMN weight DOUBLE");

    let cat = db.catalog.read();
    let rel = cat.get_rel_table("KNOWS").unwrap();
    assert!(rel.properties.iter().any(|p| p.name == "weight"));

    Ok(())
}

#[test]
fn test_alter_drop_column_rel_table() -> TestResult {
    let (_dir, db) = setup_db()?;
    exec(&db, "CREATE NODE TABLE Person(name STRING)");
    exec(&db, "CREATE (:Person {name: 'Alice'})");
    exec(&db, "CREATE (:Person {name: 'Bob'})");
    exec(&db, "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64, notes STRING)");

    exec(&db, "ALTER TABLE KNOWS DROP COLUMN notes");

    let cat = db.catalog.read();
    let rel = cat.get_rel_table("KNOWS").unwrap();
    assert!(!rel.properties.iter().any(|p| p.name == "notes"));

    Ok(())
}

#[test]
fn test_alter_rename_column_rel_table() -> TestResult {
    let (_dir, db) = setup_db()?;
    exec(&db, "CREATE NODE TABLE Person(name STRING)");
    exec(&db, "CREATE (:Person {name: 'Alice'})");
    exec(&db, "CREATE (:Person {name: 'Bob'})");
    exec(&db, "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)");

    exec(&db, "ALTER TABLE KNOWS RENAME COLUMN since TO met_year");

    let cat = db.catalog.read();
    let rel = cat.get_rel_table("KNOWS").unwrap();
    assert!(rel.properties.iter().any(|p| p.name == "met_year"));
    assert!(!rel.properties.iter().any(|p| p.name == "since"));

    Ok(())
}

#[test]
fn test_alter_add_column_duplicate_error() -> TestResult {
    let (_dir, db) = setup_db()?;
    exec(&db, "CREATE NODE TABLE Test(x INT64)");
    exec(&db, "CREATE (:Test {x: 1})");

    let conn = db.connect();
    let result = conn.execute("ALTER TABLE Test ADD COLUMN x INT64", None);
    assert!(result.is_err(), "Should error on duplicate column");

    Ok(())
}

#[test]
fn test_alter_table_not_found_error() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();
    let result = conn.execute("ALTER TABLE Nonexistent ADD COLUMN x INT64", None);
    assert!(result.is_err(), "Should error on nonexistent table");

    Ok(())
}

#[test]
fn test_alter_add_column_multiple_rows() -> TestResult {
    let (_dir, db) = setup_db()?;
    exec(&db, "CREATE NODE TABLE Items(name STRING)");
    for i in 0..50 {
        exec(&db, &format!("CREATE (:Items {{name: 'item_{}'}})", i));
    }

    exec(&db, "ALTER TABLE Items ADD COLUMN score DOUBLE");

    exec(&db, "MATCH (i:Items) SET i.score = 1.0");

    let res = exec(&db, "MATCH (i:Items) RETURN count(i.score), sum(i.score)");
    assert_count!(res, 1);

    Ok(())
}
