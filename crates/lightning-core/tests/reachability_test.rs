use lightning_core::Database;
use lightning_core::catalog::PropertyDefinition;
use lightning_types::LogicalType;
use tempfile::tempdir;

#[test]
fn test_recursive_reachability() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), lightning_core::SystemConfig { max_num_threads: 1, ..Default::default() }).unwrap();
    
    let conn = db.connect();
    
    // 1. Setup Graph: root -> dir1 -> file1
    //                     \-> dir2 -> file2
    conn.execute("CREATE NODE TABLE Item(id INT64, name STRING, is_dir BOOL, PRIMARY KEY (id))", None).unwrap();
    conn.execute("CREATE REL TABLE Contains(FROM Item TO Item)", None).unwrap();

    conn.execute("CREATE (:Item {id: 1, name: 'root', is_dir: true})", None).unwrap();
    conn.execute("CREATE (:Item {id: 2, name: 'dir1', is_dir: true})", None).unwrap();
    conn.execute("CREATE (:Item {id: 3, name: 'dir2', is_dir: true})", None).unwrap();
    conn.execute("CREATE (:Item {id: 4, name: 'file1', is_dir: false})", None).unwrap();
    conn.execute("CREATE (:Item {id: 5, name: 'file2', is_dir: false})", None).unwrap();

    conn.execute("MATCH (i1:Item {id: 1}), (i2:Item {id: 2}) CREATE (i1)-[:Contains]->(i2)", None).unwrap();
    conn.execute("MATCH (i1:Item {id: 1}), (i2:Item {id: 3}) CREATE (i1)-[:Contains]->(i2)", None).unwrap();
    conn.execute("MATCH (i1:Item {id: 2}), (i2:Item {id: 4}) CREATE (i1)-[:Contains]->(i2)", None).unwrap();
    conn.execute("MATCH (i1:Item {id: 3}), (i2:Item {id: 5}) CREATE (i1)-[:Contains]->(i2)", None).unwrap();

    // Verification: count nodes and rels
    let node_count = conn.execute("MATCH (n:Item) RETURN count(*)", None).unwrap();
    println!("DEBUG: Total Nodes: {:?}", node_count.batches[0].column(0));
    
    // Check if we can find root
    let root_check = conn.execute("MATCH (n:Item {id: 1}) RETURN n.name", None).unwrap();
    println!("DEBUG: Root check: rows={}", root_check.batches.iter().map(|b| b.num_rows()).sum::<usize>());
    if !root_check.batches.is_empty() && root_check.batches[0].num_rows() > 0 {
         let names = root_check.batches[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
         println!("DEBUG: Root name: {}", names.value(0));
    }

    let dir1_check = conn.execute("MATCH (n:Item {id: 2}) RETURN n.name", None).unwrap();
    println!("DEBUG: Dir1 check: rows={}", dir1_check.batches.iter().map(|b| b.num_rows()).sum::<usize>());
    if !dir1_check.batches.is_empty() && dir1_check.batches[0].num_rows() > 0 {
         let names = dir1_check.batches[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
         println!("DEBUG: Dir1 name: {}", names.value(0));
    }

    let rel_count = conn.execute("MATCH (a:Item)-[r:Contains]->(b:Item) RETURN count(*)", None).unwrap();
    if !rel_count.batches.is_empty() {
        println!("DEBUG: Total Relationships: {:?}", rel_count.batches[0].column(0));
    }

    // 2. Query with variable length path: Find all files reachable from root
    // Depth 1..2
    let query = "MATCH (root:Item {id: 1})-[r:Contains*1..2]->(f:Item) WHERE f.is_dir = false RETURN f.name ORDER BY f.name";
    let res = conn.execute(query, None).unwrap();
    
    assert!(res.success);
    // Ideally we should find file1 and file2
    let total_rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();
    println!("DEBUG: Found {} rows for recursive query", total_rows);
    
    assert_eq!(total_rows, 2);
    let names: Vec<String> = res.batches[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap().iter().map(|s| s.unwrap().to_string()).collect();
    assert_eq!(names, vec!["file1", "file2"]);
}
