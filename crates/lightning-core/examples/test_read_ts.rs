use lightning_core::{Database, SystemConfig};

fn main() -> lightning_core::Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING)", None)?;
    conn.execute("CREATE REL TABLE Follows(FROM Person TO Person)", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob'})", None)?;
    conn.execute("CREATE (:Person {id: 3, name: 'Charlie'})", None)?;

    println!("=== Testing cartesian product ===");

    // Test comma-separated patterns (cartesian product)
    let res = conn.execute("MATCH (a:Person), (b:Person) RETURN a.name, b.name", None);
    match &res {
        Ok(r) => println!(
            "(a:Person), (b:Person): {} rows",
            r.batches.iter().map(|b| b.num_rows()).sum::<usize>()
        ),
        Err(e) => println!("Error: {}", e),
    }

    // Test with two MATCH clauses
    println!("\n=== Testing WITH clause ===");
    let res = conn.execute(
        "MATCH (a:Person {id: 1}) WITH a MATCH (b:Person {id: 2}) RETURN a.name, b.name",
        None,
    );
    match &res {
        Ok(r) => println!("WITH: {:?}", r.batches),
        Err(e) => println!("Error: {}", e),
    }

    // Test explicit cross join syntax
    let res = conn.execute(
        "MATCH (a:Person {id: 1}) MATCH (b:Person {id: 2}) RETURN a.name, b.name",
        None,
    );
    match &res {
        Ok(r) => println!("MATCH MATCH: {:?}", r.batches),
        Err(e) => println!("Error: {}", e),
    }

    // Test relationship creation with two MATCHes
    println!("\n=== Testing relationship creation ===");
    let res = conn.execute(
        "MATCH (a:Person {id: 1}) MATCH (b:Person {id: 2}) CREATE (a)-[:Follows]->(b)",
        None,
    );
    match &res {
        Ok(r) => println!("CREATE with MATCH MATCH: {:?}", r.batches),
        Err(e) => println!("Error: {}", e),
    }

    // Check storage
    let sm = db.storage_manager().read();
    if let Some(follows_table) = sm.get_table("Follows") {
        for col in &follows_table.columns {
            let stats = col.stats.read();
            println!("Follows.{}: num_values={}", col.name, stats.num_values);
        }
    }

    // Query relationships
    let res = conn.execute(
        "MATCH (a:Person)-[:Follows]->(b:Person) RETURN a.name, b.name",
        None,
    );
    match &res {
        Ok(r) => println!("\nRelationship query: {:?}", r.batches),
        Err(e) => println!("Error: {}", e),
    }

    Ok(())
}
