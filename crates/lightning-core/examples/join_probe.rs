//! Scratch probe: cross-table join investigation.
use lightning_core::{Database, SystemConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE A(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE B(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:A {id: 1, val: 10})", None)?;
    conn.execute("CREATE (:A {id: 2, val: 20})", None)?;
    conn.execute("CREATE (:B {id: 1, val: 100})", None)?;
    conn.execute("CREATE (:B {id: 2, val: 200})", None)?;

    let queries = [
        "MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a.id, b.id",
        "MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a.val, b.val",
        "MATCH (a:A), (b:B) WHERE a.id = b.id AND a.val > 10 RETURN a.id, b.id, a.val",
        "MATCH (a:A), (b:B), (c:B) WHERE a.id = b.id AND b.id = c.id RETURN a.id, c.id",
        "MATCH (a:A), (b:A) WHERE a.id < b.id RETURN a.id, b.id",
        "MATCH (x:N {id: 1})-[:relx]->(y:N) RETURN x.id, y.id",
        "MATCH (a)-[:relx]->(b) RETURN a.id, b.id",
        "MATCH (a)-[r:relx]->(b) RETURN a.id, b.id",
        "MATCH (a:N)-[:relx]->(b) RETURN a.id, b.id",
        "MATCH (a)-[:relx]->(b:N) RETURN a.id, b.id",
        "MATCH (a:A) WHERE a.val >= 10 RETURN a.id ORDER BY a.id",
        "MATCH (a:A {id: 1}) RETURN a.val + 10",
        "MATCH (a:A {id: 1}) RETURN 100 - a.val",
        "MATCH (a:A {id: 1}) RETURN (a.val + a.val) * 2",
    ];

    // rel table for the last query
    conn.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE relx (FROM N TO N)", None)?;
    conn.execute("CREATE (:N {id: 1})", None)?;
    conn.execute("CREATE (:N {id: 2})", None)?;
    conn.execute("MATCH (x:N {id: 1}), (y:N {id: 2}) CREATE (x)-[:relx]->(y)", None)?;

    for q in queries {
        match conn.execute(q, None) {
            Ok(res) => {
                let rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();
                let mut preview = String::new();
                if let Some(b) = res.batches.first() {
                    for r in 0..b.num_rows().min(4) {
                        let mut row = Vec::new();
                        for c in 0..b.num_columns() {
                            let col = b.column(c);
                            let v = if let Some(a) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                                format!("{}", a.value(r))
                            } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
                                format!("{}", a.value(r))
                            } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                                a.value(r).to_string()
                            } else {
                                format!("({:?})", col.data_type())
                            };
                            row.push(v);
                        }
                        preview.push_str(&format!(" [{}]", row.join(", ")));
                    }
                }
                println!("OK    {q}  -> {rows} rows{preview}");
            }
            Err(e) => println!("ERR   {q}  -> {e}"),
        }
    }
    Ok(())
}
