//! Scratch probe: rel-table row counts via unlabeled node patterns (db_schema path).
use lightning_core::{Database, SystemConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    // Mirror the LightningMCP schema: multiple node tables, rel table between
    // two of the same node table.
    conn.execute("CREATE NODE TABLE CodeNode(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Workspace(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Settings(key STRING, value STRING, PRIMARY KEY (key))", None)?;
    conn.execute("CREATE REL TABLE Calls(FROM CodeNode TO CodeNode)", None)?;

    conn.execute("CREATE (:CodeNode {id: 1, name: 'a'})", None)?;
    conn.execute("CREATE (:CodeNode {id: 2, name: 'b'})", None)?;
    conn.execute("CREATE (:CodeNode {id: 3, name: 'c'})", None)?;
    conn.execute("CREATE (:Workspace {id: 1, name: 'ws'})", None)?;
    conn.execute("MATCH (x:CodeNode {id: 1}), (y:CodeNode {id: 2}) CREATE (x)-[:Calls]->(y)", None)?;
    conn.execute("MATCH (x:CodeNode {id: 2}), (y:CodeNode {id: 3}) CREATE (x)-[:Calls]->(y)", None)?;

    // Faithful mirror of the LightningMCP schema: STRING pks, ~15 node tables,
    // rel tables between differing node-table pairs.
    conn.execute("CREATE NODE TABLE GitCommit(sha STRING, author STRING, PRIMARY KEY (sha))", None)?;
    conn.execute("CREATE NODE TABLE Observation(id STRING, content STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE FileHash(file_path STRING, hash STRING, PRIMARY KEY (file_path))", None)?;
    conn.execute("CREATE NODE TABLE EnrichmentProgress(workspace_id STRING, status STRING, PRIMARY KEY (workspace_id))", None)?;
    conn.execute("CREATE NODE TABLE WatcherStats(id INT64, last_update STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Investigation(id STRING, content STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Memory(id STRING, content STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Telemetry(id STRING, tool_name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Log(id INT64, level STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Conversation(id STRING, title STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Message(id INT64, content STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE LspCache(file_path STRING, file_hash STRING, PRIMARY KEY (file_path))", None)?;
    conn.execute("CREATE REL TABLE Imports (FROM CodeNode TO CodeNode)", None)?;
    conn.execute("CREATE REL TABLE ModifiedIn (FROM CodeNode TO GitCommit)", None)?;
    conn.execute("CREATE REL TABLE SupportedBy (FROM Memory TO Investigation)", None)?;
    conn.execute("CREATE REL TABLE BelongsTo (FROM CodeNode TO Workspace)", None)?;

    conn.execute("CREATE (:GitCommit {sha: 'abc', author: 'dev'})", None)?;
    conn.execute("CREATE (:Investigation {id: 'i1', content: 'x'})", None)?;
    conn.execute("CREATE (:Memory {id: 'm1', content: 'y'})", None)?;
    conn.execute("MATCH (c:CodeNode {id: '3'}), (g:GitCommit {sha: 'abc'}) CREATE (c)-[:ModifiedIn]->(g)", None)?;
    conn.execute("MATCH (m:Memory {id: 'm1'}), (i:Investigation {id: 'i1'}) CREATE (m)-[:SupportedBy]->(i)", None)?;
    conn.execute("MATCH (c:CodeNode {id: '1'}), (w:Workspace {id: '1'}) CREATE (c)-[:BelongsTo]->(w)", None)?;

    let queries = [
        // db_schema's exact counting query shape
        "MATCH (a)-[r:Calls]->(b) RETURN count(*) AS cnt",
        // labeled variants
        "MATCH (a:CodeNode)-[r:Calls]->(b:CodeNode) RETURN count(*) AS cnt",
        // plain row output for comparison
        "MATCH (a)-[r:Calls]->(b) RETURN a.id, b.id",
        "MATCH (a:CodeNode)-[r:Calls]->(b:CodeNode) RETURN a.id, b.id",
        // rel-variable aggregate on labeled pattern
        "MATCH (a:CodeNode)-[r:Calls]->(b:CodeNode) RETURN count(r) AS cnt",
        // mixed-endpoint rels through unlabeled patterns
        "MATCH (a)-[r:ModifiedIn]->(b) RETURN count(*) AS cnt",
        "MATCH (a)-[r:SupportedBy]->(b) RETURN count(*) AS cnt",
        "MATCH (a)-[r:BelongsTo]->(b) RETURN count(*) AS cnt",
        "MATCH (a)-[r:ModifiedIn]->(b) RETURN a.id, b.sha",
    ];

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
                            } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
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