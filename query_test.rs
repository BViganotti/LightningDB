use lightning_core::{Database, Connection};
fn main() {
    let db = Database::new(".fusion_mcp.db").unwrap();
    let conn = db.connect();
    let id = 1; // dummy internal id that probably exists
    let query = format!("MATCH (pivot:CodeNode)-[:Calls]-(neighbor:CodeNode) WHERE pivot._id = {} RETURN neighbor._id LIMIT 1", id);
    println!("query: {}", query);
    let res = conn.query(&query);
    println!("res: {:?}", res.is_ok());
}
