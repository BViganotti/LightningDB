use lightning_core::processor::Value;
use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_fusion_performance() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();

    // 1. Test Vector Index
    let mut storage = db.storage_manager.write();
    storage.create_vector_index("CodeNode").unwrap();
    storage.create_fts_index("CodeNode").unwrap();
    let vec_idx = storage.vector_indexes.get("CodeNode").unwrap().clone();
    let fts_idx = storage.fts_indexes.get("CodeNode").unwrap().clone();

    let tx = db.transaction_manager.begin(true).unwrap();

    println!("--- Testing Parallel Vector Index ---");
    let start = Instant::now();
    let num_vectors = 10_000;
    let mut vecs = Vec::with_capacity(num_vectors);
    for i in 0..num_vectors {
        let mut emb = [0.0f32; 768];
        emb[0] = (i as f32) * 0.1;
        emb[1] = 1.0;
        vecs.push((i as u64, emb));
    }
    vec_idx
        .insert_batch(&vecs, &db.buffer_manager, &tx)
        .unwrap();
    println!("Inserted {} vectors in {:?}", num_vectors, start.elapsed());

    let start = Instant::now();
    let mut query = [0.0f32; 768];
    query[0] = 500.0 * 0.1;
    query[1] = 1.0;
    let res = vec_idx.search(&query, 10, &db.buffer_manager, &tx).unwrap();
    println!("Parallel Search top 10 vectors in {:?}", start.elapsed());
    assert_eq!(res.len(), 10);
    println!("Top match: Node {} with score {}", res[0].0, res[0].1);

    println!("\n--- Testing Multi-field FTS Index (Tantivy) ---");
    let start = Instant::now();
    let mut strings = Vec::with_capacity(10_000);
    let mut docs = Vec::with_capacity(10_000);
    for i in 0..10_000 {
        strings.push(format!("function test_{} code", i));
    }
    for i in 0..10_000 {
        docs.push((i as u64, strings[i as usize].as_str()));
    }
    // Test the field-specific batch insert
    fts_idx
        .insert_batch(&docs, "name", &db.buffer_manager, &tx)
        .unwrap();
    fts_idx.commit().unwrap();
    println!(
        "Inserted and committed 10,000 documents in {:?}",
        start.elapsed()
    );

    let start = Instant::now();
    // Wait for Tantivy commit reader reload
    std::thread::sleep(std::time::Duration::from_millis(100));
    let res2 = fts_idx
        .search("function", 10, &db.buffer_manager, &tx)
        .unwrap();
    println!("Searched Multi-field FTS in {:?}", start.elapsed());
    assert!(!res2.is_empty());
    println!("Top FTS match: Node {} with score {}", res2[0].0, res2[0].1);
}
