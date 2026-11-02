use crate::storage::buffer_manager::BufferManager;
use crate::Result;
use parking_lot::RwLock;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};

/// Default writer memory budget for the inverted index.
/// 50MB provides a good balance between memory usage and write performance.
const DEFAULT_WRITER_MEMORY_BYTES: usize = 50_000_000;

pub struct InvertedIndex {
    index: Index,
    writer: RwLock<IndexWriter>,
    reader: IndexReader,
    id_field: Field,
    /// Map from column name to tantivy Field for multi-column FTS.
    content_fields: std::collections::HashMap<String, Field>,
    /// True when writes have been buffered into the Tantivy writer but not yet
    /// committed. Committing every single insert was the dominant cost of
    /// `MemoryStore` writes (a Tantivy commit fsyncs, ~0.2s); small batches now
    /// defer the commit until the next search or checkpoint.
    pending: AtomicBool,
}

impl InvertedIndex {
    pub fn new(path: impl AsRef<Path>, field_names: &[String]) -> Result<Self> {
        let p = path.as_ref();
        if !p.exists() {
            std::fs::create_dir_all(p)?;
        }

        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("node_id", FAST | STORED);
        let mut content_fields = std::collections::HashMap::new();

        if field_names.is_empty() {
            let f = schema_builder.add_text_field("content", TEXT);
            content_fields.insert("content".to_string(), f);
        } else {
            for name in field_names {
                let f = schema_builder.add_text_field(name, TEXT);
                content_fields.insert(name.clone(), f);
            }
        }

        let schema = schema_builder.build();

        // Open the existing Tantivy index, or create it. If an index already
        // exists with a *different* schema (a column was added/removed, or an
        // index left by an older generation is being reused), Tantivy returns
        // "An index exists but the schema does not match". FTS is a derived
        // index that can always be rebuilt from the column store, so a schema
        // mismatch must never make the whole database unopenable — that turned
        // one stale index into a total workspace/session loss. On any open
        // failure, drop the index directory and recreate it from scratch.
        let index = match Self::open_or_create_dir(p, &schema) {
            Ok(idx) => idx,
            Err(e) => {
                tracing::warn!(
                    "FTS index at {} is unusable ({}); rebuilding it from scratch",
                    p.display(),
                    e
                );
                if let Err(re) = std::fs::remove_dir_all(p) {
                    return Err(crate::LightningError::Internal(format!(
                        "failed to remove incompatible FTS index {}: {re}",
                        p.display()
                    )));
                }
                std::fs::create_dir_all(p)?;
                Self::open_or_create_dir(p, &schema)?
            }
        };

        let writer = index
            .writer(DEFAULT_WRITER_MEMORY_BYTES)
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

        Ok(Self {
            index,
            writer: RwLock::new(writer),
            reader,
            id_field,
            content_fields,
            pending: AtomicBool::new(false),
        })
    }

    /// Open a Tantivy index at `p` with `schema`, creating it when absent.
    fn open_or_create_dir(p: &Path, schema: &Schema) -> Result<Index> {
        let dir = tantivy::directory::MmapDirectory::open(p)
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Index::open_or_create(dir, schema.clone())
            .map_err(|e| crate::LightningError::Internal(e.to_string()))
    }

    fn mark_pending(&self) {
        self.pending.store(true, Ordering::Release);
    }
    /// True when there are uncommitted FTS writes.
    pub fn has_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    pub fn insert_batch(
        &self,
        docs: &[(u64, &str)],
        _bm: &BufferManager,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        // Acquire write lock once for the entire batch to reduce lock overhead.
        // This blocks concurrent searches during the batch but is more efficient
        // than acquiring/releasing the lock per document.
        let writer = self.writer.write();
        for (node_id, text) in docs {
            let mut doc = TantivyDocument::default();
            doc.add_u64(self.id_field, *node_id);
            for field in self.content_fields.values() {
                doc.add_text(*field, text);
            }
            writer
                .add_document(doc)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        }
        self.mark_pending();
        Ok(())
    }

    pub fn insert_multi_field_batch(&self, docs: &[(u64, Vec<(String, &str)>)]) -> Result<()> {
        for (node_id, fields) in docs {
            let mut doc = TantivyDocument::default();
            doc.add_u64(self.id_field, *node_id);
            for (field_name, text) in fields {
                if !text.is_empty() {
                    if let Some(field) = self.content_fields.get(field_name) {
                        doc.add_text(*field, text);
                    }
                }
            }
            self.writer
                .write()
                .add_document(doc)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        }
        self.mark_pending();
        Ok(())
    }

    pub fn insert_multi_field(&self, node_id: u64, fields: &[(String, &str)]) -> Result<()> {
        let writer = self.writer.write();
        let mut doc = TantivyDocument::default();
        doc.add_u64(self.id_field, node_id);
        for (field_name, text) in fields {
            if !text.is_empty() {
                if let Some(field) = self.content_fields.get(field_name) {
                    doc.add_text(*field, text);
                }
            }
        }
        writer
            .add_document(doc)
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        self.mark_pending();
        Ok(())
    }

    // #48: TODO — WAL/MVCC integration for FTS (Tantivy) index.
    // Tantivy commits independently of the Lightning WAL. On transaction
    // rollback, the FTS index retains changes from the aborted transaction.
    // Fix: coordinate FTS index commits with MVCC transaction lifecycle:
    // buffer FTS mutations in memory until the transaction commits, then
    // flush to Tantivy only on successful commit. On rollback, discard
    // buffered mutations without touching Tantivy.
    // DEEP_AUDIT_FULL_2024.md item #48.
    pub fn commit(&self) -> Result<()> {
        let mut writer = self.writer.write();
        writer
            .commit()
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        self.pending.store(false, Ordering::Release);
        Ok(())
    }

    /// Commit only if there are buffered writes, so reads/checkpoints flush
    /// the index without paying a commit when nothing changed.
    pub fn commit_if_pending(&self) -> Result<()> {
        if self.has_pending() {
            self.commit()
        } else {
            Ok(())
        }
    }

    pub fn delete(&self, node_id: u64) -> Result<()> {
        let writer = self.writer.write();
        let term = tantivy::Term::from_field_u64(self.id_field, node_id);
        writer.delete_term(term);
        drop(writer);
        self.mark_pending();
        Ok(())
    }

    pub fn delete_batch(&self, node_ids: &[u64]) -> Result<()> {
        let writer = self.writer.write();
        for &node_id in node_ids {
            let term = tantivy::Term::from_field_u64(self.id_field, node_id);
            writer.delete_term(term);
        }
        drop(writer);
        self.mark_pending();
        Ok(())
    }

    pub fn search(
        &self,
        query_str: &str,
        limit: usize,
        _bm: &BufferManager,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(u64, f32)>> {
        // Flush buffered writes so a search always sees prior inserts/deletes
        // (the write path defers commits for throughput).
        self.commit_if_pending()?;
        self.reader.reload().map_err(|e| {
            crate::LightningError::Internal(format!("inverted_index reload failed: {e}"))
        })?;
        let searcher = self.reader.searcher();
        let search_fields: Vec<Field> = self.content_fields.values().copied().collect();
        let query_parser = QueryParser::for_index(&self.index, search_fields);

        let query = query_parser
            .parse_query(query_str)
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(limit).order_by_score())
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher
                .doc(doc_address)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
            if let Some(node_id) = retrieved_doc
                .get_first(self.id_field)
                .and_then(|v| v.as_u64())
            {
                results.push((node_id, score));
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::InvertedIndex;

    /// A persisted FTS index whose schema no longer matches the current model
    /// must not make the database unopenable. Opening with a different set of
    /// text fields has to self-heal (drop + rebuild) instead of surfacing
    /// Tantivy's "schema does not match" error.
    #[test]
    fn schema_mismatch_self_heals() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("CodeNode_fts");

        {
            let first = InvertedIndex::new(&path, &["name".to_string()]);
            assert!(first.is_ok(), "initial FTS index creation must succeed");
        } // drop releases the writer lock

        // Reopen with a different schema (an extra string column was added).
        let second = InvertedIndex::new(&path, &["name".to_string(), "signature".to_string()]);
        assert!(
            second.is_ok(),
            "reopening with a changed schema must rebuild the index, not fail: {:?}",
            second.err()
        );
    }
}
