use crate::storage::buffer_manager::BufferManager;
use crate::Result;
use parking_lot::RwLock;
use std::path::Path;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};

pub struct InvertedIndex {
    index: Index,
    writer: RwLock<IndexWriter>,
    reader: IndexReader,
    id_field: Field,
    /// Map from column name to tantivy Field for multi-column FTS.
    content_fields: std::collections::HashMap<String, Field>,
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

        let dir = tantivy::directory::MmapDirectory::open(p)
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

        let index = Index::open_or_create(dir, schema.clone())
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

        let writer = index
            .writer(50_000_000)
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
        })
    }

    pub fn insert_batch(
        &self,
        docs: &[(u64, &str)],
        _bm: &BufferManager,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        for (node_id, text) in docs {
            let mut doc = TantivyDocument::default();
            doc.add_u64(self.id_field, *node_id);
            for (_name, field) in &self.content_fields {
                doc.add_text(*field, text);
            }
            // Acquire write lock per document to avoid holding it for the entire batch
            // (which blocks concurrent searches that take a read lock).
            self.writer.write()
                .add_document(doc)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    pub fn insert_multi_field_batch(
        &self,
        docs: &[(u64, Vec<(String, &str)>)],
    ) -> Result<()> {
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
            self.writer.write()
                .add_document(doc)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    pub fn insert_multi_field(
        &self,
        node_id: u64,
        fields: &[(String, &str)],
    ) -> Result<()> {
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
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let mut writer = self.writer.write();
        writer
            .commit()
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Ok(())
    }

    pub fn delete(&self, node_id: u64) -> Result<()> {
        let writer = self.writer.write();
        let term = tantivy::Term::from_field_u64(self.id_field, node_id);
        writer.delete_term(term);
        Ok(())
    }

    pub fn delete_batch(&self, node_ids: &[u64]) -> Result<()> {
        let writer = self.writer.write();
        for &node_id in node_ids {
            let term = tantivy::Term::from_field_u64(self.id_field, node_id);
            writer.delete_term(term);
        }
        Ok(())
    }

    pub fn search(
        &self,
        query_str: &str,
        limit: usize,
        _bm: &BufferManager,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(u64, f32)>> {
        if let Err(e) = self.reader.reload() {
            tracing::warn!("inverted_index reload failed (stale results possible): {e}");
        }
        let searcher = self.reader.searcher();
        let search_fields: Vec<Field> = self.content_fields.values().copied().collect();
        let query_parser = QueryParser::for_index(
            &self.index,
            search_fields,
        );

        let query = query_parser
            .parse_query(query_str)
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

        let top_docs = searcher
            .search(
                &query,
                &TopDocs::with_limit(limit).order_by_score(),
            )
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
