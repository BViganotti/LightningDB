use crate::storage::buffer_manager::BufferManager;
use crate::Result;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};

pub struct InvertedIndex {
    #[allow(dead_code)]
    path: PathBuf,
    index: Index,
    writer: RwLock<IndexWriter>,
    reader: IndexReader,
    id_field: Field,
    content_field: Field,
}

impl InvertedIndex {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }

        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("node_id", FAST | STORED);
        let content_field = schema_builder.add_text_field("content", TEXT);
        let schema = schema_builder.build();

        let dir = tantivy::directory::MmapDirectory::open(&path)
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
            path,
            index,
            writer: RwLock::new(writer),
            reader,
            id_field,
            content_field,
        })
    }

    pub fn insert_batch(
        &self,
        docs: &[(u64, &str)],
        _bm: &BufferManager,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let writer = self.writer.read().unwrap();
        for (node_id, text) in docs {
            let mut doc = TantivyDocument::default();
            doc.add_u64(self.id_field, *node_id);
            doc.add_text(self.content_field, text);
            writer
                .add_document(doc)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    pub fn insert_multi_field(
        &self,
        node_id: u64,
        fields: &[&str],
    ) -> Result<()> {
        let writer = self.writer.read().unwrap();
        let mut doc = TantivyDocument::default();
        doc.add_u64(self.id_field, node_id);
        for text in fields {
            if !text.is_empty() {
                doc.add_text(self.content_field, text);
            }
        }
        writer
            .add_document(doc)
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let mut writer = self.writer.write().unwrap();
        writer
            .commit()
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Ok(())
    }

    pub fn search(
        &self,
        query_str: &str,
        limit: usize,
        _bm: &BufferManager,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(u64, f32)>> {
        let _ = self.reader.reload();
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(
            &self.index,
            vec![self.content_field],
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
            let node_id = retrieved_doc
                .get_first(self.id_field)
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            results.push((node_id, score));
        }

        Ok(results)
    }
}
