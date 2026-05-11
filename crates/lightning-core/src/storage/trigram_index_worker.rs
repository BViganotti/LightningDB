use crate::storage::index::trigram_index::TrigramIndex;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread;

pub struct TrigramIndexWorker {
    task_tx: Sender<TrigramIndexTask>,
}

#[derive(Debug)]
pub enum TrigramIndexTask {
    Insert(u64, String),
    InsertBatch(Vec<(u64, String)>),
    Flush,
    Shutdown,
}

impl TrigramIndexWorker {
    pub fn new(index: Arc<TrigramIndex>) -> Self {
        let (task_tx, task_rx) = channel();
        thread::Builder::new()
            .name("trigram-index-worker".to_string())
            .spawn(move || {
                Self::worker_loop(index, task_rx);
            })
            .expect("Failed to spawn trigram index worker thread");

        Self { task_tx }
    }

    fn worker_loop(index: Arc<TrigramIndex>, task_rx: Receiver<TrigramIndexTask>) {
        let mut pending: Vec<(u64, String)> = Vec::with_capacity(1000);
        const BATCH_SIZE: usize = 500;

        loop {
            match task_rx.recv_timeout(std::time::Duration::from_millis(1)) {
                Ok(TrigramIndexTask::Shutdown) => {
                    for (row_id, value) in pending.drain(..) {
                        index.insert(row_id, &value);
                    }
                    break;
                }
                Ok(TrigramIndexTask::Flush) => {
                    for (row_id, value) in pending.drain(..) {
                        index.insert(row_id, &value);
                    }
                }
                Ok(TrigramIndexTask::Insert(row_id, value)) => {
                    pending.push((row_id, value));
                    if pending.len() >= BATCH_SIZE {
                        for (row_id, value) in pending.drain(..) {
                            index.insert(row_id, &value);
                        }
                    }
                }
                Ok(TrigramIndexTask::InsertBatch(entries)) => {
                    for (row_id, value) in pending.drain(..) {
                        index.insert(row_id, &value);
                    }
                    for (row_id, value) in entries {
                        index.insert(row_id, &value);
                    }
                }
                Err(_) => {
                    for (row_id, value) in pending.drain(..) {
                        index.insert(row_id, &value);
                    }
                }
            }
        }
    }

    pub fn insert(&self, row_id: u64, value: String) {
        let _ = self.task_tx.send(TrigramIndexTask::Insert(row_id, value));
    }

    pub fn insert_batch(&self, entries: Vec<(u64, String)>) {
        let _ = self.task_tx.send(TrigramIndexTask::InsertBatch(entries));
    }

    pub fn flush(&self) {
        let _ = self.task_tx.send(TrigramIndexTask::Flush);
    }
}

impl Drop for TrigramIndexWorker {
    fn drop(&mut self) {
        let _ = self.task_tx.send(TrigramIndexTask::Shutdown);
    }
}
