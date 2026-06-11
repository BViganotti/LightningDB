use crate::processor::{DataChunk, PhysicalOperator};
use crate::Result;
use crossbeam::channel::{bounded, Receiver};
use rayon::ThreadPoolBuilder;
use std::sync::Arc;

pub struct Scheduler {
    pool: rayon::ThreadPool,
    num_threads: usize,
}

impl Scheduler {
    pub fn new(num_threads: usize) -> Self {
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .expect("Failed to create thread pool");
        Self { pool, num_threads }
    }

    pub fn execute_operator(
        &self,
        operator: Box<dyn PhysicalOperator>,
        database: Arc<crate::Database>,
        tx: Arc<crate::transaction::transaction_manager::Transaction>,
        params: Option<std::collections::HashMap<String, crate::processor::Value>>,
    ) -> Result<Receiver<Result<DataChunk>>> {
        let (ch_tx, rx) = bounded(64);
        let params_arc = params.map(Arc::new);

        if self.num_threads > 1 && operator.is_parallel_safe() {
            if let Some(merged) = operator.try_parallelize(self.num_threads)? {
                let db = Arc::clone(&database);
                let tx_clone = Arc::clone(&tx);
                let p_clone = params_arc.clone();
                self.pool.spawn(move || {
                    let mut op = merged;
                    loop {
                        match op.get_next(&db, &tx_clone, p_clone.as_ref().map(|p| p.as_ref())) {
                            Ok(Some(chunk)) => {
                                if ch_tx.send(Ok(chunk)).is_err() {
                                    tracing::warn!("Failed to send chunk from merged worker: receiver dropped");
                                    break;
                                }
                            }
                            Ok(None) => break,
                            Err(e) => {
                                if ch_tx.send(Err(e)).is_err() {
                                    tracing::warn!("Failed to send error from merged worker: receiver dropped");
                                }
                                break;
                            }
                        }
                    }
                });
                drop(ch_tx);
                return Ok(rx);
            }

            for i in 0..self.num_threads {
                let mut op = operator.clone_box();
                op.set_partition(i, self.num_threads);
                let ch_tx = ch_tx.clone();
                let db = Arc::clone(&database);
                let tx_clone = Arc::clone(&tx);
                let p_clone = params_arc.clone();
                self.pool.spawn(move || loop {
                    match op.get_next(&db, &tx_clone, p_clone.as_ref().map(|p| p.as_ref())) {
                        Ok(Some(chunk)) => {
                            if ch_tx.send(Ok(chunk)).is_err() {
                                tracing::warn!("Failed to send chunk from worker {i}: receiver dropped");
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            if ch_tx.send(Err(e)).is_err() {
                                tracing::warn!("Failed to send error from worker {i}: receiver dropped");
                            }
                            break;
                        }
                    }
                });
            }
        } else {
            let mut op = operator;
            loop {
                match op.get_next(&database, &tx, params_arc.as_ref().map(|p| p.as_ref())) {
                    Ok(Some(chunk)) => {
                        if ch_tx.send(Ok(chunk)).is_err() {
                            tracing::warn!("Failed to send chunk from single-threaded operator: receiver dropped");
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        if ch_tx.send(Err(e)).is_err() {
                            tracing::warn!("Failed to send error from single-threaded operator: receiver dropped");
                        }
                        break;
                    }
                }
            }
        }
        drop(ch_tx);
        Ok(rx)
    }
}
