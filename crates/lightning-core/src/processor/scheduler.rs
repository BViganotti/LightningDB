use crate::processor::{DataChunk, PhysicalOperator};
use crate::Result;
use crossbeam::channel::{unbounded, Receiver};
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
        let (ch_tx, rx) = unbounded();
        let params_arc = params.map(Arc::new);

        if self.num_threads == 1 || !operator.is_parallel_safe() {
            let mut op = operator;
            loop {
                match op.get_next(&database, &tx, params_arc.as_ref().map(|p| p.as_ref())) {
                    Ok(Some(chunk)) => {
                        if ch_tx.send(Ok(chunk)).is_err() { break; }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = ch_tx.send(Err(e));
                        break;
                    }
                }
            }
        } else {
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
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            if ch_tx.send(Err(e)).is_err() {
                                tracing::warn!("Failed to send query error to channel");
                            }
                            break;
                        }
                    }
                });
            }
        }
        drop(ch_tx);
        Ok(rx)
    }
}
