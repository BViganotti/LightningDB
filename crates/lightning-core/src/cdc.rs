use crate::storage::wal::{WALRecord, WAL};
use crate::Result;
use crossbeam::channel::{bounded, Receiver, Sender};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

/// A CDC (Change Data Capture) event emitted when a WAL page update
/// is detected.
#[derive(Debug, Clone)]
pub struct CdcEvent {
    pub timestamp: i64,
    pub tx_id: u64,
    pub file_id: u64,
    pub page_idx: u64,
}

pub struct CdcSubscriber {
    pub rx: Receiver<CdcEvent>,
}

struct SubscriberEntry {
    tx: Sender<CdcEvent>,
    start_offset: u64,
}

/// Manages WAL-based Change Data Capture.
/// A background thread polls the WAL for new records and distributes
/// events to all active subscribers via bounded crossbeam channels.
pub struct CdcManager {
    inner: Arc<CdcManagerInner>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

struct CdcManagerInner {
    subscribers: Mutex<Vec<SubscriberEntry>>,
    running: AtomicBool,
}

impl CdcManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CdcManagerInner {
                subscribers: Mutex::new(Vec::new()),
                running: AtomicBool::new(false),
            }),
            handle: Mutex::new(None),
        }
    }

    pub fn subscribe(&self, wal: &WAL) -> Result<CdcSubscriber> {
        let current_size = wal.size().unwrap_or(0);
        let (tx, rx) = bounded(64);

        // Replay any records written before subscribe() returns to close the
        // race window between capturing the offset and the poll thread starting.
        if current_size > 0 {
            let mut iter = wal.read_records_from(current_size)?;
            loop {
                match iter.next_record() {
                    Some(WALRecord::PageUpdate { tx_id, file_id, page_idx, .. }) => {
                        let event = CdcEvent {
                            timestamp: now_micros(),
                            tx_id,
                            file_id,
                            page_idx,
                        };
                        if tx.try_send(event).is_err() {
                            tracing::warn!("CDC subscribe replay channel full, dropping event");
                        }
                    }
                    Some(WALRecord::Commit { .. }) => {}
                    Some(WALRecord::Corrupt { msg }) => {
                        tracing::warn!("CDC subscribe detected corrupt WAL record: {}", msg);
                    }
                    None => break,
                }
            }
        }

        self.inner.subscribers.lock().push(SubscriberEntry {
            tx,
            start_offset: current_size,
        });
        Ok(CdcSubscriber { rx })
    }

    pub fn start(&self, wal: Arc<WAL>) {
        self.inner.running.store(true, Ordering::Release);
        let inner = Arc::clone(&self.inner);

        let handle = std::thread::spawn(move || {
            let mut last_positions: Vec<u64> = Vec::new();

            while inner.running.load(Ordering::Acquire) {
                // Snapshot subscribers under lock, then release before I/O
                let snapshot: Vec<(Sender<CdcEvent>, u64)> = {
                    let subs = inner.subscribers.lock();
                    if subs.is_empty() {
                        drop(subs);
                        std::thread::sleep(std::time::Duration::from_millis(200));
                        continue;
                    }
                    if last_positions.len() != subs.len() {
                        last_positions.resize(subs.len(), 0);
                        for (i, entry) in subs.iter().enumerate() {
                            if i >= last_positions.len() || last_positions[i] == 0 {
                                last_positions[i] = entry.start_offset;
                            }
                        }
                    }
                    subs.iter().map(|e| (e.tx.clone(), e.start_offset)).collect()
                };

                for (i, (tx, _)) in snapshot.iter().enumerate() {
                    let offset = last_positions[i];
                    if let Ok(mut iter) = wal.read_records_from(offset) {
                        loop {
                            match iter.next_record() {
                                Some(WALRecord::PageUpdate { tx_id, file_id, page_idx, .. }) => {
                                    let event = CdcEvent {
                                        timestamp: now_micros(),
                                        tx_id,
                                        file_id,
                                        page_idx,
                                    };
                                    if tx.try_send(event).is_err() {
                                        tracing::warn!("CDC subscriber channel full, dropping event");
                                    }
                                }
                                Some(WALRecord::Commit { .. }) => {}
                                Some(WALRecord::Corrupt { msg }) => {
                                    tracing::warn!("CDC detected corrupt WAL record: {}", msg);
                                }
                                None => break,
                            }
                        }
                        last_positions[i] = iter.absolute_pos();
                    }
                }

                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        });

        *self.handle.lock() = Some(handle);
    }

    pub fn stop(&self) {
        self.inner.running.store(false, Ordering::Release);
        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }
    }
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

impl Drop for CdcManager {
    fn drop(&mut self) {
        self.stop();
    }
}
