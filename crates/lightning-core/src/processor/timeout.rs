use crate::processor::DataChunk;
use crate::Result;
use crossbeam::channel::{Receiver, RecvError, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// A receiver wrapper that enforces a query execution timeout.
/// When the timeout expires, the receiver stops yielding results
/// and returns `RecvError` (channel closed) on subsequent calls.
pub struct TimeoutReceiver {
    inner: Receiver<Result<DataChunk>>,
    cancelled: Arc<AtomicBool>,
}

impl TimeoutReceiver {
    pub fn new(rx: Receiver<Result<DataChunk>>, timeout_ms: u64) -> Receiver<Result<DataChunk>> {
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_clone = Arc::clone(&cancelled);

        // Spawn a timeout thread. When it fires, mark cancelled.
        // The worker threads will detect cancellation on their next
        // iteration and stop producing results.
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(timeout_ms));
            cancelled_clone.store(true, Ordering::Release);
        });

        let (tx, new_rx): (Sender<Result<DataChunk>>, Receiver<Result<DataChunk>>) =
            crossbeam::channel::bounded(64);

        // Relay thread: forwards results from inner rx to new rx,
        // but stops forwarding when cancelled.
        std::thread::spawn(move || {
            let cancelled = cancelled;
            loop {
                if cancelled.load(Ordering::Acquire) {
                    // Timeout fired — close the channel
                    break;
                }
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(item) => {
                        if tx.send(item).is_err() {
                            break;
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        // Check cancellation flag again at top of loop
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        // Query completed normally — close the channel
                        break;
                    }
                }
            }
        });

        new_rx
    }
}
