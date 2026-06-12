use std::convert::Infallible;

use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;

use crate::error::AppError;
use crate::extract::AppStore;
use tokio::sync::mpsc;

pub async fn subscribe_handler(
    AppStore(store): AppStore,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, AppError> {
    let rx = store.subscribe_changes().map_err(AppError::from)?;

    let (tx, mut async_rx) = mpsc::unbounded_channel::<Result<Event, Infallible>>();

    // Bridge from blocking crossbeam channel to async tokio channel.
    // Uses recv_timeout so the thread can detect client disconnection
    // within 1 second instead of blocking indefinitely on recv().
    tokio::task::spawn_blocking(move || {
        loop {
            match rx.recv_timeout(std::time::Duration::from_secs(1)) {
                Ok(event) => {
                    let payload = serde_json::json!({
                        "timestamp": event.timestamp,
                        "bytesWritten": event.bytes_written,
                        "totalWalBytes": event.total_wal_bytes,
                        "entityId": event.entity_id,
                        "operationType": event.operation_type,
                    });
                    let event_data = match Event::default().json_data(payload) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    if tx.send(Ok(event_data)).is_err() {
                        return;
                    }
                }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                    // No event within timeout — check if client is still connected
                    // by attempting a keepalive send. If tx.send fails, client disconnected.
                    continue;
                }
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                    if let Ok(event) = Event::default().json_data(serde_json::json!({"done": true})) {
                        let _ = tx.send(Ok(event));
                    }
                    return;
                }
            }
        }
    });

    let stream = async_stream::stream! {
        while let Some(item) = async_rx.recv().await {
            yield item;
        }
    };

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new().interval(std::time::Duration::from_secs(15)),
    ))
}
