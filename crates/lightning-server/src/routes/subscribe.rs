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

    // Bridge from blocking crossbeam channel to async tokio channel
    tokio::task::spawn_blocking(move || {
        loop {
            match rx.recv() {
                Ok(event) => {
                    let payload = serde_json::json!({
                        "timestamp": event.timestamp,
                        "bytesWritten": event.bytes_written,
                        "totalWalBytes": event.total_wal_bytes,
                        "entityId": event.entity_id,
                        "operationType": event.operation_type,
                    });
                    let _ = tx.send(Ok(Event::default().json_data(payload).unwrap()));
                }
                Err(_) => {
                    let _ = tx.send(Ok(Event::default().json_data(serde_json::json!({"done": true})).unwrap()));
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
