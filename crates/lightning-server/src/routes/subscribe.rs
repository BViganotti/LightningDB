use std::convert::Infallible;

use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;

use crate::error::AppError;
use crate::extract::AppStore;

pub async fn subscribe_handler(
    AppStore(store): AppStore,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, AppError> {
    let rx = store.subscribe_changes().map_err(AppError::from)?;

    let stream = async_stream::stream! {
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
                    yield Ok(Event::default().json_data(payload).unwrap());
                }
                Err(_) => {
                    return;
                }
            }
        }
    };

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new().interval(std::time::Duration::from_secs(15)),
    ))
}
