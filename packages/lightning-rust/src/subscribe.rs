use futures::StreamExt;
use reqwest::Response;
use tokio::sync::mpsc;

use crate::error::Error;
use crate::types::ChangeEvent;

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes.iter().position(|b| !b.is_ascii_whitespace()).unwrap_or(bytes.len());
    let end = bytes.iter().rposition(|b| !b.is_ascii_whitespace()).map(|p| p + 1).unwrap_or(start);
    &bytes[start..end]
}

pub async fn subscribe_sse(
    response: Response,
) -> Result<mpsc::Receiver<Result<ChangeEvent, Error>>, Error> {
    subscribe_sse_generic_inner::<ChangeEvent>(response).await
}

pub async fn subscribe_sse_generic(
    response: Response,
) -> Result<mpsc::Receiver<Result<serde_json::Value, Error>>, Error> {
    subscribe_sse_generic_inner::<serde_json::Value>(response).await
}

async fn subscribe_sse_generic_inner<T>(
    response: Response,
) -> Result<mpsc::Receiver<Result<T, Error>>, Error>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Result<T, Error>>(256);

    tokio::spawn(async move {
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    buffer.extend_from_slice(&bytes);
                    while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                        let line = buffer[..pos].to_vec();
                        buffer = buffer[pos + 1..].to_vec();

                        if line.starts_with(b"data: ") {
                            let data = trim_ascii_whitespace(&line[6..]);
                            if data == b"{}" || data.is_empty() {
                                continue;
                            }
                            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(data) {
                                if val.get("done").and_then(|v| v.as_bool()) == Some(true) {
                                    return;
                                }
                                if val.get("error").is_some() {
                                    let msg = val["error"]
                                        .as_str()
                                        .unwrap_or("unknown stream error");
                                    let _ = tx
                                        .send(Err(Error::Stream(msg.to_string())))
                                        .await;
                                    return;
                                }
                            }
                            match serde_json::from_slice::<T>(data) {
                                Ok(event) => {
                                    if tx.send(Ok(event)).await.is_err() {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    let _ = tx
                                        .send(Err(Error::Stream(format!(
                                            "failed to parse SSE event: {}",
                                            e
                                        ))))
                                        .await;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(Error::Stream(format!("SSE stream error: {}", e))))
                        .await;
                    return;
                }
            }
        }
    });

    Ok(rx)
}
