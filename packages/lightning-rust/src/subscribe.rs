use futures::StreamExt;
use reqwest::Response;
use tokio::sync::mpsc;

use crate::error::Error;
use crate::types::ChangeEvent;

pub async fn subscribe_sse(
    response: Response,
) -> Result<mpsc::Receiver<Result<ChangeEvent, Error>>, Error> {
    let (tx, rx) = mpsc::channel::<Result<ChangeEvent, Error>>(256);

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
                            let data = &line[6..];
                            match serde_json::from_slice::<ChangeEvent>(data) {
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
