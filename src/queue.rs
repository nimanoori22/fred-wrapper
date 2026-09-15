// crates/fred_wrapper/src/queue.rs
use tokio::sync::{mpsc, oneshot};
use crate::{FredClient, FredError, Observation, params::Observations};
use futures::StreamExt;

pub enum FredRequest {
    ObservationsWindowed {
        params: Observations,
        window_years: u32,
        respond_to: oneshot::Sender<Result<Vec<Observation>, FredError>>,
    },
}

/// A cheaply cloneable handle to a background task that owns the single
/// `FredClient` and processes requests one at a time. Because the actual
/// FRED calls happen inside one long-running task (spawned once, with a
/// concrete non-generic loop body), this sidesteps the Send-inference
/// issues that appeared when calling `FredClient` methods directly from
/// many separately-spawned tasks.
#[derive(Clone)]
pub struct FredQueue {
    tx: mpsc::Sender<FredRequest>,
}

impl FredQueue {
    pub fn spawn(client: FredClient, queue_capacity: usize) -> (Self, mpsc::Receiver<QueueEvent>) {
        let (tx, mut rx) = mpsc::channel::<FredRequest>(queue_capacity);
        let (event_tx, event_rx) = mpsc::channel::<QueueEvent>(1024);

        tokio::spawn(async move {
            while let Some(request) = rx.recv().await {
                match request {
                    FredRequest::ObservationsWindowed { params, window_years, respond_to } => {
                        let _ = event_tx.try_send(QueueEvent::Started);

                        let client_series = client.series();

                        let stream = client_series.observations_windowed(params, window_years);
                        futures::pin_mut!(stream);

                        let mut all = Vec::new();

                        let result = async {
                            while let Some(page) = stream.next().await {
                                match page {
                                    Ok(batch) => all.extend(batch),
                                    Err(e) => return Err(e),
                                }
                            }

                            Ok::<Vec<Observation>, FredError>(all)
                        }
                        .await;

                        match result {
                            Ok(all) => {
                                let _ = event_tx.try_send(QueueEvent::Succeeded);
                                let _ = respond_to.send(Ok(all));
                            }

                            Err(e) => {
                                let _ = event_tx.try_send(QueueEvent::Failed);
                                let _ = respond_to.send(Err(e));
                            }
                        }
                    }
                }
            }
        });

        (Self { tx }, event_rx)
    }

    pub async fn observations_windowed(
        &self,
        params: Observations,
        window_years: u32,
    ) -> Result<Vec<Observation>, FredError> {
        let (respond_to, response) = oneshot::channel();
        self.tx
            .send(FredRequest::ObservationsWindowed { params, window_years, respond_to })
            .await
            .map_err(|_| FredError::Unavailable("queue closed".into()))?;
        response
            .await
            .map_err(|_| FredError::Unavailable("queue worker dropped the response".into()))?
    }
}

/// Emitted by the queue worker for live observability — exactly what you
/// wanted to print: when a request actually starts, succeeds, or fails.
pub enum QueueEvent {
    Started,
    Succeeded,
    Failed,
}