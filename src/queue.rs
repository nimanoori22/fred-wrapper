// crates/fred_wrapper/src/queue.rs
use futures::StreamExt;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{Instrument, Span};

use crate::{FredClient, FredError, Observation, params::Observations};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub enum FredRequest {
    ObservationsWindowed {
        params: Observations,
        window_years: u32,
        respond_to: oneshot::Sender<Result<Vec<Observation>, FredError>>,
    },
}

struct QueuedRequest {
    request: FredRequest,
    queued_at: Instant,
    span: Span,
}

/// A cheaply cloneable handle to a background task that owns the single
/// `FredClient` and processes requests one at a time. Because the actual
/// FRED calls happen inside one long-running task (spawned once, with a
/// concrete non-generic loop body), this sidesteps the Send-inference
/// issues that appeared when calling `FredClient` methods directly from
/// many separately-spawned tasks.
#[derive(Clone)]
pub struct FredQueue {
    tx: mpsc::Sender<QueuedRequest>,
}

impl FredQueue {
    pub fn spawn(client: FredClient, queue_capacity: usize) -> (Self, mpsc::Receiver<QueueEvent>) {
        let (tx, mut rx) = mpsc::channel::<QueuedRequest>(queue_capacity);
        let (event_tx, event_rx) = mpsc::channel::<QueueEvent>(1024);

        tokio::spawn(async move {
            while let Some(QueuedRequest {
                request,
                queued_at,
                span,
            }) = rx.recv().await
            {
                let client = client.clone();       // cheap: Arc-backed rate limiter, cloneable reqwest::Client
                let event_tx = event_tx.clone();
                tokio::spawn(async move{
                    match request {
                        FredRequest::ObservationsWindowed {
                            params,
                            window_years,
                            respond_to,
                        } => {
                            let queue_wait_ms = queued_at.elapsed().as_millis();
                            span.in_scope(|| {
                                tracing::info!(queue_wait_ms, "request dequeued");
                            });
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
                            .instrument(span.clone())
                            .await;

                            match result {
                                Ok(all) => {
                                    let observation_count = all.len();
                                    span.in_scope(|| {
                                        tracing::info!(observation_count, "request completed");
                                    });
                                    let _ = event_tx.try_send(QueueEvent::Succeeded);
                                    let _ = respond_to.send(Ok(all));
                                }

                                Err(e) => {
                                    span.in_scope(|| {
                                        tracing::info!("request failed");
                                    });
                                    let _ = event_tx.try_send(QueueEvent::Failed);
                                    let _ = respond_to.send(Err(e));
                                }
                            }
                        }
                    }
                });
            }
        });

        (Self { tx }, event_rx)
    }

    pub async fn observations_windowed(
        &self,
        params: Observations,
        window_years: u32,
    ) -> Result<Vec<Observation>, FredError> {
        let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let span = tracing::info_span!(
            "fred_request",
            request_id,
            series_id = %params.series_id,
            window_years,
        );
        span.in_scope(|| tracing::info!("request created"));

        let (respond_to, response) = oneshot::channel();
        let queued_at = Instant::now();
        self.tx
            .send(QueuedRequest {
                request: FredRequest::ObservationsWindowed {
                    params,
                    window_years,
                    respond_to,
                },
                queued_at,
                span: span.clone(),
            })
            .await
            .map_err(|_| {
                span.in_scope(|| tracing::info!("request failed: queue closed"));
                FredError::Unavailable("queue closed".into())
            })?;

        let submission_wait_ms = queued_at.elapsed().as_millis();
        span.in_scope(|| tracing::info!(submission_wait_ms, "request queued"));

        let result = response.await.map_err(|_| {
            span.in_scope(|| tracing::info!("request failed: queue worker dropped response"));
            FredError::Unavailable("queue worker dropped the response".into())
        })?;

        result
    }
}

/// Emitted by the queue worker for live observability — exactly what you
/// wanted to print: when a request actually starts, succeeds, or fails.
pub enum QueueEvent {
    Started,
    Succeeded,
    Failed,
}
