use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use hyper::StatusCode;
use serde_json::{Value, json};
use tokio::{
    io::{DuplexStream, duplex},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) struct Device {
    current: Mutex<Option<Arc<Connection>>>,
    max: usize,
    open_timeout: Duration,
}

struct Connection {
    cancelled: CancellationToken,
    outbound: mpsc::Sender<Value>,
    permits: Arc<Semaphore>,
    pending: Mutex<HashMap<String, ClaimedStream>>,
}

pub(crate) struct Session {
    connection: Arc<Connection>,
    pub outbound: mpsc::Receiver<Value>,
    pub cancelled: CancellationToken,
}

pub(crate) struct ClaimedStream {
    token: String,
    deadline: Instant,
    ready: oneshot::Sender<Result<DuplexStream, StatusCode>>,
    cancelled: CancellationToken,
}

pub(crate) struct OpenedStream {
    pub io: DuplexStream,
    pub lease: Lease,
}

pub(crate) struct Lease {
    connection: Arc<Connection>,
    stream_id: String,
    cancelled: CancellationToken,
    _permit: OwnedSemaphorePermit,
}

impl Device {
    pub(crate) fn new(max: usize, open_timeout: Duration) -> Self {
        Self {
            current: Mutex::new(None),
            max,
            open_timeout,
        }
    }

    pub(crate) fn register(&self) -> Session {
        let (outbound, receiver) = mpsc::channel(self.max);
        let connection = Arc::new(Connection {
            cancelled: CancellationToken::new(),
            outbound,
            permits: Arc::new(Semaphore::new(self.max)),
            pending: Mutex::default(),
        });
        let mut current = self.current.lock().unwrap();
        if let Some(old) = current.replace(connection.clone()) {
            old.cancelled.cancel();
        }
        Session {
            cancelled: connection.cancelled.clone(),
            connection,
            outbound: receiver,
        }
    }

    fn connection(&self) -> Option<Arc<Connection>> {
        self.current
            .lock()
            .unwrap()
            .as_ref()
            .filter(|connection| !connection.cancelled.is_cancelled())
            .cloned()
    }

    pub(crate) fn online(&self) -> bool {
        self.connection().is_some()
    }

    pub(crate) async fn open(&self) -> Result<OpenedStream, StatusCode> {
        let connection = self.connection().ok_or(StatusCode::BAD_GATEWAY)?;
        let permit = connection
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let deadline = Instant::now()
            .checked_add(self.open_timeout)
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let expires_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|now| now.checked_add(self.open_timeout))
            .and_then(|time| u64::try_from(time.as_millis()).ok())
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let stream_id = Uuid::new_v4().to_string();
        let token = Uuid::new_v4().to_string();
        let command = json!({
            "type": "open_stream",
            "streamId": stream_id,
            "streamToken": token,
            "expiresAtMs": expires_at_ms,
        });
        let lease = Lease {
            connection: connection.clone(),
            stream_id: stream_id.clone(),
            cancelled: connection.cancelled.child_token(),
            _permit: permit,
        };
        let (ready, receiver) = oneshot::channel();
        connection.pending.lock().unwrap().insert(
            stream_id,
            ClaimedStream {
                token,
                deadline,
                ready,
                cancelled: lease.cancelled.clone(),
            },
        );
        connection
            .outbound
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => StatusCode::SERVICE_UNAVAILABLE,
                mpsc::error::TrySendError::Closed(_) => StatusCode::BAD_GATEWAY,
            })?;
        let io = tokio::select! {
            biased;
            _ = connection.cancelled.cancelled() => return Err(StatusCode::BAD_GATEWAY),
            _ = sleep_until(deadline) => return Err(StatusCode::GATEWAY_TIMEOUT),
            ready = receiver => ready.unwrap_or(Err(StatusCode::BAD_GATEWAY))?,
        };
        Ok(OpenedStream { io, lease })
    }

    pub(crate) fn claim(&self, stream_id: &str, token: &str) -> Result<ClaimedStream, StatusCode> {
        let connection = self.connection().ok_or(StatusCode::UNAUTHORIZED)?;
        let mut pending = connection.pending.lock().unwrap();
        let entry = pending.get(stream_id).ok_or(StatusCode::UNAUTHORIZED)?;
        if !crate::token_matches(&entry.token, token) || Instant::now() >= entry.deadline {
            return Err(StatusCode::UNAUTHORIZED);
        }
        Ok(pending.remove(stream_id).unwrap())
    }
}

impl Session {
    pub(crate) fn reject(&self, stream_id: &str, status: StatusCode) {
        if let Some(stream) = self.connection.pending.lock().unwrap().remove(stream_id) {
            let _ = stream.ready.send(Err(status));
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.cancelled.cancel();
    }
}

impl ClaimedStream {
    pub(crate) fn attach(self) -> Option<(DuplexStream, CancellationToken)> {
        if self.cancelled.is_cancelled() || Instant::now() >= self.deadline {
            return None;
        }
        let (client, device) = duplex(64 * 1024);
        self.ready.send(Ok(client)).ok()?;
        Some((device, self.cancelled))
    }
}

impl Lease {
    pub(crate) async fn cancelled(&self) {
        self.cancelled.cancelled().await;
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.connection
            .pending
            .lock()
            .unwrap()
            .remove(&self.stream_id);
        self.cancelled.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_client_releases_slot_between_claim_and_upgrade() {
        let device = Device::new(1, Duration::from_secs(10));
        let mut session = device.register();
        let mut client = Box::pin(device.open());
        let command = tokio::select! {
            _ = &mut client => panic!("opened before connector attached"),
            command = session.outbound.recv() => command.unwrap(),
        };
        let claimed = device
            .claim(
                command["streamId"].as_str().unwrap(),
                command["streamToken"].as_str().unwrap(),
            )
            .unwrap();
        assert_eq!(
            device.open().await.err(),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );

        drop(client);
        assert!(claimed.attach().is_none());
        assert_eq!(session.connection.permits.available_permits(), 1);
    }
}
