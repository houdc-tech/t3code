use std::{sync::Arc, time::Duration};

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use hyper::StatusCode;
use serde::Deserialize;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    time::{Instant, MissedTickBehavior, interval_at, timeout},
};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::{Socket, config::DeviceConfig, state::Device};

#[derive(Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum ControlMessage {
    Register {
        protocol_version: u32,
        device_name: String,
        environment_id: String,
    },
    Ping {
        ts: Option<serde_json::Number>,
    },
    Pong,
    StreamRejected {
        stream_id: String,
        reason: Option<String>,
    },
    Disconnect,
}

async fn registration(socket: &mut Socket) -> bool {
    while let Some(Ok(message)) = socket.next().await {
        match message {
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Text(text) => {
                return matches!(
                    serde_json::from_str(&text),
                    Ok(ControlMessage::Register { protocol_version: 1, device_name, environment_id })
                        if !device_name.is_empty() && !environment_id.is_empty()
                );
            }
            _ => break,
        }
    }
    false
}

async fn send_control(socket: &mut Socket, message: Message) -> bool {
    matches!(
        timeout(Duration::from_secs(10), socket.send(message)).await,
        Ok(Ok(()))
    )
}

pub(crate) async fn control(
    mut socket: Socket,
    device: Arc<Device>,
    config: Arc<DeviceConfig>,
    heartbeat: Duration,
    register_timeout: Duration,
) {
    if timeout(register_timeout, registration(&mut socket)).await == Ok(true) {
        let mut session = device.register();
        let cancelled = session.cancelled.clone();
        let ws_scheme = if config.public_scheme == "https" {
            "wss"
        } else {
            "ws"
        };
        let ack = json!({
            "type": "registered",
            "protocolVersion": 1,
            "publicHttpBaseUrl": format!("{}://{}", config.public_scheme, config.host),
            "publicWsBaseUrl": format!("{}://{}", ws_scheme, config.host),
            "heartbeatIntervalSec": heartbeat.as_secs(),
        });
        let run = async {
            if !send_control(&mut socket, Message::text(ack.to_string())).await {
                return;
            }
            let mut last_seen = Instant::now();
            let mut ticks = interval_at(last_seen + heartbeat, heartbeat);
            ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                let outgoing = tokio::select! {
                    message = socket.next() => {
                        last_seen = Instant::now();
                        match message {
                            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                            Some(Ok(Message::Text(text))) => match serde_json::from_str(&text) {
                                Ok(ControlMessage::Ping { ts }) => Message::text(match ts {
                                    Some(ts) => json!({"type": "pong", "ts": ts}).to_string(),
                                    None => r#"{"type":"pong"}"#.to_owned(),
                                }),
                                Ok(ControlMessage::Pong) => continue,
                                Ok(ControlMessage::StreamRejected { stream_id, reason }) => {
                                    let status = if reason.as_deref() == Some("capacity_exceeded") {
                                        StatusCode::SERVICE_UNAVAILABLE
                                    } else {
                                        StatusCode::BAD_GATEWAY
                                    };
                                    session.reject(&stream_id, status);
                                    continue;
                                }
                                _ => break,
                            },
                            _ => break,
                        }
                    }
                    command = session.outbound.recv() => match command {
                        Some(command) => Message::text(command.to_string()),
                        None => break,
                    },
                    _ = ticks.tick() => {
                        if last_seen.elapsed() >= heartbeat * 3 {
                            break;
                        }
                        Message::Ping(Default::default())
                    }
                };
                if !send_control(&mut socket, outgoing).await {
                    break;
                }
            }
        };
        tokio::select! {
            _ = cancelled.cancelled() => {},
            _ = run => {},
        }
    }
    let _ = timeout(Duration::from_secs(1), SinkExt::close(&mut socket)).await;
}

pub(crate) async fn data(socket: Socket, io: DuplexStream, cancelled: CancellationToken) {
    let (mut sink, mut source) = socket.split();
    let (mut reader, mut writer) = tokio::io::split(io);
    let from_device = async {
        while let Some(Ok(message)) = source.next().await {
            match message {
                Message::Binary(bytes) => {
                    if writer.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
                // Tungstenite flushes the automatic pong on the next read.
                Message::Ping(_) | Message::Pong(_) => continue,
                _ => break,
            }
        }
    };
    let to_device = async {
        loop {
            let mut bytes = BytesMut::with_capacity(16 * 1024);
            match reader.read_buf(&mut bytes).await {
                Err(_) | Ok(0) => break,
                Ok(_) => {
                    if sink.send(Message::Binary(bytes.freeze())).await.is_err() {
                        break;
                    }
                }
            }
        }
    };
    tokio::select! {
        _ = cancelled.cancelled() => {},
        _ = from_device => {},
        _ = to_device => {},
    }
    drop((reader, writer));
    let _ = timeout(Duration::from_secs(1), sink.close()).await;
}
