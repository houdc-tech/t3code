mod config;
mod proxy;
mod sockets;
mod state;

use std::{
    collections::HashMap, convert::Infallible, future::Future, io, sync::Arc, time::Duration,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
pub use config::Config;
use config::{DeviceConfig, canonical_host};
use http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Version, header};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::{body::Incoming, server::conn::http1, service::service_fn, upgrade::Upgraded};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde_json::json;
use sha2::{Digest, Sha256};
use state::Device;
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        handshake::derive_accept_key,
        protocol::{Role, WebSocketConfig},
    },
};

type Response = http::Response<UnsyncBoxBody<Bytes, hyper::Error>>;
type Socket = WebSocketStream<TokioIo<Upgraded>>;

pub struct Relay {
    devices: HashMap<String, (Arc<DeviceConfig>, Arc<Device>)>,
    heartbeat: Duration,
    open_timeout: Duration,
}

impl Relay {
    pub fn new(config: Config) -> Self {
        let heartbeat = Duration::from_secs(config.heartbeat_interval_secs);
        let open_timeout = Duration::from_secs(config.stream_open_timeout_secs);
        let devices = config
            .devices
            .into_iter()
            .map(|device| {
                let state = Device::new(config.max_concurrent_streams_per_device, open_timeout);
                (device.host.clone(), (Arc::new(device), Arc::new(state)))
            })
            .collect();
        Self {
            devices,
            heartbeat,
            open_timeout,
        }
    }

    pub async fn serve_connection(
        self: Arc<Self>,
        io: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    ) -> Result<(), hyper::Error> {
        let service = service_fn(move |req| {
            let relay = Arc::clone(&self);
            async move { Ok::<_, Infallible>(relay.handle(req).await) }
        });
        http1::Builder::new()
            .timer(TokioTimer::new())
            .header_read_timeout(Duration::from_secs(15))
            .serve_connection(TokioIo::new(io), service)
            .with_upgrades()
            .await
    }

    async fn handle(&self, mut req: Request<Incoming>) -> Response {
        let path = req.uri().path();
        if path == "/_relay/health" {
            if req.method() != Method::GET {
                return error(StatusCode::METHOD_NOT_ALLOWED);
            }
            let online = self
                .devices
                .values()
                .filter(|(_, device)| device.online())
                .count();
            let mut response = body(json!({"ok":true,"onlineDevices":online}).to_string());
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            return response;
        }

        let Some(host) = single_header(req.headers(), header::HOST).and_then(canonical_host) else {
            return error(StatusCode::BAD_REQUEST);
        };
        let Some((config, device)) = self.devices.get(&host) else {
            return error(StatusCode::NOT_FOUND);
        };
        let stream_id = path.strip_prefix("/_relay/stream/");
        if stream_id.is_none() && path != "/_relay/control" {
            return match device.open().await {
                Ok(stream) => proxy::forward(req, &config.public_scheme, stream).await,
                Err(status) => error(status),
            };
        }
        if req.method() != Method::GET {
            return error(StatusCode::METHOD_NOT_ALLOWED);
        }
        let Some(token) = bearer(req.headers()) else {
            return error(StatusCode::UNAUTHORIZED);
        };
        let key = match websocket_key(&req) {
            Ok(key) => key,
            Err(status) => return error(status),
        };
        if let Some(id) = stream_id {
            let claimed = match device.claim(id, token) {
                Ok(claimed) => claimed,
                Err(status) => return error(status),
            };
            return upgrade(&mut req, key, 16 * 1024 * 1024, move |ws| async move {
                if let Some((io, cancelled)) = claimed.attach() {
                    sockets::data(ws, io, cancelled).await;
                }
            });
        }
        if !token_matches(&config.token, token) {
            return error(StatusCode::UNAUTHORIZED);
        }
        let device = Arc::clone(device);
        let config = Arc::clone(config);
        let heartbeat = self.heartbeat;
        let register_timeout = self.open_timeout;
        upgrade(&mut req, key, 16 * 1024, move |ws| {
            sockets::control(ws, device, config, heartbeat, register_timeout)
        })
    }
}

fn body(contents: impl Into<Bytes>) -> Response {
    http::Response::new(
        Full::new(contents.into())
            .map_err(|never| match never {})
            .boxed_unsync(),
    )
}

fn error(status: StatusCode) -> Response {
    let mut response = body(status.canonical_reason().unwrap_or_default());
    *response.status_mut() = status;
    if status == StatusCode::METHOD_NOT_ALLOWED {
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("GET"));
    }
    if status == StatusCode::UNAUTHORIZED {
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    }
    response
}

fn single_header(headers: &HeaderMap, name: header::HeaderName) -> Option<&str> {
    let mut values = headers.get_all(name).iter();
    let first = values.next()?.to_str().ok()?;
    values.next().is_none().then_some(first)
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    let (scheme, value) = single_header(headers, header::AUTHORIZATION)?.split_once(' ')?;
    let token = value.trim_start_matches(' ');
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

fn token_matches(expected: &str, supplied: &str) -> bool {
    Sha256::digest(expected)
        .ct_eq(&Sha256::digest(supplied))
        .into()
}

fn websocket_key(req: &Request<Incoming>) -> Result<String, StatusCode> {
    let headers = req.headers();
    let connection_upgrade = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .any(|token| token.trim().eq_ignore_ascii_case("upgrade"));
    if req.version() != Version::HTTP_11
        || !connection_upgrade
        || !single_header(headers, header::UPGRADE)
            .is_some_and(|s| s.eq_ignore_ascii_case("websocket"))
        || single_header(headers, header::SEC_WEBSOCKET_VERSION) != Some("13")
        || headers.contains_key(header::TRANSFER_ENCODING)
        || headers
            .get(header::CONTENT_LENGTH)
            .is_some_and(|v| v != "0")
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let key = single_header(headers, header::SEC_WEBSOCKET_KEY).ok_or(StatusCode::BAD_REQUEST)?;
    if key.len() != 24 || !STANDARD.decode(key).is_ok_and(|bytes| bytes.len() == 16) {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(derive_accept_key(key.as_bytes()))
}

fn upgrade<F, Fut>(req: &mut Request<Incoming>, key: String, max_size: usize, handle: F) -> Response
where
    F: FnOnce(Socket) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let upgraded = hyper::upgrade::on(req);
    tokio::spawn(async move {
        if let Ok(io) = upgraded.await {
            let config = WebSocketConfig::default()
                .read_buffer_size(16 * 1024)
                .write_buffer_size(16 * 1024)
                .max_frame_size(Some(max_size))
                .max_message_size(Some(max_size));
            let socket =
                WebSocketStream::from_raw_socket(TokioIo::new(io), Role::Server, Some(config))
                    .await;
            handle(socket).await;
        }
    });
    let mut response = body(Bytes::new());
    *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    let headers = response.headers_mut();
    headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(
        header::SEC_WEBSOCKET_ACCEPT,
        HeaderValue::from_str(&key).unwrap(),
    );
    response
}

pub async fn serve(listener: TcpListener, config: Config) -> io::Result<()> {
    let relay = Arc::new(Relay::new(config));
    loop {
        let (tcp, _) = match listener.accept().await {
            Ok(connection) => connection,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::Interrupted | io::ErrorKind::ConnectionAborted
                ) =>
            {
                continue;
            }
            Err(err) => return Err(err),
        };
        let relay = Arc::clone(&relay);
        // Transport errors can contain a request target with credentials.
        tokio::spawn(relay.serve_connection(tcp));
    }
}
