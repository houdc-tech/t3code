use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use self_hosted_relay::{Config, Relay, serve};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, duplex},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream, client_async,
    tungstenite::{Error as WsError, Message, client::IntoClientRequest},
};

#[path = "cases/websocket.rs"]
mod websocket;

const MAC_A: &str = "mac-a.test:8443";
const MAC_B: &str = "mac-b.test";
const TOKEN_A: &str = "test-token-a";
const TOKEN_B: &str = "test-token-b";
trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}
type Ws = WebSocketStream<Box<dyn Socket>>;

#[derive(Clone)]
enum Endpoint {
    Tcp(SocketAddr),
    InMemory(Arc<Relay>),
}

impl Endpoint {
    async fn connect(&self) -> Box<dyn Socket> {
        match self {
            Self::Tcp(addr) => Box::new(within(TcpStream::connect(addr)).await.unwrap()),
            Self::InMemory(relay) => {
                let (client, server) = duplex(16 * 1024);
                tokio::spawn(relay.clone().serve_connection(server));
                Box::new(client)
            }
        }
    }
}

async fn within<T>(action: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(8), action)
        .await
        .expect("relay timed out")
}

struct TestRelay {
    endpoint: Endpoint,
    task: Option<JoinHandle<std::io::Result<()>>>,
    heartbeat_secs: u64,
}

impl TestRelay {
    async fn new() -> Self {
        Self::with_intervals(5, 25).await
    }

    async fn with_intervals(open_secs: u64, heartbeat_secs: u64) -> Self {
        let config = Config::parse(&format!(
            r#"
                stream_open_timeout_secs = {open_secs}
                heartbeat_interval_secs = {heartbeat_secs}
                max_concurrent_streams_per_device = 1
                [[devices]]
                id = "mac-a"
                host = "{MAC_A}"
                token = "{TOKEN_A}"
                public_scheme = "https"
                [[devices]]
                id = "mac-b"
                host = "{MAC_B}"
                token = "{TOKEN_B}"
            "#
        ))
        .unwrap();
        match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => Self {
                heartbeat_secs,
                endpoint: Endpoint::Tcp(listener.local_addr().unwrap()),
                task: Some(tokio::spawn(serve(listener, config))),
            },
            // A sandbox may deny loopback bind. Exercise the same HTTP/1 connection entrypoint.
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => Self {
                heartbeat_secs,
                endpoint: Endpoint::InMemory(Arc::new(Relay::new(config))),
                task: None,
            },
            Err(err) => panic!("could not bind test listener: {err}"),
        }
    }

    fn get(&self, host: &'static str, path: &'static str) -> JoinHandle<Vec<u8>> {
        let endpoint = self.endpoint.clone();
        tokio::spawn(async move { request(&endpoint, host, "GET", path, "").await })
    }

    async fn health(&self) -> Value {
        let response = request(&self.endpoint, MAC_A, "GET", "/_relay/health", "").await;
        assert_eq!(status(&response), 200);
        serde_json::from_slice(body(&response)).unwrap()
    }
}

impl Drop for TestRelay {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn ws_connect(
    endpoint: &Endpoint,
    host: &str,
    path: &str,
    token: Option<&str>,
) -> Result<Ws, WsError> {
    let mut request = format!("ws://{host}{path}").into_client_request().unwrap();
    if let Some(token) = token {
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    let tcp = endpoint.connect().await;
    within(client_async(request, tcp)).await.map(|(ws, _)| ws)
}

async fn send_json(ws: &mut Ws, value: Value) {
    within(ws.send(Message::text(value.to_string())))
        .await
        .unwrap();
}

async fn read_json(ws: &mut Ws) -> Value {
    within(async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
                Message::Ping(_) => ws.flush().await.unwrap(),
                Message::Pong(_) => (),
                frame => panic!("unexpected control frame: {frame:?}"),
            }
        }
    })
    .await
}

async fn control(relay: &TestRelay, host: &str, token: &str) -> Ws {
    let mut ws = ws_connect(&relay.endpoint, host, "/_relay/control", Some(token))
        .await
        .unwrap();
    send_json(
        &mut ws,
        json!({"type":"register","protocolVersion":1,"deviceName":"Mac","environmentId":"local"}),
    )
    .await;
    let ack = read_json(&mut ws).await;
    assert_eq!(ack["type"], "registered");
    assert_eq!(ack["protocolVersion"], 1);
    assert_eq!(ack["publicHttpBaseUrl"], format!("https://{host}"));
    assert_eq!(ack["publicWsBaseUrl"], format!("wss://{host}"));
    assert_eq!(ack["heartbeatIntervalSec"], relay.heartbeat_secs);
    ws
}

async fn open_notice(control: &mut Ws) -> Value {
    let notice = read_json(control).await;
    assert_eq!(notice["type"], "open_stream");
    assert!(!notice["streamId"].as_str().unwrap().is_empty());
    assert!(!notice["streamToken"].as_str().unwrap().is_empty());
    assert!(notice["expiresAtMs"].as_u64().unwrap() > 0);
    notice
}

fn data_path(notice: &Value) -> String {
    format!("/_relay/stream/{}", notice["streamId"].as_str().unwrap())
}

async fn claim(relay: &TestRelay, host: &str, notice: &Value) -> Ws {
    ws_connect(
        &relay.endpoint,
        host,
        &data_path(notice),
        notice["streamToken"].as_str(),
    )
    .await
    .unwrap()
}

async fn denied(endpoint: &Endpoint, host: &str, path: &str, token: Option<&str>) {
    match ws_connect(endpoint, host, path, token).await {
        Err(WsError::Http(response)) => assert!(response.status().is_client_error()),
        _ => panic!("expected an HTTP rejection before the WebSocket upgrade"),
    }
}

async fn request(
    endpoint: &Endpoint,
    host: &str,
    method: &str,
    path: &str,
    headers: &str,
) -> Vec<u8> {
    within(async {
        let mut tcp = endpoint.connect().await;
        tcp.write_all(
            format!(
                "{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n{headers}\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let mut bytes = Vec::new();
        tcp.read_to_end(&mut bytes).await.unwrap();
        bytes
    })
    .await
}

fn status(response: &[u8]) -> u16 {
    assert!(response.starts_with(b"HTTP/1.1 "));
    std::str::from_utf8(&response[9..12])
        .unwrap()
        .parse()
        .unwrap()
}

fn body(message: &[u8]) -> &[u8] {
    let head_end = message
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap();
    &message[head_end + 4..]
}

fn head(message: &[u8]) -> &str {
    let head_end = message
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap();
    std::str::from_utf8(&message[..head_end]).unwrap()
}

fn header_values<'a>(head: &'a str, name: &str) -> Vec<&'a str> {
    head.lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .filter(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim())
        .collect()
}

async fn read_data_until(ws: &mut Ws, bytes: &mut Vec<u8>, needle: &[u8]) {
    within(async {
        while !bytes.windows(needle.len()).any(|part| part == needle) {
            match ws.next().await.unwrap().unwrap() {
                Message::Binary(chunk) => bytes.extend_from_slice(&chunk),
                Message::Ping(_) => ws.flush().await.unwrap(),
                Message::Pong(_) => (),
                frame => panic!("unexpected data frame: {frame:?}"),
            }
        }
    })
    .await;
}

async fn backend_reply(ws: &mut Ws, payload: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    within(ws.send(Message::binary(response.into_bytes())))
        .await
        .unwrap();
}

#[tokio::test]
async fn registration_is_host_bound_and_answers_both_kinds_of_ping() {
    let relay = TestRelay::new().await;
    assert_eq!(relay.health().await, json!({"ok":true,"onlineDevices":0}));
    denied(&relay.endpoint, MAC_A, "/_relay/control", None).await;
    denied(&relay.endpoint, MAC_A, "/_relay/control", Some(TOKEN_B)).await;
    denied(&relay.endpoint, MAC_B, "/_relay/control", Some(TOKEN_A)).await;

    let mut control = control(&relay, MAC_A, TOKEN_A).await;
    assert_eq!(relay.health().await["onlineDevices"], 1);
    send_json(&mut control, json!({"type":"ping"})).await;
    assert_eq!(read_json(&mut control).await["type"], "pong");
    send_json(&mut control, json!({"type":"ping","ts":123456})).await;
    assert_eq!(
        read_json(&mut control).await,
        json!({"type":"pong","ts":123456})
    );
    send_json(&mut control, json!({"type":"pong"})).await;
    control
        .send(Message::Ping(vec![1, 2, 3].into()))
        .await
        .unwrap();
    assert_eq!(
        within(control.next()).await.unwrap().unwrap(),
        Message::Pong(vec![1, 2, 3].into())
    );
}

#[tokio::test]
async fn routes_each_host_to_its_own_mac_and_preserves_paths() {
    let relay = TestRelay::new().await;
    let mut a = control(&relay, MAC_A, TOKEN_A).await;
    let mut b = control(&relay, MAC_B, TOKEN_B).await;
    assert_eq!(relay.health().await["onlineDevices"], 2);
    let first = relay.get("MAC-A.TeSt:8443", "/.well-known/t3/environment");
    let second = relay.get(MAC_B, "/api/preview/file%20name?tag=a%2Fb&x=1");
    for (control, host, path, label) in [
        (&mut a, MAC_A, "/.well-known/t3/environment", "from A"),
        (
            &mut b,
            MAC_B,
            "/api/preview/file%20name?tag=a%2Fb&x=1",
            "from B",
        ),
    ] {
        let notice = open_notice(control).await;
        let mut data = claim(&relay, host, &notice).await;
        let mut bytes = Vec::new();
        read_data_until(&mut data, &mut bytes, b"\r\n\r\n").await;
        let head = head(&bytes);
        assert_eq!(head.lines().next().unwrap(), format!("GET {path} HTTP/1.1"));
        let hosts = header_values(head, "Host");
        assert_eq!(hosts.len(), 1);
        assert!(hosts[0].eq_ignore_ascii_case(host));
        assert_eq!(header_values(head, "X-Forwarded-Proto"), vec!["https"]);
        backend_reply(&mut data, label).await;
    }
    assert_eq!(body(&within(first).await.unwrap()), b"from A");
    assert_eq!(body(&within(second).await.unwrap()), b"from B");
}

#[tokio::test]
async fn streams_post_and_response_before_either_body_is_complete() {
    let relay = TestRelay::new().await;
    let mut control = control(&relay, MAC_A, TOKEN_A).await;
    let mut client = relay.endpoint.connect().await;
    let headers = format!(
        "POST /oauth/token?key=x%2By HTTP/1.1\r\nHost: {MAC_A}\r\n\
         Connection: keep-alive\r\nContent-Length: 12\r\n\
         Origin: https://app.test\r\nAuthorization: DPoP test-access\r\nDPoP: proof\r\n\
         Forwarded: proto=http;for=attacker\r\nX-Forwarded-For: attacker\r\n\
         X-Forwarded-Proto: http\r\nx-forwarded-proto: ftp\r\n\r\nfirst-"
    );
    client.write_all(headers.as_bytes()).await.unwrap();
    let notice = open_notice(&mut control).await;
    let mut data = claim(&relay, MAC_A, &notice).await;

    let mut received = Vec::new();
    read_data_until(&mut data, &mut received, b"first-").await;
    let headers = head(&received);
    assert_eq!(
        headers.lines().next(),
        Some("POST /oauth/token?key=x%2By HTTP/1.1")
    );
    assert_eq!(header_values(headers, "Connection"), vec!["close"]);
    assert_eq!(header_values(headers, "Host"), vec![MAC_A]);
    assert_eq!(header_values(headers, "Origin"), vec!["https://app.test"]);
    assert_eq!(
        header_values(headers, "Authorization"),
        vec!["DPoP test-access"]
    );
    assert_eq!(header_values(headers, "DPoP"), vec!["proof"]);
    assert!(header_values(headers, "Forwarded").is_empty());
    assert!(header_values(headers, "X-Forwarded-For").is_empty());
    assert_eq!(header_values(headers, "X-Forwarded-Proto"), vec!["https"]);

    client.write_all(b"second").await.unwrap();
    read_data_until(&mut data, &mut received, b"first-second").await;
    assert_eq!(body(&received), b"first-second");
    data.send(Message::binary(
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nAccess-Control-Allow-Origin: https://app.test\r\n\r\nfirst".to_vec(),
    ))
    .await
    .unwrap();
    let mut response = Vec::new();
    within(async {
        let mut part = [0_u8; 1024];
        while !response.ends_with(b"first") {
            let n = client.read(&mut part).await.unwrap();
            assert_ne!(n, 0);
            response.extend_from_slice(&part[..n]);
        }
    })
    .await;
    assert_eq!(status(&response), 200);
    assert_eq!(
        header_values(head(&response), "Access-Control-Allow-Origin"),
        vec!["https://app.test"]
    );
    data.send(Message::binary(b"-last".to_vec())).await.unwrap();
    let mut tail = [0; 5];
    within(client.read_exact(&mut tail)).await.unwrap();
    assert_eq!(&tail, b"-last");
}

#[tokio::test]
async fn forwards_cors_preflight_and_backend_status() {
    let relay = TestRelay::new().await;
    let mut control = control(&relay, MAC_A, TOKEN_A).await;
    let endpoint = relay.endpoint.clone();
    let client = tokio::spawn(async move {
        request(&endpoint, MAC_A, "OPTIONS", "/api/preview/item",
            "Origin: https://app.test\r\nAccess-Control-Request-Method: POST\r\nAccess-Control-Request-Headers: dpop,authorization\r\n").await
    });
    let notice = open_notice(&mut control).await;
    let mut data = claim(&relay, MAC_A, &notice).await;
    let mut bytes = Vec::new();
    read_data_until(&mut data, &mut bytes, b"\r\n\r\n").await;
    let headers = head(&bytes);
    assert_eq!(
        headers.lines().next(),
        Some("OPTIONS /api/preview/item HTTP/1.1")
    );
    assert_eq!(header_values(headers, "Origin"), vec!["https://app.test"]);
    assert_eq!(
        header_values(headers, "Access-Control-Request-Method"),
        vec!["POST"]
    );
    assert_eq!(
        header_values(headers, "Access-Control-Request-Headers"),
        vec!["dpop,authorization"]
    );
    data.send(Message::binary(b"HTTP/1.1 204 No Content\r\nAccess-Control-Allow-Origin: https://app.test\r\nAccess-Control-Allow-Methods: POST\r\n\r\n".to_vec())).await.unwrap();
    let response = within(client).await.unwrap();
    assert_eq!(status(&response), 204);
    assert_eq!(
        header_values(head(&response), "Access-Control-Allow-Methods"),
        vec!["POST"]
    );
}

#[tokio::test]
async fn stream_tokens_are_host_bound_single_use_and_count_toward_capacity() {
    let relay = TestRelay::new().await;
    let mut control = control(&relay, MAC_A, TOKEN_A).await;
    let pending = relay.get(MAC_A, "/held");
    let notice = open_notice(&mut control).await;
    let path = data_path(&notice);
    denied(&relay.endpoint, MAC_A, &path, None).await;
    denied(&relay.endpoint, MAC_A, &path, Some(TOKEN_A)).await;
    denied(
        &relay.endpoint,
        MAC_B,
        &path,
        notice["streamToken"].as_str(),
    )
    .await;
    assert_eq!(status(&relay.get(MAC_A, "/full").await.unwrap()), 503);
    let mut data = claim(&relay, MAC_A, &notice).await;
    denied(
        &relay.endpoint,
        MAC_A,
        &path,
        notice["streamToken"].as_str(),
    )
    .await;
    let mut buf = Vec::new();
    read_data_until(&mut data, &mut buf, b"\r\n\r\n").await;
    assert_eq!(status(&relay.get(MAC_A, "/still-full").await.unwrap()), 503);
    backend_reply(&mut data, "done").await;
    assert_eq!(body(&within(pending).await.unwrap()), b"done");

    for (reason, expected) in [("capacity_exceeded", 503), ("busy", 502)] {
        let next = relay.get(MAC_A, "/next");
        let notice = open_notice(&mut control).await;
        send_json(
            &mut control,
            json!({"type":"stream_rejected","streamId": notice["streamId"],"reason":reason}),
        )
        .await;
        assert_eq!(status(&within(next).await.unwrap()), expected);
    }
}

#[tokio::test]
async fn timeout_invalidates_token_and_releases_capacity() {
    let relay = TestRelay::with_intervals(1, 25).await;
    let mut control = control(&relay, MAC_A, TOKEN_A).await;
    let pending = relay.get(MAC_A, "/timeout");
    let notice = open_notice(&mut control).await;
    let expired = within(pending).await.unwrap();
    assert!((500..600).contains(&status(&expired)));
    denied(
        &relay.endpoint,
        MAC_A,
        &data_path(&notice),
        notice["streamToken"].as_str(),
    )
    .await;
    let next = relay.get(MAC_A, "/works-after-timeout");
    let fresh = open_notice(&mut control).await;
    let mut data = claim(&relay, MAC_A, &fresh).await;
    backend_reply(&mut data, "ready").await;
    assert_eq!(body(&within(next).await.unwrap()), b"ready");
}

#[tokio::test]
async fn replacing_then_disconnecting_a_control_retires_only_its_own_streams() {
    let relay = TestRelay::new().await;
    assert_eq!(status(&relay.get("unknown.test", "/").await.unwrap()), 404);
    assert_eq!(status(&relay.get(MAC_A, "/").await.unwrap()), 502);

    let mut old = control(&relay, MAC_A, TOKEN_A).await;
    let pending = relay.get(MAC_A, "/old");
    let stale = open_notice(&mut old).await;
    let mut current = control(&relay, MAC_A, TOKEN_A).await;
    assert_eq!(status(&within(pending).await.unwrap()), 502);
    denied(
        &relay.endpoint,
        MAC_A,
        &data_path(&stale),
        stale["streamToken"].as_str(),
    )
    .await;
    drop(old);
    assert_eq!(relay.health().await["onlineDevices"], 1);

    let pending = relay.get(MAC_A, "/current");
    let _fresh = open_notice(&mut current).await;
    current.close(None).await.unwrap();
    assert_eq!(status(&within(pending).await.unwrap()), 502);
    assert_eq!(relay.health().await["onlineDevices"], 0);
    assert_eq!(status(&relay.get(MAC_A, "/offline").await.unwrap()), 502);
}

#[tokio::test]
async fn invalid_registration_leaves_the_online_device_in_place() {
    let relay = TestRelay::new().await;
    let mut current = control(&relay, MAC_A, TOKEN_A).await;
    let mut invalid = ws_connect(&relay.endpoint, MAC_A, "/_relay/control", Some(TOKEN_A))
        .await
        .unwrap();
    send_json(
        &mut invalid,
        json!({"type":"register","protocolVersion":2,"deviceName":"bad","environmentId":"local"}),
    )
    .await;
    assert!(matches!(
        within(invalid.next()).await,
        Some(Ok(Message::Close(_))) | None
    ));
    send_json(&mut current, json!({"type":"ping"})).await;
    assert_eq!(read_json(&mut current).await["type"], "pong");
    assert_eq!(relay.health().await["onlineDevices"], 1);
}

#[tokio::test]
async fn explicit_device_disconnect_closes_attached_data_and_fails_client() {
    let relay = TestRelay::new().await;
    let mut control = control(&relay, MAC_A, TOKEN_A).await;
    let client = relay.get(MAC_A, "/attached");
    let notice = open_notice(&mut control).await;
    let mut data = claim(&relay, MAC_A, &notice).await;
    let mut request = Vec::new();
    read_data_until(&mut data, &mut request, b"\r\n\r\n").await;
    send_json(
        &mut control,
        json!({"type":"disconnect","reason":"shutdown"}),
    )
    .await;
    assert!(matches!(
        within(data.next()).await,
        Some(Ok(Message::Close(_))) | None
    ));
    assert_eq!(status(&within(client).await.unwrap()), 502);
    assert_eq!(relay.health().await["onlineDevices"], 0);
}

#[tokio::test]
async fn pipelined_requests_use_separate_streams_and_keep_chunk_boundaries_valid() {
    let relay = TestRelay::new().await;
    let mut control = control(&relay, MAC_A, TOKEN_A).await;
    let mut client = relay.endpoint.connect().await;
    client.write_all(format!(
        "POST /one HTTP/1.1\r\nHost: {MAC_A}\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n\
        GET /two HTTP/1.1\r\nHost: {MAC_A}\r\nConnection: close\r\n\r\n"
    ).as_bytes()).await.unwrap();

    let first = open_notice(&mut control).await;
    let mut data = claim(&relay, MAC_A, &first).await;
    let mut received = Vec::new();
    read_data_until(&mut data, &mut received, b"0\r\n\r\n").await;
    assert_eq!(head(&received).lines().next(), Some("POST /one HTTP/1.1"));
    assert_eq!(header_values(head(&received), "Connection"), vec!["close"]);
    assert_eq!(
        header_values(head(&received), "Transfer-Encoding"),
        vec!["chunked"]
    );
    assert_eq!(body(&received), b"1\r\nx\r\n0\r\n\r\n");
    data.send(Message::binary(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1\r\ny\r\n0\r\n\r\n".to_vec())).await.unwrap();

    let second = open_notice(&mut control).await;
    assert_ne!(first["streamId"], second["streamId"]);
    let mut data = claim(&relay, MAC_A, &second).await;
    received.clear();
    read_data_until(&mut data, &mut received, b"\r\n\r\n").await;
    assert_eq!(head(&received).lines().next(), Some("GET /two HTTP/1.1"));
    assert_eq!(body(&received), b"");
    backend_reply(&mut data, "two").await;

    let mut responses = Vec::new();
    within(client.read_to_end(&mut responses)).await.unwrap();
    assert_eq!(status(&responses), 200);
    assert_eq!(
        header_values(head(&responses), "Transfer-Encoding"),
        vec!["chunked"]
    );
    let second = body(&responses)
        .strip_prefix(b"1\r\ny\r\n0\r\n\r\n")
        .unwrap();
    assert_eq!(status(second), 200);
    assert_eq!(body(second), b"two");
}

#[tokio::test]
async fn a_silent_connector_is_retired_while_a_stream_is_waiting() {
    let relay = TestRelay::with_intervals(12, 1).await;
    let mut control = control(&relay, MAC_A, TOKEN_A).await;
    let pending = relay.get(MAC_A, "/waiting");
    open_notice(&mut control).await;
    assert!(matches!(
        within(control.next()).await,
        Some(Ok(Message::Ping(_)))
    ));
    // Leave the automatic pong queued to simulate an unresponsive connector.
    assert_eq!(status(&within(pending).await.unwrap()), 502);
    assert_eq!(relay.health().await["onlineDevices"], 0);
}
