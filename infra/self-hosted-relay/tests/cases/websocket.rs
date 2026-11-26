use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio_tungstenite::{
    accept_hdr_async, client_async,
    tungstenite::{
        client::IntoClientRequest,
        handshake::server::{Request, Response},
    },
};

#[tokio::test]
async fn websocket_upgrade_forwards_handshake_and_bidirectional_frames() {
    const PATH: &str = "/ws?wsTicket=test-ticket&dpop=test-proof";
    const ORIGIN: &str = "https://web.test";
    let relay = TestRelay::new().await;
    let mut control = control(&relay, MAC_A, TOKEN_A).await;
    let (backend_io, mut wire) = duplex(8192);
    let payload = vec![0xa7; 48 * 1024];

    let connector = async {
        let notice = open_notice(&mut control).await;
        let mut data = claim(&relay, MAC_A, &notice).await;
        let mut buffer = [0; 4096];
        loop {
            tokio::select! {
                message = data.next() => match message {
                    Some(Ok(Message::Binary(bytes))) => wire.write_all(&bytes).await.unwrap(),
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                    None | Some(Ok(Message::Close(_))) => break,
                    _ => panic!("expected binary data on the connector stream"),
                },
                read = wire.read(&mut buffer) => {
                    let len = read.unwrap();
                    if len == 0 {
                        data.close(None).await.unwrap();
                        break;
                    }
                    data.send(Message::binary(buffer[..len].to_vec())).await.unwrap();
                }
            }
        }
    };

    let backend = async {
        #[expect(
            clippy::result_large_err,
            reason = "Tungstenite specifies the handshake error type"
        )]
        let handshake = |req: &Request, mut response: Response| {
            assert_eq!(req.method(), "GET");
            assert_eq!(req.uri().path_and_query().unwrap().as_str(), PATH);
            let headers = req.headers();
            assert_eq!(headers["host"], MAC_A);
            assert_eq!(headers["origin"], ORIGIN);
            assert_eq!(headers["authorization"], "Bearer client-auth");
            assert_eq!(headers["dpop"], "client-proof");
            assert_eq!(headers["x-forwarded-proto"], "https");
            assert_eq!(headers["sec-websocket-protocol"], "t3");
            response
                .headers_mut()
                .insert("sec-websocket-protocol", "t3".parse().unwrap());
            Ok(response)
        };
        let mut ws = accept_hdr_async(backend_io, handshake).await.unwrap();
        ws.send(Message::text("ready")).await.unwrap();
        let data = ws.next().await.unwrap().unwrap();
        assert_eq!(data, Message::binary(payload.clone()));
        ws.send(data).await.unwrap();
        assert!(matches!(ws.next().await, Some(Ok(Message::Close(_)))));
        ws.flush().await.unwrap();
    };

    let client = async {
        let mut request = format!("ws://{MAC_A}{PATH}").into_client_request().unwrap();
        let headers = request.headers_mut();
        headers.insert("origin", ORIGIN.parse().unwrap());
        headers.insert("authorization", "Bearer client-auth".parse().unwrap());
        headers.insert("dpop", "client-proof".parse().unwrap());
        headers.insert("x-forwarded-proto", "http".parse().unwrap());
        headers.insert("sec-websocket-protocol", "t3".parse().unwrap());
        let tcp = relay.endpoint.connect().await;
        let (mut ws, response) = client_async(request, tcp).await.unwrap();
        assert_eq!(response.status(), 101);
        assert_eq!(response.headers()["sec-websocket-protocol"], "t3");
        assert_eq!(ws.next().await.unwrap().unwrap(), Message::text("ready"));
        ws.send(Message::binary(payload.clone())).await.unwrap();
        assert_eq!(
            ws.next().await.unwrap().unwrap(),
            Message::binary(payload.clone())
        );
        ws.close(None).await.unwrap();
        assert!(matches!(ws.next().await, Some(Ok(Message::Close(_)))));
    };

    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        tokio::join!(connector, backend, client);
    })
    .await
    .expect("WebSocket forwarding stalled");
}
