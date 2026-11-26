use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, Uri, Version,
    header::{
        AUTHORIZATION, CONNECTION, CONTENT_LENGTH, HOST, ORIGIN, TE, TRANSFER_ENCODING, UPGRADE,
    },
};
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Frame, Incoming, SizeHint},
    client::conn::http1,
    upgrade::{self, OnUpgrade},
};
use hyper_util::rt::TokioIo;
use tokio::io;

use crate::{
    Response, error,
    state::{Lease, OpenedStream},
};

pub(crate) async fn forward(
    mut req: Request<Incoming>,
    scheme: &str,
    stream: OpenedStream,
) -> Response {
    let is_connect = req.method() == Method::CONNECT;
    if is_connect
        && !req
            .uri()
            .to_string()
            .as_bytes()
            .eq_ignore_ascii_case(req.headers()[HOST].as_bytes())
    {
        return error(StatusCode::BAD_REQUEST);
    }
    let accepts_trailers = req
        .headers()
        .get_all(TE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|list| list.split(','))
        .any(|token| token.trim().eq_ignore_ascii_case("trailers"));
    let Ok(is_upgrade) = clean_connection(req.headers_mut()) else {
        return error(StatusCode::BAD_REQUEST);
    };
    let client_upgrade = (is_upgrade || is_connect).then(|| upgrade::on(&mut req));
    let headers = req.headers_mut();
    strip_forwarded(headers);
    let Ok(scheme) = HeaderValue::from_str(scheme) else {
        return error(StatusCode::INTERNAL_SERVER_ERROR);
    };
    headers.insert("x-forwarded-proto", scheme);
    if accepts_trailers && !is_upgrade {
        headers.insert(TE, HeaderValue::from_static("trailers"));
        headers.insert(CONNECTION, HeaderValue::from_static("close, TE"));
    } else if !is_upgrade {
        headers.insert(CONNECTION, HeaderValue::from_static("close"));
    }
    if req.uri().scheme().is_some() {
        *req.uri_mut() = req
            .uri()
            .path_and_query()
            .cloned()
            .map(Uri::from)
            .unwrap_or_default();
    }
    *req.version_mut() = Version::HTTP_11;
    let req = req.map(|body| {
        body.map_frame(|mut frame| {
            if let Some(headers) = frame.trailers_mut() {
                strip_forwarded(headers);
            }
            frame
        })
    });

    let OpenedStream { io, lease } = stream;
    let Ok((mut upstream, connection)) = http1::handshake(TokioIo::new(io)).await else {
        return error(StatusCode::BAD_GATEWAY);
    };
    tokio::spawn(connection.with_upgrades());
    let Ok(mut res) = upstream.send_request(req).await else {
        return error(StatusCode::BAD_GATEWAY);
    };
    let Ok(upstream_upgrade) = clean_connection(res.headers_mut()) else {
        return error(StatusCode::BAD_GATEWAY);
    };
    if res.status() == StatusCode::SWITCHING_PROTOCOLS || (is_connect && res.status().is_success())
    {
        let Some(client) = client_upgrade else {
            return error(StatusCode::BAD_GATEWAY);
        };
        if res.status() == StatusCode::SWITCHING_PROTOCOLS && !(is_upgrade && upstream_upgrade) {
            return error(StatusCode::BAD_GATEWAY);
        }
        let backend = upgrade::on(&mut res);
        tokio::spawn(async move {
            tokio::select! {
                _ = lease.cancelled() => {},
                _ = bridge_upgraded(client, backend) => {},
            }
        });
        return res.map(BodyExt::boxed_unsync);
    }
    res.map(|body| {
        LeasedBody {
            body,
            _lease: lease,
        }
        .boxed_unsync()
    })
}

fn strip_forwarded(headers: &mut HeaderMap) {
    for name in [
        "forwarded",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-port",
        "x-forwarded-proto",
    ] {
        headers.remove(name);
    }
}

// Each side has its own HTTP connection. Only the negotiated Upgrade crosses both hops.
fn clean_connection(headers: &mut HeaderMap) -> Result<bool, ()> {
    let mut remove = Vec::new();
    let mut upgrade = false;
    for value in headers.get_all(CONNECTION) {
        for token in value
            .to_str()
            .map_err(|_| ())?
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let name = HeaderName::from_bytes(token.as_bytes()).map_err(|_| ())?;
            if name == UPGRADE {
                upgrade = headers.contains_key(UPGRADE);
            } else if matches!(
                name,
                HOST | CONTENT_LENGTH | TRANSFER_ENCODING | ORIGIN | AUTHORIZATION
            ) || name == "dpop"
                || name.as_str().starts_with("sec-websocket-")
            {
                return Err(());
            } else {
                remove.push(name);
            }
        }
    }
    for name in remove {
        headers.remove(name);
    }
    for name in [
        "proxy-connection",
        "keep-alive",
        "proxy-authorization",
        "proxy-authenticate",
        "te",
    ] {
        headers.remove(name);
    }
    if headers.contains_key(TRANSFER_ENCODING) {
        headers.remove(CONTENT_LENGTH);
    }
    if upgrade {
        headers.insert(CONNECTION, HeaderValue::from_static("upgrade"));
    } else {
        headers.remove(CONNECTION);
        headers.remove(UPGRADE);
    }
    Ok(upgrade)
}

async fn bridge_upgraded(client: OnUpgrade, backend: OnUpgrade) {
    let Ok((client, backend)) = tokio::try_join!(client, backend) else {
        return;
    };
    let (mut client_read, mut client_write) = io::split(TokioIo::new(client));
    let (mut backend_read, mut backend_write) = io::split(TokioIo::new(backend));
    // The data WebSocket cannot half-close. Either EOF ends the tunnel.
    tokio::select! {
        _ = io::copy(&mut client_read, &mut backend_write) => {},
        _ = io::copy(&mut backend_read, &mut client_write) => {},
    }
}

struct LeasedBody {
    body: Incoming,
    _lease: Lease,
}

impl Body for LeasedBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, hyper::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
