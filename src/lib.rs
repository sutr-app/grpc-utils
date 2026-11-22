use anyhow::{Context, Result};
use axum::http::{HeaderValue, Method};
use prost::bytes::{Buf, BufMut};
use std::convert::Infallible;
use std::{sync::Arc, time::Duration};
use tokio::sync::{RwLock, RwLockReadGuard};
use tonic::body::Body;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::server::NamedService;
use tonic::service::LayerExt as _;
use tonic::transport::{ClientTlsConfig, Endpoint};
use tonic_web::GrpcWebService;
use tower::Service;
use tower::util::Either;
use tower_http::cors::{AllowHeaders, AllowOrigin, Cors, CorsLayer, ExposeHeaders};

pub mod reflection;

#[derive(Debug, Clone)]
pub struct GrpcConnection {
    endpoint: tonic::transport::Endpoint,
    channel: Arc<RwLock<tonic::transport::Channel>>,
}

impl GrpcConnection {
    pub async fn new(addr: String, request_timeout: Option<Duration>) -> Result<Self> {
        let use_tls: bool = addr.as_str().starts_with("https://");
        let endpoint = if let Some(timeout) = request_timeout {
            Endpoint::try_from(addr.clone())?.timeout(timeout)
        } else {
            Endpoint::try_from(addr.clone())?
        };
        let endpoint = if use_tls {
            endpoint.tls_config(ClientTlsConfig::new().with_enabled_roots())?
        } else {
            endpoint
        };

        let channel =
            Arc::new(RwLock::new(endpoint.connect().await.context(format!(
                "Failed to connect to gRPC server at {}",
                addr
            ))?));
        Ok(Self { endpoint, channel })
    }
    pub async fn create(endpoint: Endpoint, channel: tonic::transport::Channel) -> Result<Self> {
        let channel = Arc::new(RwLock::new(channel));
        Ok(Self { endpoint, channel })
    }

    pub async fn reconnect(&self) -> Result<()> {
        let c = self.channel.clone();
        let mut m = c.write().await;
        *m = self.endpoint.connect().await?;
        Ok(())
    }

    pub async fn read_channel(&self) -> RwLockReadGuard<'_, tonic::transport::Channel> {
        self.channel.read().await
    }
}

// Define a custom codec that passes through raw bytes without additional protobuf encoding
#[derive(Debug, Clone)]
pub struct RawBytesCodec;

impl Default for RawBytesCodec {
    fn default() -> Self {
        RawBytesCodec
    }
}

impl Encoder for RawBytesCodec {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        // Simply write the raw bytes as-is
        buf.reserve(item.len());
        buf.put_slice(&item);
        Ok(())
    }
}

impl Decoder for RawBytesCodec {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        if !buf.has_remaining() {
            return Ok(None);
        }

        // Just copy the entire buffer into a new Vec<u8>
        let bytes = buf.copy_to_bytes(buf.remaining());
        Ok(Some(bytes.to_vec()))
    }
}

impl Codec for RawBytesCodec {
    type Encode = Vec<u8>;
    type Decode = Vec<u8>;
    type Encoder = RawBytesCodec;
    type Decoder = RawBytesCodec;

    fn encoder(&mut self) -> Self::Encoder {
        RawBytesCodec
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawBytesCodec
    }
}

/// Origins allowed to call the gRPC-Web endpoints from a browser.
#[derive(Debug, Clone, Default)]
pub enum GrpcWebCorsOrigins {
    /// No CORS headers are returned, so browsers reject cross-origin calls.
    #[default]
    Disabled,
    /// Every origin is allowed (development only).
    Any,
    /// Only these origins (`scheme://host[:port]`, lowercase) are allowed.
    List(Vec<HeaderValue>),
}

impl GrpcWebCorsOrigins {
    /// Parses a comma separated origin list. Empty means `Disabled`, `*` means `Any`.
    ///
    /// Each origin must be `http(s)://host[:port]` without a path, because browsers send
    /// exactly that form in the `Origin` header and anything else would never match.
    pub fn parse(value: &str) -> Result<Self> {
        let items: Vec<&str> = value
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        match items.as_slice() {
            [] => Ok(Self::Disabled),
            ["*"] => Ok(Self::Any),
            _ if items.contains(&"*") => {
                anyhow::bail!("'*' cannot be combined with other origins: {value}")
            }
            _ => items
                .into_iter()
                .map(Self::parse_origin)
                .collect::<Result<Vec<_>>>()
                .map(Self::List),
        }
    }

    fn parse_origin(origin: &str) -> Result<HeaderValue> {
        // Scheme and host are case-insensitive, and browsers serialize them in lowercase.
        let normalized = origin.to_ascii_lowercase();
        let authority = normalized
            .strip_prefix("http://")
            .or_else(|| normalized.strip_prefix("https://"))
            .with_context(|| format!("origin must start with http:// or https://: {origin}"))?;
        if authority.is_empty()
            || authority.contains(|c: char| c.is_whitespace() || matches!(c, '/' | '?' | '#'))
        {
            anyhow::bail!("origin must be scheme://host[:port] without a path: {origin}");
        }
        HeaderValue::from_str(&normalized).with_context(|| format!("invalid origin: {origin}"))
    }

    fn cors_layer(&self) -> Option<CorsLayer> {
        match self {
            Self::Disabled => None,
            Self::Any => Some(CorsLayer::permissive()),
            // gRPC metadata travels as arbitrary headers (auth tokens, trace context,
            // `*-bin` response metadata), so only the origin is restricted.
            Self::List(origins) => Some(
                CorsLayer::new()
                    .allow_origin(AllowOrigin::list(origins.clone()))
                    .allow_methods([Method::POST])
                    .allow_headers(AllowHeaders::mirror_request())
                    .expose_headers(ExposeHeaders::any())
                    .max_age(Duration::from_secs(24 * 60 * 60)),
            ),
        }
    }
}

pub fn enable_grpc_web<S>(
    service: S,
    origins: &GrpcWebCorsOrigins,
) -> tonic::service::Layered<Either<Cors<GrpcWebService<S>>, GrpcWebService<S>>, S>
where
    S: Service<axum::http::Request<Body>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Response: axum::response::IntoResponse,
    S::Future: Send + 'static,
{
    tower::ServiceBuilder::new()
        .option_layer(origins.cors_layer())
        .layer(tonic_web::GrpcWebLayer::new())
        .into_inner()
        .named_layer(service)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::codec::Codec;

    fn assert_raw_bytes_codec_traits<T>()
    where
        T: Codec<Encode = Vec<u8>, Decode = Vec<u8>> + Clone + Default,
    {
    }

    #[test]
    fn raw_bytes_codec_keeps_vec_byte_contract() {
        assert_raw_bytes_codec_traits::<RawBytesCodec>();

        let mut codec = RawBytesCodec;
        let _encoder = codec.encoder();
        let _decoder = codec.decoder();
    }

    #[tokio::test]
    async fn grpc_connection_create_accepts_lazy_channel() {
        let endpoint = Endpoint::from_static("http://127.0.0.1:50051");
        let channel = endpoint.connect_lazy();
        let connection = GrpcConnection::create(endpoint, channel).await.unwrap();

        let _channel = connection.read_channel().await;
    }

    #[derive(Clone)]
    struct EchoService;

    impl NamedService for EchoService {
        const NAME: &'static str = "test.Echo";
    }

    impl Service<axum::http::Request<Body>> for EchoService {
        type Response = axum::http::Response<Body>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Infallible>>;

        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: axum::http::Request<Body>) -> Self::Future {
            std::future::ready(Ok(axum::http::Response::new(Body::empty())))
        }
    }

    const GRPC_WEB_REQUEST_HEADERS: &str = "x-grpc-web,content-type";

    async fn cors_response_headers(
        origins: &str,
        method: Method,
        origin: &str,
        request_headers: &str,
    ) -> axum::http::HeaderMap {
        use tower::ServiceExt;
        let mut svc = enable_grpc_web(EchoService, &GrpcWebCorsOrigins::parse(origins).unwrap());
        let mut req = axum::http::Request::builder()
            .method(method.clone())
            .uri("/test.Echo/Call")
            .header("origin", origin)
            .header("content-type", "application/grpc-web+proto");
        if method == Method::OPTIONS {
            req = req
                .header("access-control-request-method", "POST")
                .header("access-control-request-headers", request_headers);
        }
        let res = ServiceExt::<axum::http::Request<Body>>::ready(&mut svc)
            .await
            .unwrap()
            .call(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        res.headers().clone()
    }

    async fn allow_origin_header(origins: &str, method: Method, origin: &str) -> Option<String> {
        cors_response_headers(origins, method, origin, GRPC_WEB_REQUEST_HEADERS)
            .await
            .get("access-control-allow-origin")
            .map(|v| v.to_str().unwrap().to_string())
    }

    fn list_of(value: &str) -> Vec<HeaderValue> {
        match GrpcWebCorsOrigins::parse(value).unwrap() {
            GrpcWebCorsOrigins::List(v) => v,
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_handles_empty_wildcard_and_lists() {
        assert!(matches!(
            GrpcWebCorsOrigins::parse("").unwrap(),
            GrpcWebCorsOrigins::Disabled
        ));
        assert!(matches!(
            GrpcWebCorsOrigins::parse("  , ").unwrap(),
            GrpcWebCorsOrigins::Disabled
        ));
        assert!(matches!(
            GrpcWebCorsOrigins::parse(" * ").unwrap(),
            GrpcWebCorsOrigins::Any
        ));
        assert_eq!(
            list_of(" http://a.example , https://b.example:8443, "),
            vec![
                HeaderValue::from_static("http://a.example"),
                HeaderValue::from_static("https://b.example:8443"),
            ]
        );
    }

    #[test]
    fn parse_lowercases_origins_to_match_browser_serialization() {
        assert_eq!(
            list_of("HTTP://A.Example:5173"),
            vec![HeaderValue::from_static("http://a.example:5173")]
        );
    }

    #[test]
    fn parse_rejects_invalid_values() {
        for value in [
            "*,http://a.example",
            "http://a.example,*",
            "http://a\nb",
            // A browser never sends a path or trailing slash in Origin, so these would never match.
            "http://a.example/",
            "http://a.example/admin",
            "http://a.example?x=1",
            "http://a.example#x",
            "a.example",
            "ftp://a.example",
            "http://",
        ] {
            assert!(
                GrpcWebCorsOrigins::parse(value).is_err(),
                "expected error for {value:?}"
            );
        }
    }

    #[tokio::test]
    async fn allowed_origin_gets_cors_headers_on_preflight_and_post() {
        for method in [Method::OPTIONS, Method::POST] {
            assert_eq!(
                allow_origin_header("http://a.example", method, "http://a.example")
                    .await
                    .as_deref(),
                Some("http://a.example")
            );
        }
    }

    #[tokio::test]
    async fn allowed_origin_preflight_accepts_custom_metadata_headers() {
        let headers = cors_response_headers(
            "http://a.example",
            Method::OPTIONS,
            "http://a.example",
            "x-grpc-web,content-type,jobworkerp-auth,traceparent",
        )
        .await;
        let allowed = headers
            .get("access-control-allow-headers")
            .expect("allow-headers must be returned")
            .to_str()
            .unwrap()
            .to_ascii_lowercase();
        for name in [
            "x-grpc-web",
            "content-type",
            "jobworkerp-auth",
            "traceparent",
        ] {
            assert!(allowed.contains(name), "{name} not allowed: {allowed}");
        }
    }

    #[tokio::test]
    async fn allowed_origin_response_exposes_metadata_headers() {
        let headers =
            cors_response_headers("http://a.example", Method::POST, "http://a.example", "").await;
        assert_eq!(
            headers
                .get("access-control-expose-headers")
                .map(|v| v.to_str().unwrap()),
            Some("*")
        );
    }

    #[tokio::test]
    async fn disallowed_origin_gets_no_cors_header() {
        for method in [Method::OPTIONS, Method::POST] {
            assert_eq!(
                allow_origin_header("http://a.example", method, "http://evil.example").await,
                None
            );
        }
    }

    #[tokio::test]
    async fn disabled_does_not_allow_any_origin() {
        for method in [Method::OPTIONS, Method::POST] {
            assert_eq!(
                allow_origin_header("", method, "http://evil.example").await,
                None
            );
        }
    }

    #[tokio::test]
    async fn wildcard_allows_every_origin() {
        assert!(
            allow_origin_header("*", Method::POST, "http://any.example")
                .await
                .is_some()
        );
    }
}
