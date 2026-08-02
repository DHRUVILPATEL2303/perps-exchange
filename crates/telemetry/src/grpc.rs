use std::task::{Context, Poll};
use std::time::Instant;
use std::future::Future;
use std::pin::Pin;

#[derive(Clone)]
pub struct GrpcLoggingMiddleware<S> {
    inner: S,
}

impl<S> GrpcLoggingMiddleware<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

#[derive(Clone)]
pub struct GrpcLoggingLayer;

impl<S> tower::Layer<S> for GrpcLoggingLayer {
    type Service = GrpcLoggingMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        GrpcLoggingMiddleware::new(service)
    }
}

impl<S, ReqBody, ResBody> tower::Service<http::Request<ReqBody>> for GrpcLoggingMiddleware<S>
where
    S: tower::Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let method = req.uri().path().to_string();
        let start = Instant::now();

        let fut = inner.call(req);

        Box::pin(async move {
            let res = fut.await;
            let duration = start.elapsed();

            match &res {
                Ok(response) => {
                    let status = response.headers()
                        .get("grpc-status")
                        .and_then(|val| val.to_str().ok())
                        .unwrap_or("0"); // default to 0 (gRPC OK)

                    tracing::info!(
                        target: "grpc_server",
                        method = %method,
                        status = %status,
                        duration_ms = %duration.as_millis(),
                        "gRPC Request completed: method={} status={} duration={}ms",
                        method, status, duration.as_millis()
                    );
                }
                Err(_) => {
                    tracing::error!(
                        target: "grpc_server",
                        method = %method,
                        duration_ms = %duration.as_millis(),
                        "gRPC Request failed: method={} duration={}ms",
                        method, duration.as_millis()
                    );
                }
            }
            res
        })
    }
}
