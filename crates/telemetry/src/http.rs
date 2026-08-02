use actix_web::{
    Error, HttpResponse, Responder,
    dev::{Service, ServiceRequest, ServiceResponse, Transform},
    get,
};
use futures_util::future::LocalBoxFuture;
use std::future::{Ready, ready};
use std::time::Instant;

pub struct HttpMetrics;

impl<S, B> Transform<S, ServiceRequest> for HttpMetrics
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = HttpMetricsMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(HttpMetricsMiddleware { service }))
    }
}

pub struct HttpMetricsMiddleware<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for HttpMetricsMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let path = req.path().to_string();

        if path == "/metrics" {
            let fut = self.service.call(req);
            return Box::pin(async move { fut.await });
        }

        let method = req.method().to_string();
        let start = Instant::now();
        let fut = self.service.call(req);

        Box::pin(async move {
            let res = fut.await?;
            let duration = start.elapsed();
            let status = res.status().as_u16().to_string();

            // Record metrics
            crate::metrics::HTTP_REQUESTS_TOTAL
                .with_label_values(&[&path, &method, &status])
                .inc();
            crate::metrics::HTTP_REQUEST_DURATION_SECONDS
                .with_label_values(&[&path, &method])
                .observe(duration.as_secs_f64());

            Ok(res)
        })
    }
}

#[get("/metrics")]
pub async fn metrics_handler() -> impl Responder {
    let metrics = crate::metrics::gather_metrics();
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4")
        .body(metrics)
}

pub fn spawn_metrics_server(port: u16) {
    std::thread::spawn(move || {
        let sys = actix_web::rt::System::new();
        let _ = sys.block_on(async move {
            let server =
                actix_web::HttpServer::new(|| actix_web::App::new().service(metrics_handler))
                    .bind(("0.0.0.0", port));

            match server {
                Ok(s) => {
                    tracing::info!("Telemetry Metrics server started on port {}", port);
                    let _ = s.run().await;
                }
                Err(e) => {
                    tracing::error!("Failed to bind metrics server to port {}: {:?}", port, e);
                }
            }
        });
    });
}
