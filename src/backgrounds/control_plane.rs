use crate::backgrounds::Stats;
use crate::config::Scenario;
use crate::domain::ProxyError;
use crate::flows::Flow;
use arc_swap::ArcSwapOption;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Clone)]
struct AppState {
    stats: Arc<Stats>,
    reload: Arc<ArcSwapOption<Flow>>,
}

/// Binds the control-plane listener, reporting the address actually bound
/// (relevant when `addr` uses an ephemeral port).
pub async fn bind(addr: SocketAddr) -> Result<(TcpListener, SocketAddr), ProxyError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| ProxyError::Bind { addr, source })?;
    let local_addr = listener.local_addr().map_err(ProxyError::Io)?;
    Ok((listener, local_addr))
}

pub async fn run(
    listener: TcpListener,
    stats: Arc<Stats>,
    reload: Arc<ArcSwapOption<Flow>>,
) -> Result<(), ProxyError> {
    let app = router(stats, reload);
    axum::serve(listener, app).await.map_err(ProxyError::Io)
}

pub async fn serve(
    addr: SocketAddr,
    stats: Arc<Stats>,
    reload: Arc<ArcSwapOption<Flow>>,
) -> Result<(), ProxyError> {
    let (listener, _local_addr) = bind(addr).await?;
    run(listener, stats, reload).await
}

fn router(stats: Arc<Stats>, reload: Arc<ArcSwapOption<Flow>>) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .route("/reload", post(reload_scenario))
        .with_state(AppState { stats, reload })
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.stats.snapshot())
}

async fn reload_scenario(State(state): State<AppState>, body: String) -> impl IntoResponse {
    match parse_flow(&body) {
        Ok(flow) => {
            state.reload.store(Some(Arc::new(flow)));
            (StatusCode::OK, String::new())
        }
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()),
    }
}

fn parse_flow(body: &str) -> Result<Flow, ProxyError> {
    let scenario: Scenario =
        toml::from_str(body).map_err(|err| ProxyError::Config(err.to_string()))?;
    scenario.build_flow()
}
