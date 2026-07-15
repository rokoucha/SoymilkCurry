use std::{collections::HashSet, env, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Serialize;

use crate::{
    config::ChannelConfig,
    tuner::{AppState, OpenError, TunerSnapshot},
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiChannel {
    #[serde(rename = "type")]
    channel_type: String,
    channel: String,
    name: String,
}

impl From<&ChannelConfig> for ApiChannel {
    fn from(channel: &ChannelConfig) -> Self {
        Self {
            channel_type: channel.channel_type.clone(),
            channel: channel.channel.clone(),
            name: channel.name.clone(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiTuner {
    index: usize,
    name: String,
    types: Vec<String>,
    command: String,
    pid: i64,
    users: Vec<ApiTunerUser>,
    is_available: bool,
    is_remote: bool,
    is_free: bool,
    is_using: bool,
    is_fault: bool,
}

impl From<TunerSnapshot> for ApiTuner {
    fn from(tuner: TunerSnapshot) -> Self {
        Self {
            index: tuner.index,
            name: tuner.name,
            types: tuner.types,
            command: tuner.command,
            pid: tuner.pid,
            users: tuner
                .users
                .into_iter()
                .map(|id| ApiTunerUser { id, priority: 0 })
                .collect(),
            is_available: true,
            is_remote: false,
            is_free: tuner.is_free,
            is_using: !tuner.is_free,
            is_fault: false,
        }
    }
}

#[derive(Serialize)]
struct ApiTunerUser {
    id: String,
    priority: i32,
}

#[derive(Serialize)]
struct ApiError {
    code: u16,
    reason: &'static str,
}

type ApiResult = Result<Response, (StatusCode, Json<ApiError>)>;

pub(crate) fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/version", get(version))
        .route("/api/status", get(status))
        .route("/api/tuners", get(tuners))
        .route("/api/channels", get(channels))
        .route("/api/channels/{type}/{channel}", get(channel))
        .route(
            "/api/channels/{type}/{channel}/stream",
            get(channel_stream).head(channel_stream_head),
        )
        .with_state(state)
}

async fn version() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "current": VERSION, "latest": "", "server": "SoymilkCurry" }))
}

async fn status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "time": unix_time_ms(),
        "version": VERSION,
        "process": {
            "arch": env::consts::ARCH,
            "platform": env::consts::OS,
            "pid": std::process::id()
        },
        "streamCount": {
            "tunerDevice": state.stream_count(),
            "tsFilter": 0,
            "decoder": 0
        }
    }))
}

async fn tuners(State(state): State<Arc<AppState>>) -> Json<Vec<ApiTuner>> {
    Json(
        state
            .tuner_snapshots()
            .into_iter()
            .map(ApiTuner::from)
            .collect(),
    )
}

async fn channels(State(state): State<Arc<AppState>>) -> Json<Vec<ApiChannel>> {
    let mut seen = HashSet::new();
    Json(
        state
            .channels()
            .iter()
            .filter(|channel| seen.insert((&channel.channel_type, &channel.channel)))
            .map(ApiChannel::from)
            .collect(),
    )
}

async fn channel(
    State(state): State<Arc<AppState>>,
    Path((channel_type, channel)): Path<(String, String)>,
) -> ApiResult {
    let item = state
        .channel(&channel_type, &channel)
        .ok_or_else(not_found)?;
    Ok(Json(ApiChannel::from(item)).into_response())
}

async fn channel_stream_head(
    State(state): State<Arc<AppState>>,
    Path((channel_type, channel)): Path<(String, String)>,
) -> ApiResult {
    if state.channel(&channel_type, &channel).is_none() {
        return Err(not_found());
    }
    if !state.stream_available(&channel_type, &channel) {
        return Err(unavailable());
    }
    Ok(ts_response(Body::empty(), None))
}

async fn channel_stream(
    State(state): State<Arc<AppState>>,
    Path((channel_type, channel)): Path<(String, String)>,
) -> ApiResult {
    let opened = state
        .open_stream(&channel_type, &channel)
        .map_err(|error| match error {
            OpenError::NotFound => not_found(),
            OpenError::Unavailable => unavailable(),
            OpenError::Spawn => internal_error(),
        })?;
    Ok(ts_response(
        Body::from_stream(opened.stream),
        Some(&opened.user_id),
    ))
}

fn ts_response(body: Body, user_id: Option<&str>) -> Response {
    let mut response = Response::new(body);
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("video/MP2T"));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some(user_id) = user_id
        && let Ok(value) = HeaderValue::from_str(user_id)
    {
        response
            .headers_mut()
            .insert("x-mirakurun-tuner-user-id", value);
    }
    response
}

fn not_found() -> (StatusCode, Json<ApiError>) {
    api_error(StatusCode::NOT_FOUND, "channel not found")
}

fn unavailable() -> (StatusCode, Json<ApiError>) {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "tuner resource unavailable",
    )
}

fn internal_error() -> (StatusCode, Json<ApiError>) {
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to start tuner command",
    )
}

fn api_error(status: StatusCode, reason: &'static str) -> (StatusCode, Json<ApiError>) {
    (
        status,
        Json(ApiError {
            code: status.as_u16(),
            reason,
        }),
    )
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::config::{Config, TunerConfig};

    fn test_config(command: &str, channels: &[&str]) -> Config {
        Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            tuners: vec![TunerConfig {
                name: "test".into(),
                types: vec!["GR".into()],
                command: command.into(),
            }],
            channels: channels
                .iter()
                .map(|channel| ChannelConfig {
                    name: format!("channel {channel}"),
                    channel_type: "GR".into(),
                    channel: (*channel).into(),
                    service_id: None,
                    command_vars: BTreeMap::new(),
                })
                .collect(),
        }
    }

    fn stream_request(channel: &str) -> Request<Body> {
        Request::builder()
            .uri(format!("/api/channels/GR/{channel}/stream"))
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn streams_command_stdout() {
        let response = router(Arc::new(AppState::new(test_config(
            "printf test-ts",
            &["27"],
        ))))
        .oneshot(stream_request("27"))
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "video/MP2T");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "test-ts"
        );
    }

    #[tokio::test]
    async fn shares_a_command_for_the_same_channel() {
        let state = Arc::new(AppState::new(test_config(
            "sleep 0.1; printf shared-ts",
            &["27"],
        )));
        let app = router(Arc::clone(&state));
        let first = app.clone().oneshot(stream_request("27")).await.unwrap();
        let second = app.clone().oneshot(stream_request("27")).await.unwrap();
        let tuner_status = app
            .oneshot(
                Request::builder()
                    .uri("/api/tuners")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::OK);
        let tuner_status: serde_json::Value = serde_json::from_slice(&tuner_status).unwrap();
        assert_eq!(tuner_status[0]["users"].as_array().unwrap().len(), 2);

        let (first_body, second_body) =
            tokio::join!(first.into_body().collect(), second.into_body().collect());
        assert_eq!(first_body.unwrap().to_bytes(), "shared-ts");
        assert_eq!(second_body.unwrap().to_bytes(), "shared-ts");
    }

    #[tokio::test]
    async fn stops_the_command_after_the_last_client_disconnects() {
        let state = Arc::new(AppState::new(test_config("sleep 10", &["27"])));
        let response = router(Arc::clone(&state))
            .oneshot(stream_request("27"))
            .await
            .unwrap();
        drop(response);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while state.stream_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("tuner command did not stop");
        assert!(state.tuner_is_free(0));
    }

    #[tokio::test]
    async fn rejects_a_different_channel_while_tuner_is_in_use() {
        let app = router(Arc::new(AppState::new(test_config(
            "sleep 10",
            &["26", "27"],
        ))));
        let first = app.clone().oneshot(stream_request("26")).await.unwrap();
        let second = app.oneshot(stream_request("27")).await.unwrap();

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
        drop(first);
    }
}
