//! Local Confluence-, Mattermost-, and WebSocket-like APIs.

pub mod cli;

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{Message as WsMessage, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{RwLock, broadcast, watch},
    task::JoinHandle,
    time::timeout,
};

pub const DEFAULT_CONFLUENCE_PORT: u16 = 8090;
pub const DEFAULT_MATTERMOST_HTTP_PORT: u16 = 8091;
pub const DEFAULT_MATTERMOST_WS_PORT: u16 = 8092;
pub const DEFAULT_WEBSOCKET_PORT: u16 = 8093;
const TEST_TOKEN: &str = "test-token-123";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SettingRule {
    pub event_type: String,
    pub pattern: String,
    pub channel_id: String,
    pub message_template: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfluenceSettings {
    pub rules: Vec<SettingRule>,
}

#[derive(Debug, Clone, Serialize)]
struct ConfluencePage {
    id: String,
    title: String,
    #[serde(rename = "spaceKey")]
    space_key: String,
    body: Value,
    version: Value,
}

#[derive(Clone)]
pub struct ConfluenceState {
    pages: Arc<RwLock<BTreeMap<String, ConfluencePage>>>,
}

impl Default for ConfluenceState {
    fn default() -> Self {
        let settings = ConfluenceSettings {
            rules: vec![
                SettingRule {
                    event_type: "AAA".to_owned(),
                    pattern: ".*".to_owned(),
                    channel_id: "test-channel-1".to_owned(),
                    message_template: "Event AAA received: {{.Content}}".to_owned(),
                    enabled: true,
                },
                SettingRule {
                    event_type: "BBB".to_owned(),
                    pattern: "important.*".to_owned(),
                    channel_id: "alerts-channel".to_owned(),
                    message_template: "Important event: {{.Content}}".to_owned(),
                    enabled: true,
                },
                SettingRule {
                    event_type: "CCC".to_owned(),
                    pattern: ".*".to_owned(),
                    channel_id: "general-channel".to_owned(),
                    message_template: "Event CCC: {{.Content}}".to_owned(),
                    enabled: false,
                },
            ],
        };
        let page = build_page("settings-page-1", "Workflow Settings", "TEST", &settings, 1);
        Self {
            pages: Arc::new(RwLock::new(BTreeMap::from([(
                "settings-page-1".to_owned(),
                page,
            )]))),
        }
    }
}

impl ConfluenceState {
    pub async fn update_settings(&self, page_id: &str, settings: &ConfluenceSettings) -> bool {
        let mut pages = self.pages.write().await;
        let Some(page) = pages.get(page_id) else {
            return false;
        };
        let version = page.version["number"].as_u64().unwrap_or(0) + 1;
        let next = build_page(&page.id, &page.title, &page.space_key, settings, version);
        pages.insert(page_id.to_owned(), next);
        true
    }
}

fn build_page(
    id: &str,
    title: &str,
    space_key: &str,
    settings: &ConfluenceSettings,
    version: u64,
) -> ConfluencePage {
    ConfluencePage {
        id: id.to_owned(),
        title: title.to_owned(),
        space_key: space_key.to_owned(),
        body: json!({"storage": {"value": serde_json::to_string(settings).unwrap()}}),
        version: json!({"number": version}),
    }
}

pub fn confluence_router(state: ConfluenceState) -> Router {
    Router::new()
        .route("/rest/api/content/:page_id", get(confluence_page))
        .route("/rest/api/space", get(confluence_spaces))
        .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
        .with_state(state)
}

async fn confluence_page(
    State(state): State<ConfluenceState>,
    Path(page_id): Path<String>,
) -> Response {
    match state.pages.read().await.get(&page_id).cloned() {
        Some(page) => Json(page).into_response(),
        None => (StatusCode::NOT_FOUND, "Page not found").into_response(),
    }
}

async fn confluence_spaces() -> Json<Value> {
    Json(json!({"results": [{"key": "TEST", "name": "Test Space"}]}))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MattermostPost {
    pub id: String,
    pub channel_id: String,
    pub user_id: String,
    pub message: String,
    pub create_at: i64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct CreatePost {
    channel_id: String,
    message: String,
}

#[derive(Clone)]
pub struct MattermostState {
    posts: Arc<RwLock<Vec<MattermostPost>>>,
    channels: Arc<BTreeMap<String, Value>>,
    sequence: Arc<AtomicU64>,
    events: broadcast::Sender<String>,
    disconnects: broadcast::Sender<()>,
    closing: Arc<AtomicBool>,
}

impl Default for MattermostState {
    fn default() -> Self {
        let (events, _) = broadcast::channel(128);
        let (disconnects, _) = broadcast::channel(16);
        Self {
            posts: Arc::new(RwLock::new(Vec::new())),
            channels: Arc::new(BTreeMap::from([
                (
                    "test-channel-1".to_owned(),
                    json!({"id":"test-channel-1","name":"test-channel","display_name":"Test Channel","type":"O"}),
                ),
                (
                    "alerts-channel".to_owned(),
                    json!({"id":"alerts-channel","name":"alerts","display_name":"Alerts","type":"O"}),
                ),
                (
                    "general-channel".to_owned(),
                    json!({"id":"general-channel","name":"general","display_name":"General","type":"O"}),
                ),
            ])),
            sequence: Arc::new(AtomicU64::new(0)),
            events,
            disconnects,
            closing: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl MattermostState {
    pub async fn posts(&self) -> Vec<MattermostPost> {
        self.posts.read().await.clone()
    }

    fn begin_start(&self) {
        self.closing.store(false, Ordering::SeqCst);
    }

    fn shutdown_clients(&self) {
        self.closing.store(true, Ordering::SeqCst);
        let _ = self.disconnects.send(());
    }
}

pub fn mattermost_http_router(state: MattermostState) -> Router {
    Router::new()
        .route("/api/v4/users/login", post(mattermost_login))
        .route(
            "/api/v4/posts",
            get(mattermost_list_posts).post(mattermost_create_post),
        )
        .route("/api/v4/channels/:channel_id", get(mattermost_channel))
        .route("/api/v4/users/me", get(mattermost_me))
        .route(
            "/api/v4/system/ping",
            get(|| async { Json(json!({"status": "OK"})) }),
        )
        .with_state(state)
}

pub fn mattermost_websocket_router(state: MattermostState) -> Router {
    Router::new()
        .route("/api/v4/websocket", get(mattermost_websocket))
        .with_state(state)
}

async fn mattermost_login() -> Response {
    let mut response =
        Json(json!({"id":"user-1","username":"testuser","email":"test@example.com"}))
            .into_response();
    response.headers_mut().insert(
        "Token",
        TEST_TOKEN.parse().expect("valid static token header"),
    );
    response
}

async fn mattermost_create_post(
    State(state): State<MattermostState>,
    headers: HeaderMap,
    Json(input): Json<CreatePost>,
) -> Response {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }
    let sequence = state.sequence.fetch_add(1, Ordering::SeqCst) + 1;
    let now = Utc::now();
    let post = MattermostPost {
        id: format!("post-{sequence}"),
        channel_id: input.channel_id,
        user_id: "user-1".to_owned(),
        message: input.message,
        create_at: now.timestamp_millis(),
        timestamp: now,
    };
    state.posts.write().await.push(post.clone());
    let _ = state
        .events
        .send(json!({"event":"posted","data":{"post":post}}).to_string());
    Json(post).into_response()
}

async fn mattermost_list_posts(
    State(state): State<MattermostState>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }
    Json(state.posts().await).into_response()
}

async fn mattermost_channel(
    State(state): State<MattermostState>,
    headers: HeaderMap,
    Path(channel_id): Path<String>,
) -> Response {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }
    match state.channels.get(&channel_id) {
        Some(channel) => Json(channel.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, "Channel not found").into_response(),
    }
}

async fn mattermost_me(headers: HeaderMap) -> Response {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }
    Json(json!({"id":"user-1","username":"testuser","email":"test@example.com"})).into_response()
}

fn authorized(headers: &HeaderMap) -> bool {
    let bearer = headers
        .get("Authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let token = bearer.or_else(|| headers.get("Token").and_then(|value| value.to_str().ok()));
    token == Some(TEST_TOKEN)
}

async fn mattermost_websocket(
    State(state): State<MattermostState>,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade
        .on_upgrade(move |socket| mattermost_socket(socket, state))
        .into_response()
}

async fn mattermost_socket(mut socket: WebSocket, state: MattermostState) {
    if state.closing.load(Ordering::SeqCst) {
        return;
    }
    let mut disconnects = state.disconnects.subscribe();
    if state.closing.load(Ordering::SeqCst) {
        return;
    }
    if socket
        .send(WsMessage::Text(
            json!({"event":"hello","data":{"server_version":"6.0.0-fake"}}).to_string(),
        ))
        .await
        .is_err()
    {
        return;
    }
    let mut events = state.events.subscribe();
    loop {
        tokio::select! {
            _ = disconnects.recv() => return,
            incoming = socket.recv() => {
                if incoming.is_none_or(|message| message.is_err()) {
                    return;
                }
            }
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        if socket.send(WsMessage::Text(event)).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                    _ => {}
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct WebSocketEvent {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub content: String,
    pub user_id: String,
    pub timestamp: DateTime<Utc>,
    pub metadata: BTreeMap<String, Value>,
}

impl Default for WebSocketEvent {
    fn default() -> Self {
        Self {
            id: String::new(),
            kind: String::new(),
            content: String::new(),
            user_id: String::new(),
            timestamp: Utc::now(),
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct WebSocketState {
    messages: Arc<RwLock<Vec<WebSocketEvent>>>,
    events: broadcast::Sender<String>,
    disconnects: broadcast::Sender<()>,
    clients: Arc<AtomicUsize>,
    sequence: Arc<AtomicU64>,
    connections: Arc<AtomicU64>,
    closing: Arc<AtomicBool>,
}

impl Default for WebSocketState {
    fn default() -> Self {
        let (events, _) = broadcast::channel(256);
        let (disconnects, _) = broadcast::channel(16);
        Self {
            messages: Arc::new(RwLock::new(Vec::new())),
            events,
            disconnects,
            clients: Arc::new(AtomicUsize::new(0)),
            sequence: Arc::new(AtomicU64::new(0)),
            connections: Arc::new(AtomicU64::new(0)),
            closing: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl WebSocketState {
    pub async fn messages(&self) -> Vec<WebSocketEvent> {
        self.messages.read().await.clone()
    }

    pub fn client_count(&self) -> usize {
        self.clients.load(Ordering::SeqCst)
    }

    pub fn total_connections(&self) -> u64 {
        self.connections.load(Ordering::SeqCst)
    }

    /// Closes current generic WebSocket sessions while keeping the listener active.
    pub fn disconnect_clients(&self) {
        let _ = self.disconnects.send(());
    }

    fn begin_start(&self) {
        self.closing.store(false, Ordering::SeqCst);
    }

    fn shutdown_clients(&self) {
        self.closing.store(true, Ordering::SeqCst);
        let _ = self.disconnects.send(());
    }
}

pub fn websocket_router(state: WebSocketState) -> Router {
    Router::new()
        .route("/ws", get(generic_websocket))
        .route("/api/send", post(send_event))
        .route("/api/messages", get(list_events))
        .route("/health", get(websocket_health))
        .with_state(state)
}

async fn generic_websocket(
    State(state): State<WebSocketState>,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade
        .on_upgrade(move |socket| generic_socket(socket, state))
        .into_response()
}

async fn generic_socket(mut socket: WebSocket, state: WebSocketState) {
    if state.closing.load(Ordering::SeqCst) {
        return;
    }
    let mut events = state.events.subscribe();
    let mut disconnects = state.disconnects.subscribe();
    if state.closing.load(Ordering::SeqCst) {
        return;
    }
    state.clients.fetch_add(1, Ordering::SeqCst);
    state.connections.fetch_add(1, Ordering::SeqCst);
    let welcome = WebSocketEvent {
        id: "welcome".to_owned(),
        kind: "system".to_owned(),
        content: "Connected to Fake WebSocket Server".to_owned(),
        ..WebSocketEvent::default()
    };
    if socket
        .send(WsMessage::Text(
            serde_json::to_string(&welcome).expect("serialize welcome event"),
        ))
        .await
        .is_err()
    {
        state.clients.fetch_sub(1, Ordering::SeqCst);
        return;
    }
    loop {
        tokio::select! {
            _ = disconnects.recv() => break,
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(WsMessage::Text(text))) => {
                        if let Ok(mut event) = serde_json::from_str::<WebSocketEvent>(&text) {
                            event.timestamp = Utc::now();
                            state.messages.write().await.push(event);
                        }
                    }
                    Some(Ok(WsMessage::Binary(bytes))) => {
                        if let Ok(mut event) = serde_json::from_slice::<WebSocketEvent>(&bytes) {
                            event.timestamp = Utc::now();
                            state.messages.write().await.push(event);
                        }
                    }
                    Some(Ok(_)) => {}
                    _ => break,
                }
            }
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        if socket.send(WsMessage::Text(event)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    _ => {}
                }
            }
        }
    }
    state.clients.fetch_sub(1, Ordering::SeqCst);
}

async fn send_event(
    State(state): State<WebSocketState>,
    Json(mut event): Json<WebSocketEvent>,
) -> Json<Value> {
    if event.id.is_empty() {
        let sequence = state.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        event.id = format!("msg-{sequence}");
    }
    event.timestamp = Utc::now();
    state.messages.write().await.push(event.clone());
    let _ = state
        .events
        .send(serde_json::to_string(&event).expect("serialize WebSocket broadcast event"));
    Json(json!({
        "success": true,
        "message": event,
        "clients": state.client_count(),
    }))
}

async fn list_events(State(state): State<WebSocketState>) -> Json<Vec<WebSocketEvent>> {
    Json(state.messages().await)
}

async fn websocket_health(State(state): State<WebSocketState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "clients": state.client_count(),
        "message_count": state.messages.read().await.len(),
    }))
}

#[derive(Debug, Clone, Copy)]
pub struct Ports {
    pub confluence: u16,
    pub mattermost_http: u16,
    pub mattermost_ws: u16,
    pub websocket: u16,
}

impl Default for Ports {
    fn default() -> Self {
        Self {
            confluence: DEFAULT_CONFLUENCE_PORT,
            mattermost_http: DEFAULT_MATTERMOST_HTTP_PORT,
            mattermost_ws: DEFAULT_MATTERMOST_WS_PORT,
            websocket: DEFAULT_WEBSOCKET_PORT,
        }
    }
}

pub struct FakeApiManager {
    ports: Ports,
    confluence: ConfluenceState,
    mattermost: MattermostState,
    websocket: WebSocketState,
    running: tokio::sync::Mutex<Option<RunningServers>>,
}

struct RunningServers {
    addresses: Ports,
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<std::io::Result<()>>>,
}

impl FakeApiManager {
    pub fn new(ports: Ports) -> Arc<Self> {
        Arc::new(Self {
            ports,
            confluence: ConfluenceState::default(),
            mattermost: MattermostState::default(),
            websocket: WebSocketState::default(),
            running: tokio::sync::Mutex::new(None),
        })
    }

    pub fn confluence(&self) -> &ConfluenceState {
        &self.confluence
    }

    pub fn mattermost(&self) -> &MattermostState {
        &self.mattermost
    }

    pub fn websocket(&self) -> &WebSocketState {
        &self.websocket
    }

    pub async fn start(&self) -> anyhow::Result<Ports> {
        let mut running = self.running.lock().await;
        if let Some(running) = running.as_ref() {
            return Ok(running.addresses);
        }
        self.mattermost.begin_start();
        self.websocket.begin_start();
        let listeners = [
            TcpListener::bind(("0.0.0.0", self.ports.confluence)).await?,
            TcpListener::bind(("0.0.0.0", self.ports.mattermost_http)).await?,
            TcpListener::bind(("0.0.0.0", self.ports.mattermost_ws)).await?,
            TcpListener::bind(("0.0.0.0", self.ports.websocket)).await?,
        ];
        let addresses = Ports {
            confluence: listeners[0].local_addr()?.port(),
            mattermost_http: listeners[1].local_addr()?.port(),
            mattermost_ws: listeners[2].local_addr()?.port(),
            websocket: listeners[3].local_addr()?.port(),
        };
        let routers = [
            confluence_router(self.confluence.clone()),
            mattermost_http_router(self.mattermost.clone()),
            mattermost_websocket_router(self.mattermost.clone()),
            websocket_router(self.websocket.clone()),
        ];
        let (shutdown, _) = watch::channel(false);
        let mut tasks = Vec::new();
        for (listener, router) in listeners.into_iter().zip(routers) {
            let mut stop = shutdown.subscribe();
            tasks.push(tokio::spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(async move {
                        while stop.changed().await.is_ok() {
                            if *stop.borrow() {
                                break;
                            }
                        }
                    })
                    .await
            }));
        }
        *running = Some(RunningServers {
            addresses,
            shutdown,
            tasks,
        });
        Ok(addresses)
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        let running = self.running.lock().await.take();
        let Some(running) = running else {
            return Ok(());
        };
        let _ = running.shutdown.send(true);
        self.mattermost.shutdown_clients();
        self.websocket.shutdown_clients();
        let mut failures = Vec::new();
        for mut task in running.tasks.into_iter().rev() {
            match timeout(Duration::from_secs(5), &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => failures.push(format!("serve fake API: {error}")),
                Ok(Err(error)) => failures.push(format!("join fake API server: {error}")),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    failures.push("fake API server shutdown timed out".to_owned());
                }
            }
        }
        if !failures.is_empty() {
            anyhow::bail!(failures.join("; "));
        }
        Ok(())
    }
}

pub async fn healthcheck(port: u16) -> anyhow::Result<()> {
    reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/health"))
        .timeout(std::time::Duration::from_secs(2))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use futures_util::{SinkExt, StreamExt};
    use tokio::{net::TcpListener, time::timeout};
    use tokio_tungstenite::{connect_async, tungstenite::Message as ClientMessage};
    use tower::ServiceExt;

    use super::*;

    async fn response_json(response: Response) -> Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn confluence_page_contains_parseable_settings() {
        let response = confluence_router(ConfluenceState::default())
            .oneshot(
                Request::builder()
                    .uri("/rest/api/content/settings-page-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn confluence_supports_updates_missing_pages_spaces_and_health() {
        let state = ConfluenceState::default();
        assert!(
            !state
                .update_settings("missing", &ConfluenceSettings { rules: Vec::new() })
                .await
        );
        let settings = ConfluenceSettings {
            rules: vec![SettingRule {
                event_type: "DEPLOY".to_owned(),
                pattern: "production".to_owned(),
                channel_id: "alerts-channel".to_owned(),
                message_template: "{{.Content}}".to_owned(),
                enabled: true,
            }],
        };
        assert!(state.update_settings("settings-page-1", &settings).await);
        let app = confluence_router(state);

        let page = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/api/content/settings-page-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let page = response_json(page).await;
        assert_eq!(page["version"]["number"], 2);
        let decoded: ConfluenceSettings =
            serde_json::from_str(page["body"]["storage"]["value"].as_str().unwrap()).unwrap();
        assert_eq!(decoded.rules, settings.rules);

        for (uri, expected) in [
            ("/rest/api/content/missing", StatusCode::NOT_FOUND),
            ("/rest/api/space", StatusCode::OK),
            ("/health", StatusCode::OK),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{uri}");
        }
    }

    #[tokio::test]
    async fn mattermost_requires_auth_and_stores_posts() {
        let state = MattermostState::default();
        let app = mattermost_http_router(state.clone());
        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v4/posts")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"channel_id":"test-channel-1","message":"hello"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let accepted = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v4/posts")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {TEST_TOKEN}"))
                    .body(Body::from(
                        r#"{"channel_id":"test-channel-1","message":"hello"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        assert_eq!(state.posts().await.len(), 1);

        let listed = mattermost_http_router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v4/posts")
                    .header("authorization", format!("Bearer {TEST_TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn mattermost_supports_login_auth_variants_and_lookup_routes() {
        let state = MattermostState::default();
        let app = mattermost_http_router(state.clone());
        let login = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v4/users/login")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(login.status(), StatusCode::OK);
        assert_eq!(login.headers()["token"], TEST_TOKEN);

        for uri in [
            "/api/v4/posts",
            "/api/v4/channels/test-channel-1",
            "/api/v4/users/me",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
        }

        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v4/posts")
                    .header("content-type", "application/json")
                    .header("token", TEST_TOKEN)
                    .body(Body::from(
                        r#"{"channel_id":"alerts-channel","message":"token auth"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);
        assert_eq!(state.posts().await[0].id, "post-1");

        for (uri, expected) in [
            ("/api/v4/channels/alerts-channel", StatusCode::OK),
            ("/api/v4/channels/missing", StatusCode::NOT_FOUND),
            ("/api/v4/users/me", StatusCode::OK),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(uri)
                        .header("authorization", format!("Bearer {TEST_TOKEN}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{uri}");
        }

        let ping = app
            .oneshot(
                Request::builder()
                    .uri("/api/v4/system/ping")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ping.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn send_endpoint_stores_event_and_reports_clients() {
        let state = WebSocketState::default();
        let app = websocket_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/send")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"type":"AAA","content":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.messages().await.len(), 1);
        assert_eq!(state.messages().await[0].id, "msg-1");
    }

    #[tokio::test]
    async fn websocket_http_routes_list_messages_and_report_health() {
        let state = WebSocketState::default();
        assert_eq!(state.client_count(), 0);
        assert_eq!(state.total_connections(), 0);
        state.disconnect_clients();
        let app = websocket_router(state.clone());

        let sent = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/send")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"id":"provided","type":"DEPLOY","content":"done"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response_json(sent).await["message"]["id"], "provided");

        let listed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/messages")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response_json(listed).await.as_array().unwrap().len(), 1);

        let health = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let health = response_json(health).await;
        assert_eq!(health["status"], "ok");
        assert_eq!(health["message_count"], 1);
    }

    fn ephemeral_ports() -> Ports {
        Ports {
            confluence: 0,
            mattermost_http: 0,
            mattermost_ws: 0,
            websocket: 0,
        }
    }

    #[tokio::test]
    async fn manager_lifecycle_and_healthcheck_are_idempotent() {
        let manager = FakeApiManager::new(ephemeral_ports());
        assert!(
            manager
                .confluence()
                .pages
                .read()
                .await
                .contains_key("settings-page-1")
        );
        assert!(manager.mattermost().posts().await.is_empty());
        assert!(manager.websocket().messages().await.is_empty());

        let first = manager.start().await.unwrap();
        let second = manager.start().await.unwrap();
        assert_eq!(first.confluence, second.confluence);
        assert_eq!(first.websocket, second.websocket);
        healthcheck(first.websocket).await.unwrap();
        manager.stop().await.unwrap();
        manager.stop().await.unwrap();
        assert!(healthcheck(first.websocket).await.is_err());

        let restarted = manager.start().await.unwrap();
        healthcheck(restarted.websocket).await.unwrap();
        manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn partial_bind_failure_releases_listeners_for_retry() {
        let blocker = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let mut ports = ephemeral_ports();
        ports.mattermost_http = blocker.local_addr().unwrap().port();
        let manager = FakeApiManager::new(ports);

        assert!(manager.start().await.is_err());
        drop(blocker);

        let bound = manager.start().await.unwrap();
        healthcheck(bound.websocket).await.unwrap();
        manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn websocket_servers_exchange_welcome_client_and_broadcast_events() {
        let manager = FakeApiManager::new(ephemeral_ports());
        let ports = manager.start().await.unwrap();

        let (mut generic, _) = connect_async(format!("ws://127.0.0.1:{}/ws", ports.websocket))
            .await
            .unwrap();
        let welcome = timeout(Duration::from_secs(1), generic.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let welcome: WebSocketEvent = serde_json::from_slice(&welcome.into_data()).unwrap();
        assert_eq!(welcome.kind, "system");

        let text_event = WebSocketEvent {
            id: "client-text".to_owned(),
            kind: "CLIENT".to_owned(),
            content: "text".to_owned(),
            ..WebSocketEvent::default()
        };
        generic
            .send(ClientMessage::Text(
                serde_json::to_string(&text_event).unwrap(),
            ))
            .await
            .unwrap();
        let binary_event = WebSocketEvent {
            id: "client-binary".to_owned(),
            kind: "CLIENT".to_owned(),
            content: "binary".to_owned(),
            ..WebSocketEvent::default()
        };
        generic
            .send(ClientMessage::Binary(
                serde_json::to_vec(&binary_event).unwrap(),
            ))
            .await
            .unwrap();
        generic
            .send(ClientMessage::Text("not-json".to_owned()))
            .await
            .unwrap();
        generic
            .send(ClientMessage::Ping(b"ping".to_vec()))
            .await
            .unwrap();

        let response = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{}/api/send", ports.websocket))
            .json(&json!({"type":"SERVER","content":"broadcast"}))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let broadcast = timeout(Duration::from_secs(1), async {
            loop {
                let message = generic.next().await.unwrap().unwrap();
                if message.is_text() {
                    let event =
                        serde_json::from_slice::<WebSocketEvent>(&message.into_data()).unwrap();
                    if event.kind == "SERVER" {
                        break event;
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(broadcast.content, "broadcast");

        timeout(Duration::from_secs(1), async {
            while manager.websocket().messages().await.len() < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        manager.websocket().disconnect_clients();
        timeout(Duration::from_secs(1), async {
            while manager.websocket().client_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(generic);

        let (mut mattermost, _) = connect_async(format!(
            "ws://127.0.0.1:{}/api/v4/websocket",
            ports.mattermost_ws
        ))
        .await
        .unwrap();
        let hello = timeout(Duration::from_secs(1), mattermost.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(hello.into_text().unwrap().contains("\"event\":\"hello\""));

        reqwest::Client::new()
            .post(format!(
                "http://127.0.0.1:{}/api/v4/posts",
                ports.mattermost_http
            ))
            .bearer_auth(TEST_TOKEN)
            .json(&json!({"channel_id":"test-channel-1","message":"broadcast"}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        let posted = timeout(Duration::from_secs(1), mattermost.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(posted.into_text().unwrap().contains("\"event\":\"posted\""));
        mattermost.close(None).await.unwrap();
        drop(mattermost);

        manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn manager_shutdown_closes_active_websocket_sessions() {
        let manager = FakeApiManager::new(ephemeral_ports());
        let ports = manager.start().await.unwrap();
        let (mut generic, _) = connect_async(format!("ws://127.0.0.1:{}/ws", ports.websocket))
            .await
            .unwrap();
        let _ = generic.next().await;
        let (mut mattermost, _) = connect_async(format!(
            "ws://127.0.0.1:{}/api/v4/websocket",
            ports.mattermost_ws
        ))
        .await
        .unwrap();
        let _ = mattermost.next().await;

        timeout(Duration::from_secs(1), manager.stop())
            .await
            .unwrap()
            .unwrap();
        assert!(
            timeout(Duration::from_secs(1), generic.next())
                .await
                .is_ok()
        );
        assert!(
            timeout(Duration::from_secs(1), mattermost.next())
                .await
                .is_ok()
        );
    }
}
