use std::time::Duration;

use fake_server::{FakeApiManager, Ports};
use service_platform::{
    config::RuntimeConfig,
    health::{ServiceHealth, Status},
};
use service_runtime::service::{
    CacheDataAccessor, ConfluenceSettingsService, MattermostService, Service, WebSocketService,
};
use tokio::time::{sleep, timeout};

fn ephemeral_ports() -> Ports {
    Ports {
        confluence: 0,
        mattermost_http: 0,
        mattermost_ws: 0,
        websocket: 0,
    }
}

#[tokio::test]
async fn mattermost_logs_in_when_token_is_absent_and_posts_with_returned_token() {
    let fake = FakeApiManager::new(ephemeral_ports());
    let ports = fake.start().await.unwrap();
    let mut config = RuntimeConfig::default().adapters.mattermost;
    config.enabled = true;
    config.server_url = format!("http://127.0.0.1:{}", ports.mattermost_http);
    config.websocket_url.clear();
    config.api_token.clear();
    config.username = "local-user".to_owned();
    config.password = "local-password".to_owned();
    let service = MattermostService::new(config, "test").unwrap();

    service.start().await.unwrap();
    service.send_message("authenticated message").await.unwrap();

    let captured_posts = fake.mattermost().posts().await;
    assert_eq!(captured_posts.len(), 1);
    assert_eq!(captured_posts[0].message, "authenticated message");
    assert_eq!(
        service.metrics().await["api_token_configured"],
        serde_json::json!(true)
    );
    service.stop().await.unwrap();
    fake.stop().await.unwrap();
}

#[tokio::test]
async fn confluence_fetches_settings_populates_shared_cache_and_validates_reconfigure() {
    let fake = FakeApiManager::new(ephemeral_ports());
    let ports = fake.start().await.unwrap();
    let mut config = RuntimeConfig::default().adapters.confluence;
    config.enabled = true;
    config.api_endpoint = format!("http://127.0.0.1:{}", ports.confluence);
    config.refresh_interval = Duration::from_millis(20);
    let cache = CacheDataAccessor::default();
    let service =
        ConfluenceSettingsService::with_cache(config.clone(), "test", cache.clone()).unwrap();

    service.start().await.unwrap();

    assert!(!service.settings().await.is_empty());
    assert!(cache.get("confluence_settings").await.is_some());
    assert_eq!(service.custom_checks().await[0].status, Status::Up);

    config.settings_page_id.clear();
    assert!(service.configure(config).await.is_err());
    service.stop().await.unwrap();
    fake.stop().await.unwrap();
}

#[tokio::test]
async fn websocket_reconnects_after_server_closes_active_session() {
    let fake = FakeApiManager::new(ephemeral_ports());
    let ports = fake.start().await.unwrap();
    let mut config = RuntimeConfig::default().inputs.websocket;
    config.server_url = format!("ws://127.0.0.1:{}", ports.websocket);
    config.reconnect_interval = Duration::from_millis(5);
    let service = WebSocketService::new(config, "test");
    service.start().await.unwrap();

    timeout(Duration::from_secs(2), async {
        while fake.websocket().total_connections() < 1 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    fake.websocket().disconnect_clients();
    timeout(Duration::from_secs(2), async {
        while fake.websocket().total_connections() < 2 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    assert_eq!(service.custom_checks().await[0].status, Status::Up);
    service.stop().await.unwrap();
    fake.stop().await.unwrap();
}
