use std::{path::Path, time::Duration};

use fake_server::{FakeApiManager, Ports, WebSocketEvent};
use service_platform::config::RuntimeConfig;
use service_runtime::app::App;
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn demo_profile_routes_websocket_event_to_mattermost() {
    let fake = FakeApiManager::new(Ports {
        confluence: 0,
        mattermost_http: 0,
        mattermost_ws: 0,
        websocket: 0,
    });
    let assigned_ports = fake.start().await.unwrap();
    let mut runtime = RuntimeConfig::default();
    runtime.admin.address = "127.0.0.1:0".to_owned();
    runtime.inputs.websocket.server_url = format!("ws://127.0.0.1:{}", assigned_ports.websocket);
    runtime.inputs.websocket.reconnect_interval = Duration::from_millis(10);
    runtime.adapters.confluence.enabled = true;
    runtime.adapters.confluence.api_endpoint =
        format!("http://127.0.0.1:{}", assigned_ports.confluence);
    runtime.adapters.confluence.refresh_interval = Duration::from_millis(20);
    runtime.adapters.mattermost.enabled = true;
    runtime.adapters.mattermost.server_url =
        format!("http://127.0.0.1:{}", assigned_ports.mattermost_http);
    runtime.adapters.mattermost.websocket_url =
        format!("ws://127.0.0.1:{}", assigned_ports.mattermost_ws);
    runtime.adapters.badgedb.enabled = true;

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let app = App::new(
        "integration",
        runtime,
        root.join("config/profiles/demo/rule-engine.yaml"),
        root.join("config/workflow-rules.yaml"),
    );
    app.start().await.unwrap();

    timeout(Duration::from_secs(2), async {
        while fake.websocket().client_count() == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/api/send",
            assigned_ports.websocket
        ))
        .json(&WebSocketEvent {
            id: "integration-1".to_owned(),
            kind: "AAA".to_owned(),
            content: "integration event".to_owned(),
            user_id: "tester".to_owned(),
            ..WebSocketEvent::default()
        })
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    timeout(Duration::from_secs(2), async {
        while fake.mattermost().posts().await.is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let posts = fake.mattermost().posts().await;
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].channel_id, "test-channel-1");
    assert_eq!(posts[0].message, "Event AAA received: integration event");

    app.stop().await.unwrap();
    fake.stop().await.unwrap();
}
