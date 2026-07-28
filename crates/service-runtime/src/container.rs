//! Reference-runtime dependency construction and service registration.

use std::{sync::Arc, time::Duration};

use service_platform::{config::RuntimeConfig, errors::Result, health::HealthManager};

use crate::{
    manager::ServiceManager,
    service::{
        BadgeDbService, CacheDataAccessor, ConfluenceSettingsService, MattermostService, Service,
        WebSocketService,
    },
};

/// Owns the singleton dependencies used by one application lifecycle.
pub struct Container {
    version: String,
    config: RuntimeConfig,
    cache: CacheDataAccessor,
    health: Arc<HealthManager>,
    services: Arc<ServiceManager>,
}

impl Container {
    pub fn new(version: impl Into<String>, config: RuntimeConfig) -> Arc<Self> {
        let version = version.into();
        let health = HealthManager::new(Duration::from_secs(30), &version);
        let services = ServiceManager::new(&version, health.clone());
        Arc::new(Self {
            version,
            config,
            cache: CacheDataAccessor::default(),
            health,
            services,
        })
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    pub fn cache(&self) -> CacheDataAccessor {
        self.cache.clone()
    }

    pub fn health_manager(&self) -> Arc<HealthManager> {
        self.health.clone()
    }

    pub fn service_manager(&self) -> Arc<ServiceManager> {
        self.services.clone()
    }

    /// Creates and registers every service enabled by the runtime profile.
    pub async fn register_services(&self) -> Result<()> {
        self.config.validate()?;

        if self.config.inputs.websocket.enabled {
            self.register(Arc::new(WebSocketService::new(
                self.config.inputs.websocket.clone(),
                &self.version,
            )))
            .await?;
        }
        if self.config.adapters.badgedb.enabled {
            self.register(Arc::new(BadgeDbService::new(
                self.config.adapters.badgedb.clone(),
                &self.version,
            )))
            .await?;
        }
        if self.config.adapters.mattermost.enabled {
            self.register(Arc::new(MattermostService::new(
                self.config.adapters.mattermost.clone(),
                &self.version,
            )?))
            .await?;
        }
        if self.config.adapters.confluence.enabled {
            self.register(Arc::new(ConfluenceSettingsService::with_cache(
                self.config.adapters.confluence.clone(),
                &self.version,
                self.cache.clone(),
            )?))
            .await?;
        }
        Ok(())
    }

    pub async fn service(&self, name: &str) -> Option<Arc<dyn Service>> {
        self.services.get(name).await
    }

    async fn register<S>(&self, service: Arc<S>) -> Result<()>
    where
        S: Service + 'static,
    {
        let service: Arc<dyn Service> = service;
        self.services.register(service).await
    }
}

#[cfg(test)]
mod tests {
    use service_platform::config::RuntimeConfig;

    use super::*;

    #[tokio::test]
    async fn container_registers_only_enabled_services_and_reuses_singletons() {
        let mut config = RuntimeConfig::default();
        config.inputs.websocket.enabled = false;
        config.adapters.badgedb.enabled = true;
        config.adapters.mattermost.enabled = true;
        config.adapters.confluence.enabled = true;
        let container = Container::new("test", config.clone());

        container.register_services().await.unwrap();

        assert_eq!(container.version(), "test");
        assert_eq!(container.config().admin.address, config.admin.address);
        let _cache = container.cache();
        assert!(container.service("BadgeDBService").await.is_some());
        assert!(container.service("MattermostService").await.is_some());
        assert!(
            container
                .service("ConfluenceSettingsService")
                .await
                .is_some()
        );
        assert_eq!(container.service_manager().list().await.len(), 3);
        assert!(Arc::ptr_eq(
            &container.health_manager(),
            &container.health_manager()
        ));
        container.service_manager().start_all().await.unwrap();
        container.service_manager().stop_all().await.unwrap();
    }
}
