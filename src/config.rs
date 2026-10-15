/*
 * Copyright (C) 2026 ROS-Industrial Consortium Asia Pacific
 * Advanced Remanufacturing and Technology Centre
 * A*STAR Research Entities (Co. Registration No. 199702110H)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::sync::OnceLock;

#[derive(serde::Deserialize, Clone)]
pub struct AmqpSettings {
    pub host: String,
    pub port: u16,
    pub consumer: ConsumerSettings,
}

#[derive(serde::Deserialize, Clone)]
pub struct ConsumerSettings {
    pub exchange: String,
    pub queue: String,
    #[serde(default)]
    pub routing_key: String,
    #[serde(default = "default_exchange_kind")]
    pub exchange_kind: String,
}

fn default_exchange_kind() -> String {
    "topic".to_string()
}

impl From<&AmqpSettings> for String {
    fn from(config: &AmqpSettings) -> String {
        format!("amqp://{}:{}", config.host, config.port)
    }
}

#[derive(serde::Deserialize, Clone)]
pub struct HttpSettings {
    pub port: u16,
    pub host: String,
}

impl From<&HttpSettings> for String {
    fn from(config: &HttpSettings) -> String {
        format!("http://{}:{}", config.host, config.port)
    }
}

impl HttpSettings {
    pub fn addr(&self) -> (String, u16) {
        (self.host.clone(), self.port)
    }
}

#[derive(serde::Deserialize, Clone)]
pub struct Settings {
    pub http: HttpSettings,
    pub amqp: AmqpSettings,
}

pub enum Environment {
    Staging,
    Development,
    Production,
    Testing,
}

impl Environment {
    pub fn from_env() -> Self {
        match std::env::var("MODE").as_deref() {
            Ok("production") => Self::Production,
            Ok("staging") => Self::Staging,
            Ok("test") => Self::Testing,
            _ => Self::Development,
        }
    }
    pub fn load_env_file(&self) -> &'static str {
        match self {
            Self::Production => ".env.production",
            Self::Staging => ".env.staging",
            Self::Testing => ".env.test",
            Self::Development => ".env.development",
        }
    }
}

static BASE_CONFIG: OnceLock<config::Config> = OnceLock::new();

#[cfg(test)]
static CONFIG_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn load_base_configuration_once() -> Result<config::Config, config::ConfigError> {
    #[cfg(test)]
    CONFIG_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let mut builder = config::Config::builder()
        .add_source(config::File::new("config.toml", config::FileFormat::Toml));
    let env = Environment::from_env();
    let env_file = env.load_env_file();
    tracing::info!("Loading configuration from env file '{}'", env_file);
    if !std::path::Path::new(env_file).exists() {
        if let Environment::Development = env {
            tracing::info!(
                "Mode is set to DEVELOPMENT but no .env file is found. Defaulting to config.toml variables"
            );
        } else {
            return Err(config::ConfigError::NotFound(format!(
                "Env file '{}' not found. Ensure that it has been created with env variables set.",
                env_file
            )));
        }
    }
    dotenvy::from_filename(env_file).ok();
    builder = builder.add_source(
        config::Environment::default()
            .separator("__")
            .prefix("RMF2_TO")
            .prefix_separator("__"),
    );

    builder.build()
}

fn base_configuration() -> &'static config::Config {
    BASE_CONFIG.get_or_init(|| load_base_configuration_once().unwrap())
}

pub fn load_base_configuration<T>() -> Result<T, config::ConfigError>
where
    T: serde::de::DeserializeOwned,
{
    base_configuration().clone().try_deserialize::<T>()
}

// serde `deny_unknown_fields` can be enforced within
/// deserialise only within a specific section, ignoring other sections
/// eg. within config.toml: only parse `xx.yy`
pub fn load_configuration_section<T>(key: &str) -> Result<T, config::ConfigError>
where
    T: serde::de::DeserializeOwned,
{
    base_configuration().get::<T>(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::mqtt::{MqttSettings, MqttTomlFormat};

    #[allow(dead_code)]
    #[derive(serde::Deserialize)]
    struct InvalidSettings {
        some_rubbish: String,
    }

    #[test]
    fn test_invalid_load_base_configuration() {
        let result = load_base_configuration::<InvalidSettings>();
        assert!(result.is_err());
        let result = load_base_configuration::<MqttSettings>();
        assert!(result.is_err()); // deny_unknown_fields
        let result = load_base_configuration::<MqttTomlFormat>();
        assert!(result.is_ok()); // used
        let result = load_base_configuration::<Settings>();
        assert!(result.is_ok()); // used
        assert_eq!(CONFIG_CALLS.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn test_load_base_configuration_concurrently() {
        let n = 6;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(n));
        std::thread::scope(|s| {
            for i in 0..n {
                let barrier = barrier.clone();
                s.spawn(move || {
                    println!("test_load_base_configuration_concurrently: t{i} waiting...");
                    barrier.wait();
                    println!("test_load_base_configuration_concurrently: t{i} started!");
                    let _ = load_base_configuration::<Settings>();
                    assert_eq!(CONFIG_CALLS.load(std::sync::atomic::Ordering::Relaxed), 1);
                });
            }
        });
        println!("test_load_base_configuration_concurrently: all threads done");
        assert_eq!(CONFIG_CALLS.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
