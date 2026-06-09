use amqp::AmqpSettings;

#[derive(serde::Deserialize, Clone)]
pub struct ServiceManagerSettings {
    pub host: String,
    pub port: u16,
}

#[derive(serde::Deserialize, Clone)]
pub struct TaskOrchestratorSettings {
    pub http: HttpSettings,
    pub amqp: AmqpSettings,
    pub mqtt: MqttSettings,
}

#[derive(serde::Deserialize, Clone)]
pub struct HttpSettings {
    pub port: u16,
    pub host: String,
}

#[derive(serde::Deserialize, Clone)]
pub struct MqttSettings {
    pub port: u16,
    pub host: String,
}

#[derive(serde::Deserialize, Clone)]
pub struct Settings {
    pub service_manager: ServiceManagerSettings,
    pub task_orchestrator: TaskOrchestratorSettings,
}

pub enum Environment {
    Staging,
    Development,
    Production,
    Testing,
}

// For looking up the .env file to use
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

pub fn load_base_configuration() -> Result<Settings, config::ConfigError> {
    // We load up the config.toml vars first. If any env vars are set, it overwrites the config var
    let mut builder = config::Config::builder()
        .add_source(config::File::new("config.toml", config::FileFormat::Toml));
    // If no MODE env var specified, default to .env.development
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
    builder = builder.add_source(config::Environment::default().separator("__"));

    builder.build()?.try_deserialize::<Settings>()
}
