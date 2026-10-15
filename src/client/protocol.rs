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

//! Generalise protocol clients to reduce boilerplate on implementations. Provides the tools for downstream users to implement custom protocol clients.
//! See: [`settings!`], [`handle!`], [`ProtoStream`]
#![warn(missing_docs)]

use crossflow::bevy_ecs;
use std::future::Future;
use std::sync::{Arc, Mutex};

#[derive(Debug, thiserror::Error)]
/// Result error type in rmf2_to protocols.
///
/// All error types accept a [`String`].
pub enum ProtoError {
    /// Error within [`ProtoSettings`]'s fields.
    #[error("Configuration error: {0}")]
    Config(String),
    /// Error connecting with [`ProtoHandle`] or [`ProtoStream`] with config from [`ProtoSettings`].
    #[error("Connection error: {0}")]
    Connect(String),
    /// Error subscribing with [`ProtoHandle`] or [`ProtoStream`].
    #[error("Publishing error: {0}")]
    Publish(String),
    /// Error publishing with [`ProtoHandle`].
    #[error("Subscribing error: {0}")]
    Subscribe(String),
}

fn get_type<T: ?Sized>() -> &'static str {
    std::any::type_name::<T>()
}

// -----------------------------------------------------------------

// Send: Arc<Mutex<_>>
// DeserializeOwned + Default: load_base_configuration
/// Handle loading of protocol configuration. Constructed via [`settings!`].
pub trait ProtoSettings: serde::de::DeserializeOwned + Default + Send {
    /// The `[table]` name in `config.toml` this settings struct deserialises from.
    const TOML_NAME: &'static str;

    /// Deserialises `[TOML_NAME]` table from `config.toml`. Missing fields fallback to defaults.
    fn load_config() -> Result<Self, ProtoError> {
        match crate::config::load_configuration_section::<Self>(Self::TOML_NAME) {
            Ok(settings) => Ok(settings),
            Err(::config::ConfigError::NotFound(_)) => {
                tracing::warn!(
                    "No [{}] table in configuration, using defaults for {}",
                    Self::TOML_NAME,
                    get_type::<Self>()
                );
                Ok(Self::default())
            }
            Err(e) => Err(ProtoError::Config(format!(
                "Failed to load [{}] into {}: {e}",
                Self::TOML_NAME,
                get_type::<Self>()
            ))),
        }
    }

    /// Ensure all required fields are present and valid, otherwise raise [`ProtoError`][ProtoError::Config].
    fn validate(&self) -> Result<(), ProtoError>;
}

/// Constructs [`ProtoSettings`]. Do NOT write visibility modifiers within fields, they will be forced to be `pub`.
///
/// # Example
///
/// ```
/// # #[macro_use] extern crate rmf2_task_orchestrator;
/// # use rmf2_task_orchestrator::client::protocol::*;
/// settings! {
///     pub struct MQTTSettings in "mqtt_client" {
///         host: String = "localhost".into(),
///         port: u16 = 1883,
///         client_id: String = "p1".into(),
///     }
///     // Input: &self
///     // Output: Result<(), ProtoError>
///     validate |s| {
///         if s.client_id.is_empty() {
///             return Err(ProtoError::Config("client_id is required".into()));
///         }
///         if s.host.is_empty() {
///             return Err(ProtoError::Config("host is required".into()));
///         }
///         Ok(())
///     }
/// }
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! __proto_settings {
    (
        $(#[$m:meta])*
        $v:vis struct $name:ident in $table:literal {
            $($(#[$fm:meta])* $f:ident : $t:ty = $d:expr),* $(,)?
        }
        validate | $s:ident | $body:block
    ) => {
        $crate::__paste::paste! {
            #[doc(hidden)]
            #[allow(non_camel_case_types, unused_imports)]
            use $crate::__serde as [<__rmf2_to_serde_ $name>];

            $(#[$m])*
            #[derive($crate::__serde::Deserialize, Clone, PartialEq, Debug)]
            #[serde(crate = "__rmf2_to_serde_" $name, default, deny_unknown_fields)]
            $v struct $name {
                $($(#[$fm])* pub $f: $t),*
            }
        }

        impl ::core::default::Default for $name {
            fn default() -> Self {
                Self { $($f: $d),* }
            }
        }

        impl $crate::client::protocol::ProtoSettings for $name {
            const TOML_NAME: &'static str = $table;

            fn validate(&self) -> Result<(), $crate::client::protocol::ProtoError> {
                let $s = self;
                $body
            }
        }
    };
    // CATCH: Throw error when struct field visibility is specified
    (
        $(#[$m:meta])*
        $v:vis struct $name:ident in $table:literal {
            $($(#[$fm:meta])* $fv:vis $f:ident : $t:ty = $d:expr),* $(,)?
        }
        validate | $s:ident | $body:block
    ) => {
        ::core::compile_error!(::core::concat!(
            "`settings!` struct fields are always `pub`. \
             Remove all visibility qualifiers inside `struct ",
            ::core::stringify!($name),
            "`"
        ));

        // Second diagnostic, pointed at each offending token: `$fv` carries the
        // caller's span, and a visibility on a trait item is E0449. `const _` plus
        // block scoping lets this repeat per field without name collisions.
        $( const _: () = { trait __ProtoFieldVis { $fv fn $f(); } }; )*
    };
    // CATCH: Throw error when TOML_NAME is not specified
    (
        $(#[$m:meta])*
        $v:vis struct $name:ident {
            $($(#[$fm:meta])* $fv:vis $f:ident : $t:ty = $d:expr),* $(,)?
        }
        validate | $s:ident | $body:block
    ) => {
        ::core::compile_error!(::core::concat!(
            "`settings!` missing field `TOML_NAME`. Suggestion: `",
            ::core::stringify!($v),
            " struct ",
            ::core::stringify!($name),
            " in \"table_name_in_config.toml\"`"
        ));
    };
}

#[doc(inline)]
pub use __proto_settings as settings;

// -----------------------------------------------------------------

// bevy_ecs::prelude::Resource: Send + Sync + 'static
// Clone: for Res<bevy_ecs::prelude::Resource>::clone
/// Handles protocol connection, providing pub/sub/connect.
/// Surround original struct with [`handle!`] prior to `impl`.
///
/// # Example
///
/// ```ignore
/// impl ProtoHandle for XXXHandle {
///     type Settings = XXXSettings;
///     type NodeConfig = serde_json::Value;
///     type In = [u8];
///     type Stream = XXXStream;
///     // reference Out: XXXStream::Out
///
///     fn connect(
///         settings: Self::Settings,
///         runtime: tokio::runtime::Handle,
///     ) -> Result<Self, ProtoError> {...}
///
///     async fn publish(
///         &self,
///         address: &str,
///         payload: Self::In,
///         config: Self::NodeConfig,
///     ) -> Result<(), ProtoError> {...}
///
///     async fn subscribe(
///         &self,
///         address: &str,
///         config: Self::NodeConfig,
///     ) -> Result<Self::Stream, ProtoError> {...}
/// }
/// ```
pub trait ProtoHandle: bevy_ecs::prelude::Resource + Clone {
    /// Linked [`Settings`][ProtoSettings] implementation for this protocol.
    type Settings: ProtoSettings;

    /// Config per node
    type NodeConfig; // serde_json::Value

    /// Payload type to be [`published`][ProtoHandle::publish].
    type In; // [u8]

    /// Linked [`ProtoStream`] implementation for this protocol.
    type Stream: ProtoStream;

    /// Synchronously connects to protocol's server, returning client handle.
    fn connect(
        settings: Self::Settings,
        runtime: tokio::runtime::Handle,
    ) -> Result<Self, ProtoError>;

    /// Asynchronously publishes a message.
    fn publish(
        &self,
        address: &str,
        payload: Self::In,
        config: Self::NodeConfig,
    ) -> impl Future<Output = Result<(), ProtoError>> + Send;

    /// Asynchronously subscribes to a topic/address.
    fn subscribe(
        &self,
        address: &str,
        config: Self::NodeConfig,
    ) -> impl Future<Output = Result<Self::Stream, ProtoError>> + Send;
}

/// Prerequisites for [`ProtoHandle`].
///
/// # Example
///
/// ```
/// # #[macro_use] extern crate rmf2_task_orchestrator;
/// # use rmf2_task_orchestrator::client::protocol::*;
/// # use dashmap::DashMap;
/// # use rumqttc::{AsyncClient,MqttOptions};
/// # use std::sync::Arc;
/// # use tokio::runtime::Handle;
/// # use tokio::sync::broadcast;
/// # use tokio::sync::broadcast::error::RecvError;
/// pub type MqttOut = Vec<u8>;
///
/// pub struct MqttIn {
///     payload: MqttOut,
///     retain: bool,
/// }
/// impl MqttIn {
///     fn new(payload: impl Into<MqttOut>, retain: bool) -> Self {
///         Self { payload: payload.into(), retain }
///     }
/// }
///
/// # settings! {
/// #     pub struct MQTTSettings in "mqtt_client" {
/// #         host: String = "localhost".into(),
/// #         port: u16 = 1883,
/// #         client_id: String = "p1".into(),
/// #     }
/// #     validate |s| {
/// #         if s.client_id.is_empty() {
/// #             return Err(ProtoError::Config("client_id is required".into()));
/// #         }
/// #         if s.host.is_empty() {
/// #             return Err(ProtoError::Config("host is required".into()));
/// #         }
/// #         Ok(())
/// #     }
/// # }
/// # pub struct MQTTStream(broadcast::Receiver<MqttOut>);
/// # impl ProtoStream for MQTTStream {
/// #     type Out = MqttOut;
/// #     async fn recv(&mut self) -> Option<Self::Out> {
/// #         loop {
/// #             match self.0.recv().await {
/// #                 Ok(v) => return Some(v),
/// #                 Err(RecvError::Lagged(n)) => tracing::warn!("MqttListen: lagged {n}"),
/// #                 Err(RecvError::Closed) => return None,
/// #             }
/// #         }
/// #     }
/// # }
/// handle! {
///     pub struct MQTTHandle {
///         client: Arc<AsyncClient>,
///         subscriptions: Arc<DashMap<String, broadcast::Sender<MqttOut>>>,
///     }
/// }
/// # impl MQTTHandle {
/// #     fn parse_qos(qos: u8) -> Result<rumqttc::QoS, ProtoError> {
/// #         match qos {
/// #             0 => Ok(rumqttc::QoS::AtMostOnce),
/// #             _ => Err(ProtoError::Config(format!("{qos} not a valid QoS"))),
/// #         }
/// #     }
/// # }
/// impl ProtoHandle for MQTTHandle {
///     type Settings = MQTTSettings;
///     type NodeConfig = u8;
///     type In = MqttIn;
///     type Stream = MQTTStream;
///
///     fn connect(settings: MQTTSettings, runtime: Handle) -> Result<Self, ProtoError> {
///         // ...
///         # let MQTTSettings {
///         #     client_id,
///         #     host,
///         #     port,
///         # } = settings;
///         # let mut mqttoptions = MqttOptions::new(client_id, host, port);
///         # let (client, mut _eventloop) = AsyncClient::new(mqttoptions, 64);
///         # let subscriptions: Arc<DashMap<String, broadcast::Sender<MqttOut>>> = Arc::new(DashMap::new());
///         # Ok(Self {
///         #     client: Arc::new(client),
///         #     subscriptions,
///         # })
///     }
///
///     async fn publish(
///         &self,
///         topic: &str,
///         payload: MqttIn,
///         qos: Self::NodeConfig,
///     ) -> Result<(), ProtoError> {
///         // ...
///         # self.client
///         #     .publish(topic, Self::parse_qos(qos)?, payload.retain, payload.payload)
///         #     .await
///         #     .map_err(|e| ProtoError::Publish(format!("Failed to publish to {topic} topic: {e}")))?;
///         # Ok(())
///     }
///
///     async fn subscribe(
///         &self,
///         topic: &str,
///         qos: Self::NodeConfig,
///     ) -> Result<Self::Stream, ProtoError> {
///         // ...
///         # if let Some(tx) = self.subscriptions.get(topic) {
///         #     return Ok(MQTTStream(tx.subscribe()));
///         # }
///         # let (tx, rx) = broadcast::channel(16);
///         # self.client
///         #     .subscribe(topic, Self::parse_qos(qos)?)
///         #     .await
///         #     .map_err(|e| {
///         #         ProtoError::Subscribe(format!("Failed to subscribe to {topic} topic: {e}"))
///         #     })?;
///         # self.subscriptions.insert(topic.to_string(), tx);
///         # Ok(MQTTStream(rx))
///     }
/// }
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! __proto_handle {
    (
        $(#[$m:meta])*
        $v:vis struct $n:ident { $($fv:vis $f:ident : $t:ty),* $(,)? }
    ) => {
        $(#[$m])*
        #[derive(Clone)]
        $v struct $n { $($fv $f: $t),* }

        // Implement Resource trait manually to prevent versioning issues
        // Simple `#[derive(Resource)]` uses downstream version
        impl $crate::__bevy_ecs::prelude::Resource for $n {}
    };
}

#[doc(inline)]
pub use __proto_handle as handle;

// -----------------------------------------------------------------

/// Wrapper around a protocol output stream.
/// [`type Out`][ProtoStream::Out]: recommend [`Vec<u8>`] or [`serde_json::Value`]. Any other should be a custom type.
///
/// # Example
///
/// ```
/// # #[macro_use] extern crate rmf2_task_orchestrator;
/// # use rmf2_task_orchestrator::client::protocol::*;
/// # use tokio::sync::broadcast;
/// # use tokio::sync::broadcast::error::RecvError;
/// pub type MqttOut = Vec<u8>;
///
/// pub struct MQTTStream(broadcast::Receiver<MqttOut>);
/// impl ProtoStream for MQTTStream {
///     type Out = MqttOut;
///     async fn recv(&mut self) -> Option<Self::Out> {
///         loop {
///             match self.0.recv().await {
///                 Ok(v) => return Some(v),
///                 Err(RecvError::Lagged(n)) => tracing::warn!("Mqtt: lagged {n}"),
///                 Err(RecvError::Closed) => return None,
///             }
///         }
///     }
/// }
/// ```
pub trait ProtoStream: Send + 'static {
    /// Output message type.
    type Out;

    /// Asynchronously receives a message.
    fn recv(&mut self) -> impl Future<Output = Option<Self::Out>> + Send;
}

// -----------------------------------------------------------------

/// Wrapper to initialise [`ProtoHandle`] from the given [`ProtoSettings`].
pub struct EnsureProto<Handle: ProtoHandle>(Arc<Mutex<Option<Handle::Settings>>>);

impl<Handle: ProtoHandle> EnsureProto<Handle> {
    /// New instance given [`Option<settings>`][ProtoSettings].
    ///
    /// If `None` is given, [`load_config`][ProtoSettings::load_config] is used instead.
    pub fn new(settings: Option<Handle::Settings>) -> Self {
        Self::try_new(settings).unwrap_or_else(|e| panic!("{e}"))
    }
    // Preserves error to display full traceback
    fn try_new(settings: Option<Handle::Settings>) -> Result<Self, ProtoError> {
        let settings = match settings {
            Some(s) => s,
            None => Handle::Settings::load_config()?,
        };
        settings.validate()?;
        Ok(Self(Arc::new(Mutex::new(Some(settings)))))
    }
}

// Manually implementing #[derive(Clone)]
impl<Handle: ProtoHandle> Clone for EnsureProto<Handle> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

/// Oneshot lazy initialising of [`ProtoHandle`]
impl<Handle: ProtoHandle> bevy_ecs::system::Command for EnsureProto<Handle> {
    fn apply(self, world: &mut bevy_ecs::prelude::World) {
        if let Some(config) = (self.0).lock().unwrap().take() {
            let runtime = world.resource::<crate::TokioHandle>().0.clone();
            let instance = Handle::connect(config, runtime)
                .unwrap_or_else(|e| panic!("Failed to connect {}: {e}", get_type::<Handle>()));
            world.insert_resource(instance);
        }
    }
}
