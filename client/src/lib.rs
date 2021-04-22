// Copyright 2019-2021 Parity Technologies (UK) Ltd.
// This file is part of substrate-subxt.
//
// subxt is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// subxt is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with substrate-subxt.  If not, see <http://www.gnu.org/licenses/>.

//! Client for embedding substrate nodes.

#![deny(missing_docs)]

use async_std::{
    sync::{
        Arc,
        RwLock,
    },
    task,
};
use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    compat::Stream01CompatExt,
    future::{
        select,
        FutureExt,
    },
    sink::SinkExt,
    stream::StreamExt,
};
use futures01::sync::mpsc as mpsc01;
use jsonrpsee_types::{
    error::Error as JsonRpseeError,
    to_json_value,
    v2::{
        error::JsonRpcErrorAlloc,
        params::{
            Id,
            JsonRpcParams,
            SubscriptionId,
        },
        request::{
            JsonRpcCallSer,
            JsonRpcNotificationSer,
        },
        response::{
            JsonRpcNotifResponse,
            JsonRpcResponse,
        },
    },
    DeserializeOwned,
    FrontToBack,
    JsonValue,
    RequestMessage,
    Subscription,
    SubscriptionMessage,
};
use sc_network::config::TransportConfig;
pub use sc_service::{
    config::{
        DatabaseConfig,
        KeystoreConfig,
        WasmExecutionMethod,
    },
    Error as ServiceError,
};
use sc_service::{
    config::{
        NetworkConfiguration,
        TaskType,
        TelemetryEndpoints,
    },
    ChainSpec,
    Configuration,
    RpcHandlers,
    RpcSession,
    TaskManager,
};
use std::{
    collections::HashMap,
    marker::PhantomData,
};
use thiserror::Error;

const DEFAULT_CHANNEL_SIZE: usize = 16;

/// Error thrown by the client.
#[derive(Debug, Error)]
pub enum SubxtClientError {
    /// Failed to parse json rpc message.
    #[error("{0}")]
    Json(#[from] serde_json::Error),
    /// Channel closed.
    #[error("{0}")]
    Mpsc(#[from] mpsc::SendError),
}

/// Client for an embedded substrate node.
#[derive(Clone)]
pub struct SubxtClient {
    to_back: mpsc::Sender<FrontToBack>,
    request_id: Arc<RwLock<u64>>,
}

impl SubxtClient {
    /// Create a new client.
    pub fn new(mut task_manager: TaskManager, rpc: RpcHandlers) -> Self {
        let (to_back, from_front) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        let subscriptions = Arc::new(RwLock::new(HashMap::<u64, (u64, String)>::new()));

        task::spawn(
            select(
                Box::pin(
                    from_front.for_each(move |message: FrontToBack| {
                        let rpc = rpc.clone();
                        let (to_front, from_back) = mpsc01::channel(DEFAULT_CHANNEL_SIZE);
                        let session = RpcSession::new(to_front.clone());

                        let subscriptions = subscriptions.clone();

                        async move {
                            match message {
                                FrontToBack::Batch(_) => {
                                    unimplemented!()
                                }

                                FrontToBack::Notification(message) => {
                                    rpc.rpc_query(&session, &message).await;
                                }

                                FrontToBack::Request(RequestMessage {
                                    raw: message,
                                    send_back,
                                    ..
                                }) => {
                                    if let Some(response) =
                                        rpc.rpc_query(&session, &message).await
                                    {
                                        let result = if let Ok(success) =
                                            serde_json::from_str::<
                                                JsonRpcResponse<JsonValue>,
                                            >(
                                                &response
                                            ) {
                                            Ok(success.result)
                                        } else if let Ok(failure) =
                                            serde_json::from_str::<JsonRpcErrorAlloc>(
                                                &response,
                                            )
                                        {
                                            Err(JsonRpseeError::Request(failure))
                                        } else {
                                            panic!("failed to decode message");
                                        };

                                        send_back.map(|tx| {
                                            tx.send(result)
                                                .expect("failed to send request response")
                                        });
                                    }
                                }

                                FrontToBack::Subscribe(SubscriptionMessage {
                                    raw: message,
                                    unsubscribe_method,
                                    send_back,
                                    subscribe_id: request_id,
                                    unsubscribe_id,
                                }) => {
                                    {
                                        let mut subscriptions =
                                            subscriptions.write().await;
                                        subscriptions.insert(
                                            request_id,
                                            (unsubscribe_id, unsubscribe_method),
                                        );
                                    }

                                    let (mut send_front_sub, send_back_sub) =
                                        mpsc::channel(DEFAULT_CHANNEL_SIZE);

                                    if let Some(response) =
                                        rpc.rpc_query(&session, &message).await
                                    {
                                        let result = if let Ok(_success) =
                                            serde_json::from_str::<
                                                JsonRpcResponse<JsonValue>,
                                            >(
                                                &response
                                            ) {
                                            Ok((
                                                send_back_sub,
                                                SubscriptionId::Num(request_id),
                                            ))
                                        } else if let Ok(failure) =
                                            serde_json::from_str::<JsonRpcErrorAlloc>(
                                                &response,
                                            )
                                        {
                                            Err(JsonRpseeError::Request(failure))
                                        } else {
                                            panic!("failed to decode message");
                                        };

                                        send_back.send(result).expect(
                                            "failed to send subscription response",
                                        );
                                    }

                                    task::spawn(async move {
                                        let mut from_back = from_back.compat();
                                        let _session = session.clone();

                                        while let Some(Ok(response)) =
                                            from_back.next().await
                                        {
                                            let notif = serde_json::from_str::<
                                                JsonRpcNotifResponse<JsonValue>,
                                            >(
                                                &response
                                            )
                                            .expect(
                                                "failed to decode subscription notif",
                                            );
                                            // ignore send error since the channel is probably closed
                                            let _ = send_front_sub
                                                .send(notif.params.result)
                                                .await;
                                        }
                                    });
                                }

                                FrontToBack::SubscriptionClosed(subscription_id) => {
                                    let sub_id = if let SubscriptionId::Num(num) =
                                        subscription_id
                                    {
                                        num
                                    } else {
                                        unreachable!("subscription id should be num")
                                    };
                                    let json_sub_id = to_json_value(sub_id).unwrap();

                                    let subscriptions = subscriptions.read().await;
                                    if let Some((unsubscribe_id, unsubscribe_method)) =
                                        subscriptions.get(&sub_id)
                                    {
                                        let raw =
                                            serde_json::to_string(&JsonRpcCallSer::new(
                                                Id::Number(*unsubscribe_id),
                                                unsubscribe_method,
                                                JsonRpcParams::Array(vec![json_sub_id]),
                                            ))
                                            .unwrap();

                                        rpc.rpc_query(&session, &raw).await;
                                    }
                                }
                            }
                        }
                    }),
                ),
                Box::pin(async move {
                    task_manager.future().await.ok();
                }),
            )
            .map(drop),
        );

        let request_id = Arc::new(RwLock::new(u64::MIN));
        Self {
            to_back,
            request_id,
        }
    }

    /// Creates a new client from a config.
    pub fn from_config<C: ChainSpec + 'static>(
        config: SubxtClientConfig<C>,
        builder: impl Fn(Configuration) -> Result<(TaskManager, RpcHandlers), ServiceError>,
    ) -> Result<Self, ServiceError> {
        let config = config.into_service_config();
        let (task_manager, rpc_handlers) = (builder)(config)?;
        Ok(Self::new(task_manager, rpc_handlers))
    }

    /// Send a JSONRPC notification.
    pub async fn notification<'a, M, P>(
        &self,
        method: M,
        params: P,
    ) -> Result<(), JsonRpseeError>
    where
        M: Into<String> + Send,
        P: Into<JsonRpcParams<'a>> + Send,
    {
        let method = method.into();
        let notif = JsonRpcNotificationSer::new(&method, params.into());
        let raw = serde_json::to_string(&notif).unwrap();

        self.to_back
            .clone()
            .send(FrontToBack::Notification(raw))
            .await
            .map_err(|e| JsonRpseeError::TransportError(Box::new(e)))
    }

    async fn next_request_id(&self) -> u64 {
        let mut prev_request_id = self.request_id.write().await;
        let next_request_id = prev_request_id.clone();
        *prev_request_id = prev_request_id.wrapping_add(1);
        next_request_id
    }

    /// Send a JSONRPC request.
    pub async fn request<'a, T, M, P>(
        &self,
        method: M,
        params: P,
    ) -> Result<T, JsonRpseeError>
    where
        T: DeserializeOwned,
        M: Into<String> + Send,
        P: Into<JsonRpcParams<'a>> + Send,
    {
        let method = method.into();
        let params = params.into();

        let (send_back_tx, send_back_rx) = oneshot::channel();

        let req_id = self.next_request_id().await;
        let raw = serde_json::to_string(&JsonRpcCallSer::new(
            Id::Number(req_id),
            &method,
            params,
        ))
        .unwrap();

        self.to_back
            .clone()
            .send(FrontToBack::Request(RequestMessage {
                raw,
                id: req_id,
                send_back: Some(send_back_tx),
            }))
            .await
            .map_err(|e| JsonRpseeError::TransportError(Box::new(e)))?;

        let json_value = match send_back_rx.await {
            Ok(Ok(v)) => v,
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(JsonRpseeError::TransportError(Box::new(err))),
        };
        serde_json::from_value(json_value).map_err(JsonRpseeError::ParseError)
    }

    /// Send a subscription request to the server.
    pub async fn subscribe<'a, SM, UM, P, N>(
        &self,
        subscribe_method: SM,
        params: P,
        unsubscribe_method: UM,
    ) -> Result<Subscription<N>, JsonRpseeError>
    where
        SM: Into<String> + Send,
        UM: Into<String> + Send,
        P: Into<JsonRpcParams<'a>> + Send,
        N: DeserializeOwned,
    {
        let subscribe_method = subscribe_method.into();
        let unsubscribe_method = unsubscribe_method.into();
        let params = params.into();

        let subscribe_id = self.next_request_id().await;
        let unsubscribe_id = self.next_request_id().await;

        let raw = serde_json::to_string(&JsonRpcCallSer::new(
            Id::Number(subscribe_id),
            &subscribe_method,
            params,
        ))
        .unwrap();

        let (send_back_tx, send_back_rx) = oneshot::channel();
        self.to_back
            .clone()
            .send(FrontToBack::Subscribe(SubscriptionMessage {
                raw,
                subscribe_id,
                unsubscribe_id,
                unsubscribe_method,
                send_back: send_back_tx,
            }))
            .await
            .map_err(JsonRpseeError::Internal)?;

        let (notifs_rx, id) = match send_back_rx.await {
            Ok(Ok(val)) => val,
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(JsonRpseeError::TransportError(Box::new(err))),
        };
        Ok(Subscription {
            to_back: self.to_back.clone(),
            notifs_rx,
            marker: PhantomData,
            id,
        })
    }
}

/// Role of the node.
#[derive(Clone, Copy, Debug)]
pub enum Role {
    /// Light client.
    Light,
    /// A full node (mainly used for testing purposes).
    Authority(sp_keyring::AccountKeyring),
}

impl From<Role> for sc_service::Role {
    fn from(role: Role) -> Self {
        match role {
            Role::Light => Self::Light,
            Role::Authority(_) => Self::Authority,
        }
    }
}

impl From<Role> for Option<String> {
    fn from(role: Role) -> Self {
        match role {
            Role::Light => None,
            Role::Authority(key) => Some(key.to_seed()),
        }
    }
}

/// Client configuration.
#[derive(Clone)]
pub struct SubxtClientConfig<C: ChainSpec + 'static> {
    /// Name of the implementation.
    pub impl_name: &'static str,
    /// Version of the implementation.
    pub impl_version: &'static str,
    /// Author of the implementation.
    pub author: &'static str,
    /// Copyright start year.
    pub copyright_start_year: i32,
    /// Database configuration.
    pub db: DatabaseConfig,
    /// Keystore configuration.
    pub keystore: KeystoreConfig,
    /// Chain specification.
    pub chain_spec: C,
    /// Role of the node.
    pub role: Role,
    /// Enable telemetry on the given port.
    pub telemetry: Option<u16>,
    /// Wasm execution method
    pub wasm_method: WasmExecutionMethod,
}

impl<C: ChainSpec + 'static> SubxtClientConfig<C> {
    /// Creates a service configuration.
    pub fn into_service_config(self) -> Configuration {
        let mut network = NetworkConfiguration::new(
            format!("{} (subxt client)", self.chain_spec.name()),
            "unknown",
            Default::default(),
            None,
        );
        network.boot_nodes = self.chain_spec.boot_nodes().to_vec();
        network.transport = TransportConfig::Normal {
            enable_mdns: true,
            allow_private_ipv4: true,
            wasm_external_transport: None,
        };
        let telemetry_endpoints = if let Some(port) = self.telemetry {
            let endpoints = TelemetryEndpoints::new(vec![(
                format!("/ip4/127.0.0.1/tcp/{}/ws", port),
                0,
            )])
            .expect("valid config; qed");
            Some(endpoints)
        } else {
            None
        };
        let service_config = Configuration {
            keystore_remote: None,
            state_pruning: sc_service::PruningMode::ArchiveAll,
            keep_blocks: sc_service::KeepBlocks::All,
            transaction_storage: sc_service::TransactionStorageMode::BlockBody,
            wasm_runtime_overrides: None,
            disable_log_reloading: false,
            network,
            impl_name: self.impl_name.to_string(),
            impl_version: self.impl_version.to_string(),
            chain_spec: Box::new(self.chain_spec),
            role: self.role.into(),
            task_executor: (move |fut, ty| {
                match ty {
                    TaskType::Async => task::spawn(fut),
                    TaskType::Blocking => task::spawn_blocking(|| task::block_on(fut)),
                }
            })
            .into(),
            database: self.db,
            keystore: self.keystore,
            max_runtime_instances: 8,
            announce_block: true,
            dev_key_seed: self.role.into(),
            telemetry_endpoints,
            telemetry_external_transport: Default::default(),
            default_heap_pages: Default::default(),
            disable_grandpa: Default::default(),
            execution_strategies: Default::default(),
            force_authoring: Default::default(),
            offchain_worker: Default::default(),
            prometheus_config: Default::default(),
            rpc_cors: Default::default(),
            rpc_http: Default::default(),
            rpc_ipc: Default::default(),
            rpc_ws: Default::default(),
            rpc_ws_max_connections: Default::default(),
            rpc_methods: Default::default(),
            state_cache_child_ratio: Default::default(),
            state_cache_size: Default::default(),
            tracing_receiver: Default::default(),
            tracing_targets: Default::default(),
            transaction_pool: Default::default(),
            wasm_method: self.wasm_method,
            base_path: Default::default(),
            informant_output_format: Default::default(),
        };

        log::info!("{}", service_config.impl_name);
        log::info!("✌️  version {}", service_config.impl_version);
        log::info!("❤️  by {}, {}", self.author, self.copyright_start_year);
        log::info!(
            "📋 Chain specification: {}",
            service_config.chain_spec.name()
        );
        log::info!("🏷  Node name: {}", service_config.network.node_name);
        log::info!("👤 Role: {:?}", self.role);

        service_config
    }
}
