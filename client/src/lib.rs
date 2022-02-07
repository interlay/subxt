// Copyright 2019-2022 Parity Technologies (UK) Ltd.
// This file is part of subxt.
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
// along with subxt.  If not, see <http://www.gnu.org/licenses/>.

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
    future::{
        select,
        FutureExt,
    },
    sink::SinkExt,
    stream::StreamExt,
};
use jsonrpsee_core::{
    client::{
        FrontToBack,
        RequestMessage,
        Subscription,
        SubscriptionKind,
        SubscriptionMessage,
    },
    DeserializeOwned,
    Error as JsonRpseeError,
    JsonValue,
};
use jsonrpsee_types::{
    error::ErrorResponse,
    params::{
        Id,
        ParamsSer,
        SubscriptionId,
    },
    request::{
        Notification,
        NotificationSer,
        RequestSer,
    },
    response::{
        Response,
        SubscriptionPayload,
    },
};
use sc_network::config::TransportConfig;
pub use sc_service::{
    config::{
        DatabaseSource,
        KeystoreConfig,
        WasmExecutionMethod,
    },
    ChainSpec,
    Error as ServiceError,
    RpcHandlers,
    TaskManager,
};
use sc_service::{
    config::{
        NetworkConfiguration,
        TelemetryEndpoints,
    },
    Configuration,
    KeepBlocks,
    RpcSession,
};
pub use sp_keyring::AccountKeyring;
use std::{
    collections::HashMap,
    sync::atomic::{
        AtomicU64,
        Ordering,
    },
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
    next_id: Arc<AtomicU64>,
}

impl SubxtClient {
    /// Create a new client.
    pub fn new(mut task_manager: TaskManager, rpc: RpcHandlers) -> Self {
        let (to_back, from_front) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let subscriptions =
            Arc::new(RwLock::new(HashMap::<SubscriptionId, (String, Id)>::new()));

        task::spawn(
            select(
                Box::pin(from_front.for_each(move |message: FrontToBack| {
                    let rpc = rpc.clone();
                    let (to_front, mut from_back) = mpsc::unbounded();
                    let session = RpcSession::new(to_front.clone());

                    let subscriptions = subscriptions.clone();

                    async move {
                        match message {
                            FrontToBack::Notification(raw) => {
                                let _ = rpc.rpc_query(&session, &raw).await;
                            }
                            FrontToBack::Request(RequestMessage {
                                raw,
                                id,
                                send_back,
                            }) => {
                                let raw_response = rpc.rpc_query(&session, &raw).await;
                                let to_front =
                                    match read_jsonrpc_response(raw_response, id) {
                                        Some(Err(e)) => Err(e),
                                        Some(Ok(rp)) => Ok(rp),
                                        None => return,
                                    };

                                send_back
                                    .expect("request should have send_back")
                                    .send(to_front)
                                    .expect("failed to send request response");
                            }

                            FrontToBack::Subscribe(SubscriptionMessage {
                                raw,
                                subscribe_id,
                                unsubscribe_id,
                                unsubscribe_method,
                                send_back,
                            }) => {
                                let (mut send_front_sub, send_back_sub) =
                                    mpsc::channel(DEFAULT_CHANNEL_SIZE);

                                let raw_response = rpc.rpc_query(&session, &raw).await;

                                let sub_id = match read_jsonrpc_response(
                                    raw_response,
                                    subscribe_id,
                                ) {
                                    Some(Ok(value)) => value.to_string(),
                                    Some(Err(err)) => {
                                        send_back
                                            .send(Err(err))
                                            .expect("failed to send request response");
                                        return
                                    }
                                    None => return,
                                };
                                let sub_id: SubscriptionId = sub_id.into();

                                send_back
                                    .send(Ok((send_back_sub, sub_id.clone())))
                                    .expect("failed to send request response");

                                {
                                    let mut subscriptions = subscriptions.write().await;
                                    subscriptions.insert(
                                        sub_id.clone(),
                                        (unsubscribe_method, unsubscribe_id),
                                    );
                                }

                                task::spawn(async move {
                                    // let mut from_back = from_back.compat();
                                    let _session = session.clone();

                                    while let Some(response) = from_back.next().await {
                                        let notif = serde_json::from_str::<
                                            Notification<SubscriptionPayload<_>>,
                                        >(
                                            &response
                                        )
                                        .expect("failed to decode subscription notif");
                                        // ignore send error since the channel is probably closed
                                        let _ = send_front_sub
                                            .send(notif.params.result)
                                            .await;
                                    }
                                });
                            }

                            FrontToBack::SubscriptionClosed(sub_id) => {
                                let params: &[JsonValue] = &[sub_id.clone().into()];

                                let subscriptions = subscriptions.read().await;
                                if let Some((unsub_method, unsub_id)) =
                                    subscriptions.get(&sub_id)
                                {
                                    let message =
                                        serde_json::to_string(&RequestSer::new(
                                            &unsub_id,
                                            unsub_method,
                                            Some(params.into()),
                                        ))
                                        .unwrap();
                                    let _ = rpc.rpc_query(&session, &message).await;
                                }
                            }
                            _ => (),
                        }
                    }
                })),
                Box::pin(async move {
                    task_manager.future().await.ok();
                }),
            )
            .map(drop),
        );

        Self {
            to_back,
            next_id: Arc::new(AtomicU64::new(0)),
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
    pub async fn notification<'a>(
        &self,
        method: &'a str,
        params: ParamsSer<'a>,
    ) -> Result<(), JsonRpseeError> {
        let msg = serde_json::to_string(&NotificationSer::new(method, Some(params)))
            .map_err(JsonRpseeError::ParseError)?;
        self.to_back
            .clone()
            .send(FrontToBack::Notification(msg))
            .await
            .map_err(|e| JsonRpseeError::Internal(e))
    }

    /// Send a JSONRPC request.
    pub async fn request<'a, T>(
        &self,
        method: &'a str,
        params: Option<ParamsSer<'a>>,
    ) -> Result<T, JsonRpseeError>
    where
        T: DeserializeOwned,
    {
        let (send_back_tx, send_back_rx) = oneshot::channel();

        let id = Id::Number(self.next_id.fetch_add(1, Ordering::Relaxed));
        let msg = serde_json::to_string(&RequestSer::new(&id, method, params))
            .map_err(JsonRpseeError::ParseError)?;
        self.to_back
            .clone()
            .send(FrontToBack::Request(RequestMessage {
                raw: msg,
                id,
                send_back: Some(send_back_tx),
            }))
            .await
            .map_err(|e| JsonRpseeError::Internal(e))?;

        let json_value = match send_back_rx.await {
            Ok(Ok(v)) => v,
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(JsonRpseeError::Transport(err.into())),
        };
        serde_json::from_value(json_value).map_err(JsonRpseeError::ParseError)
    }

    /// Send a subscription request to the server.
    pub async fn subscribe<'a, N>(
        &self,
        subscribe_method: &'a str,
        params: Option<ParamsSer<'a>>,
        unsubscribe_method: &'a str,
    ) -> Result<Subscription<N>, JsonRpseeError>
    where
        N: DeserializeOwned,
    {
        let sub_req_id = Id::Number(self.next_id.fetch_add(1, Ordering::Relaxed));
        let unsub_req_id = Id::Number(self.next_id.fetch_add(1, Ordering::Relaxed));
        let msg = serde_json::to_string(&RequestSer::new(
            &sub_req_id,
            subscribe_method,
            params,
        ))
        .map_err(JsonRpseeError::ParseError)?;

        let (send_back_tx, send_back_rx) = oneshot::channel();
        self.to_back
            .clone()
            .send(FrontToBack::Subscribe(SubscriptionMessage {
                raw: msg,
                subscribe_id: sub_req_id,
                unsubscribe_id: unsub_req_id,
                unsubscribe_method: unsubscribe_method.to_owned(),
                send_back: send_back_tx,
            }))
            .await
            .map_err(JsonRpseeError::Internal)?;

        let (notifs_rx, id) = match send_back_rx.await {
            Ok(Ok(val)) => val,
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(JsonRpseeError::Transport(err.into())),
        };
        Ok(Subscription::new(
            self.to_back.clone(),
            notifs_rx,
            SubscriptionKind::Subscription(id),
        ))
    }
}

/// Role of the node.
#[derive(Clone, Copy, Debug)]
pub enum Role {
    /// Light client.
    Light,
    /// A full node (mainly used for testing purposes).
    Authority(AccountKeyring),
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
    pub db: DatabaseSource,
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
    /// Handle to the tokio runtime. Will be used to spawn futures by the task manager.
    pub tokio_handle: tokio::runtime::Handle,
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
            // wasm_external_transport: None,
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
            network,
            impl_name: self.impl_name.to_string(),
            impl_version: self.impl_version.to_string(),
            chain_spec: Box::new(self.chain_spec),
            role: self.role.into(),
            database: self.db,
            keystore: self.keystore,
            max_runtime_instances: 8,
            announce_block: true,
            dev_key_seed: self.role.into(),
            telemetry_endpoints,
            tokio_handle: self.tokio_handle,
            default_heap_pages: Default::default(),
            disable_grandpa: Default::default(),
            execution_strategies: Default::default(),
            force_authoring: Default::default(),
            keep_blocks: KeepBlocks::All,
            keystore_remote: Default::default(),
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
            state_pruning: Default::default(),
            transaction_storage: sc_client_db::TransactionStorageMode::BlockBody,
            wasm_runtime_overrides: Default::default(),
            rpc_max_payload: Default::default(),
            ws_max_out_buffer_capacity: Default::default(),
            runtime_cache_size: 2,
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

fn read_jsonrpc_response(
    maybe_msg: Option<String>,
    id: Id,
) -> Option<Result<JsonValue, JsonRpseeError>> {
    let msg: String = maybe_msg?;
    // NOTE: `let result` is a workaround because rustc otherwise doesn't compile
    // `msg` doesn't live long enough.
    let result = match serde_json::from_str::<Response<JsonValue>>(&msg) {
        Ok(resp) if resp.id == id => Some(Ok(resp.result)),
        Ok(_) => Some(Err(JsonRpseeError::InvalidRequestId)),
        Err(_) => {
            match serde_json::from_str::<ErrorResponse>(&msg) {
                Ok(err) => {
                    Some(Err(JsonRpseeError::Request(err.error.message.to_string())))
                }
                Err(_) => None,
            }
        }
    };
    result
}
