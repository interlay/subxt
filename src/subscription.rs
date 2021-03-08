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

use crate::rpc::Rpc;
use core::{
    future::Future,
    pin::Pin,
};
use jsonrpsee_types::error::Error as RpcError;
use jsonrpsee_ws_client::WsSubscription as Subscription;
use sp_core::{
    storage::{
        StorageChangeSet,
        StorageKey,
    },
    twox_128,
};
use sp_runtime::traits::Header;
use std::collections::VecDeque;

use crate::{
    error::Error,
    events::{
        EventsDecoder,
        Raw,
        RawEvent,
    },
    frame::{
        system::Phase,
        Event,
    },
    runtimes::Runtime,
};

/// Event subscription simplifies filtering a storage change set stream for
/// events of interest.
pub struct EventSubscription<'a, T: Runtime, S: StorageChangeStream> {
    subscription: S,
    decoder: &'a EventsDecoder<T>,
    block: Option<T::Hash>,
    extrinsic: Option<usize>,
    event: Option<(&'static str, &'static str)>,
    events: VecDeque<RawEvent>,
    finished: bool,
}

impl<'a, T: Runtime, S: StorageChangeStream<Item = StorageChangeSet<T::Hash>>>
    EventSubscription<'a, T, S>
{
    /// Creates a new event subscription.
    pub fn new(subscription: S, decoder: &'a EventsDecoder<T>) -> Self {
        Self {
            subscription,
            decoder,
            block: None,
            extrinsic: None,
            event: None,
            events: Default::default(),
            finished: false,
        }
    }

    /// Only returns events contained in the block with the given hash.
    pub fn filter_block(&mut self, block: T::Hash) {
        self.block = Some(block);
    }

    /// Only returns events from block emitted by extrinsic with index.
    pub fn filter_extrinsic(&mut self, block: T::Hash, ext_index: usize) {
        self.block = Some(block);
        self.extrinsic = Some(ext_index);
    }

    /// Filters events by type.
    pub fn filter_event<E: Event<T>>(&mut self) {
        self.event = Some((E::MODULE, E::EVENT));
    }

    /// Gets the next event.
    pub async fn next(&mut self) -> Option<Result<RawEvent, Error>> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Some(Ok(event))
            }
            if self.finished {
                return None
            }
            let change_set = match self.subscription.next().await {
                Some(c) => c,
                None => {
                    return Some(Err(
                        RpcError::Custom("RPC subscription dropped".into()).into()
                    ))
                }
            };
            if let Some(hash) = self.block.as_ref() {
                if &change_set.block == hash {
                    self.finished = true;
                } else {
                    continue
                }
            }
            for (_key, data) in change_set.changes {
                if let Some(data) = data {
                    let raw_events = match self.decoder.decode_events(&mut &data.0[..]) {
                        Ok(events) => events,
                        Err(error) => return Some(Err(error)),
                    };
                    for (phase, raw) in raw_events {
                        if let Phase::ApplyExtrinsic(i) = phase {
                            if let Some(ext_index) = self.extrinsic {
                                if i as usize != ext_index {
                                    continue
                                }
                            }
                            let event = match raw {
                                Raw::Event(event) => event,
                                Raw::Error(err) => return Some(Err(err.into())),
                            };
                            if let Some((module, variant)) = self.event {
                                if event.module != module || event.variant != variant {
                                    continue
                                }
                            }
                            self.events.push_back(event);
                        }
                    }
                }
            }
        }
    }
}

/// Stream storage set changes
pub trait StorageChangeStream {
    /// Type to return from next.
    type Item;

    /// Fetch the next storage item.
    fn next<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Option<Self::Item>> + Send + 'a>>;
}

/// Wrapper over the normal storage subscription
pub struct ImportedStorageSubscription<T: Runtime>(
    pub(crate) Subscription<StorageChangeSet<T::Hash>>,
);

impl<T: Runtime> StorageChangeStream for ImportedStorageSubscription<T> {
    type Item = StorageChangeSet<T::Hash>;

    fn next<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Option<Self::Item>> + Send + 'a>> {
        async fn run<T: Runtime>(
            _self: &mut ImportedStorageSubscription<T>,
        ) -> Option<StorageChangeSet<T::Hash>> {
            _self.0.next().await
        }

        Box::pin(run(self))
    }
}

/// Event subscription to only fetch finalized storage changes.
pub struct FinalizedStorageSubscription<T: Runtime> {
    rpc: Rpc<T>,
    subscription: Subscription<T::Header>,
    storage_changes: VecDeque<StorageChangeSet<T::Hash>>,
    storage_key: StorageKey,
}

impl<T: Runtime> FinalizedStorageSubscription<T> {
    /// Creates a new event subscription.
    pub fn new(rpc: Rpc<T>, subscription: Subscription<T::Header>) -> Self {
        let mut storage_key = twox_128(b"System").to_vec();
        storage_key.extend(twox_128(b"Events").to_vec());
        log::debug!("Events storage key {:?}", hex::encode(&storage_key));

        Self {
            rpc,
            subscription,
            storage_changes: Default::default(),
            storage_key: StorageKey(storage_key),
        }
    }
}

impl<T: Runtime> StorageChangeStream for FinalizedStorageSubscription<T> {
    type Item = StorageChangeSet<T::Hash>;

    /// Gets the next event.
    fn next<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Option<Self::Item>> + Send + 'a>> {
        async fn run<T: Runtime>(
            _self: &mut FinalizedStorageSubscription<T>,
        ) -> Option<StorageChangeSet<T::Hash>> {
            loop {
                if let Some(storage_change) = _self.storage_changes.pop_front() {
                    return Some(storage_change)
                }
                let header: T::Header = _self.subscription.next().await?;
                if let Ok(storage_changes) = _self
                    .rpc
                    .query_storage_at(&[_self.storage_key.clone()], Some(header.hash()))
                    .await
                {
                    _self.storage_changes.extend(storage_changes);
                }
            }
        }

        Box::pin(run(self))
    }
}
