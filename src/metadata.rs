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

use std::{
    collections::HashMap,
    convert::TryFrom,
    marker::PhantomData,
    str::FromStr,
};

use codec::{
    Decode,
    Encode,
    Error as CodecError,
};

use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed, StorageEntryModifier, StorageEntryType, StorageHasher, META_RESERVED, RuntimeMetadataLastVersion, PalletConstantMetadata};
use sp_core::storage::StorageKey;

use crate::Encoded;

/// Metadata error.
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    /// Module is not in metadata.
    #[error("Module {0} not found")]
    ModuleNotFound(String),
    /// Module is not in metadata.
    #[error("Module index {0} not found")]
    ModuleIndexNotFound(u8),
    /// Call is not in metadata.
    #[error("Call {0} not found")]
    CallNotFound(&'static str),
    /// Event is not in metadata.
    #[error("Event {0} not found")]
    EventNotFound(u8),
    /// Event is not in metadata.
    #[error("Error {0} not found")]
    ErrorNotFound(u8),
    /// Storage is not in metadata.
    #[error("Storage {0} not found")]
    StorageNotFound(&'static str),
    /// Storage type does not match requested type.
    #[error("Storage type error")]
    StorageTypeError,
    /// Default error.
    #[error("Failed to decode default: {0}")]
    DefaultError(CodecError),
    /// Failure to decode constant value.
    #[error("Failed to decode constant value: {0}")]
    ConstantValueError(CodecError),
    /// Constant is not in metadata.
    #[error("Constant {0} not found")]
    ConstantNotFound(&'static str),
}

/// Runtime metadata.
#[derive(Clone, Debug)]
pub struct Metadata {
    metadata: RuntimeMetadataLastVersion,
}

impl Metadata {
    /// Returns `ModuleMetadata`.
    pub fn pallet<S>(&self, name: S) -> Result<&PalletMetadata, MetadataError>
    where
        S: ToString,
    {
        todo!()
        // let name = name.to_string();
        // self.modules
        //     .get(&name)
        //     .ok_or(MetadataError::ModuleNotFound(name))
    }
}

#[derive(Clone, Debug)]
pub struct PalletMetadata {
    index: u8,
    name: String,
    storage: HashMap<String, StorageMetadata>,
    constants: HashMap<String, PalletConstantMetadata>,
}

impl PalletMetadata {
    pub fn storage(&self, key: &'static str) -> Result<&StorageMetadata, MetadataError> {
        self.storage
            .get(key)
            .ok_or(MetadataError::StorageNotFound(key))
    }

    /// Get a constant's metadata by name
    pub fn constant(
        &self,
        key: &'static str,
    ) -> Result<&PalletConstantMetadata, MetadataError> {
        self.constants
            .get(key)
            .ok_or(MetadataError::ConstantNotFound(key))
    }
}

#[derive(Clone, Debug)]
pub struct StorageMetadata {
    module_prefix: String,
    storage_prefix: String,
    modifier: StorageEntryModifier,
    ty: StorageEntryType,
    default: Vec<u8>,
}

impl StorageMetadata {
    pub fn prefix(&self) -> StorageKey {
        let mut bytes = sp_core::twox_128(self.module_prefix.as_bytes()).to_vec();
        bytes.extend(&sp_core::twox_128(self.storage_prefix.as_bytes())[..]);
        StorageKey(bytes)
    }

    pub fn default<V: Decode>(&self) -> Result<V, MetadataError> {
        Decode::decode(&mut &self.default[..]).map_err(MetadataError::DefaultError)
    }

    pub fn hash(hasher: &StorageHasher, bytes: &[u8]) -> Vec<u8> {
        match hasher {
            StorageHasher::Identity => bytes.to_vec(),
            StorageHasher::Blake2_128 => sp_core::blake2_128(bytes).to_vec(),
            StorageHasher::Blake2_128Concat => {
                // copied from substrate Blake2_128Concat::hash since StorageHasher is not public
                sp_core::blake2_128(bytes)
                    .iter()
                    .chain(bytes)
                    .cloned()
                    .collect()
            }
            StorageHasher::Blake2_256 => sp_core::blake2_256(bytes).to_vec(),
            StorageHasher::Twox128 => sp_core::twox_128(bytes).to_vec(),
            StorageHasher::Twox256 => sp_core::twox_256(bytes).to_vec(),
            StorageHasher::Twox64Concat => {
                sp_core::twox_64(bytes)
                    .iter()
                    .chain(bytes)
                    .cloned()
                    .collect()
            }
        }
    }

    pub fn hash_key<K: Encode>(hasher: &StorageHasher, key: &K) -> Vec<u8> {
        Self::hash(hasher, &key.encode())
    }

    pub fn plain(&self) -> Result<StoragePlain, MetadataError> {
        match &self.ty {
            StorageEntryType::Plain(_) => {
                Ok(StoragePlain {
                    prefix: self.prefix().0,
                })
            }
            _ => Err(MetadataError::StorageTypeError),
        }
    }

    pub fn map<K: Encode>(&self) -> Result<StorageMap<K>, MetadataError> {
        todo!()
        // match &self.ty {
        //     StorageEntryType::Map { hasher, .. } => {
        //         Ok(StorageMap {
        //             _marker: PhantomData,
        //             prefix: self.prefix().0,
        //             hasher: hasher.clone(),
        //         })
        //     }
        //     _ => Err(MetadataError::StorageTypeError),
        // }
    }

    pub fn double_map<K1: Encode, K2: Encode>(
        &self,
    ) -> Result<StorageDoubleMap<K1, K2>, MetadataError> {
        todo!()
        // match &self.ty {
        //     StorageEntryType::DoubleMap {
        //         hasher,
        //         key2_hasher,
        //         ..
        //     } => {
        //         Ok(StorageDoubleMap {
        //             _marker: PhantomData,
        //             prefix: self.prefix().0,
        //             hasher1: hasher.clone(),
        //             hasher2: key2_hasher.clone(),
        //         })
        //     }
        //     _ => Err(MetadataError::StorageTypeError),
        // }
    }
}

#[derive(Clone, Debug)]
pub struct StoragePlain {
    prefix: Vec<u8>,
}

impl StoragePlain {
    pub fn key(&self) -> StorageKey {
        StorageKey(self.prefix.clone())
    }
}

#[derive(Clone, Debug)]
pub struct StorageMap<K> {
    _marker: PhantomData<K>,
    prefix: Vec<u8>,
    hasher: StorageHasher,
}

impl<K: Encode> StorageMap<K> {
    pub fn key(&self, key: &K) -> StorageKey {
        let mut bytes = self.prefix.clone();
        bytes.extend(StorageMetadata::hash_key(&self.hasher, key));
        StorageKey(bytes)
    }
}

#[derive(Clone, Debug)]
pub struct StorageDoubleMap<K1, K2> {
    _marker: PhantomData<(K1, K2)>,
    prefix: Vec<u8>,
    hasher1: StorageHasher,
    hasher2: StorageHasher,
}

impl<K1: Encode, K2: Encode> StorageDoubleMap<K1, K2> {
    pub fn key(&self, key1: &K1, key2: &K2) -> StorageKey {
        let mut bytes = self.prefix.clone();
        bytes.extend(StorageMetadata::hash_key(&self.hasher1, key1));
        bytes.extend(StorageMetadata::hash_key(&self.hasher2, key2));
        StorageKey(bytes)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidMetadataError {
    #[error("Invalid prefix")]
    InvalidPrefix,
    #[error("Invalid version")]
    InvalidVersion,
}

impl TryFrom<RuntimeMetadataPrefixed> for Metadata {
    type Error = InvalidMetadataError;

    fn try_from(metadata: RuntimeMetadataPrefixed) -> Result<Self, Self::Error> {
        todo!()
        // if metadata.0 != META_RESERVED {
        //     return Err(ConversionError::InvalidPrefix.into())
        // }
        // let meta = match metadata.1 {
        //     RuntimeMetadata::V14(meta) => meta,
        //     _ => return Err(ConversionError::InvalidVersion.into()),
        // };
        // let mut modules = HashMap::new();
        // let mut modules_with_calls = HashMap::new();
        // let mut modules_with_events = HashMap::new();
        // let mut modules_with_errors = HashMap::new();
        // for module in convert(meta.modules)?.into_iter() {
        //     let module_name = convert(module.name.clone())?;
        //
        //     let mut constant_map = HashMap::new();
        //     for constant in convert(module.constants)?.into_iter() {
        //         let constant_meta = convert_constant(constant)?;
        //         constant_map.insert(constant_meta.name.clone(), constant_meta);
        //     }
        //
        //     let mut storage_map = HashMap::new();
        //     if let Some(storage) = module.storage {
        //         let storage = convert(storage)?;
        //         let module_prefix = convert(storage.prefix)?;
        //         for entry in convert(storage.entries)?.into_iter() {
        //             let storage_prefix = convert(entry.name.clone())?;
        //             let entry = convert_entry(
        //                 module_prefix.clone(),
        //                 storage_prefix.clone(),
        //                 entry,
        //             )?;
        //             storage_map.insert(storage_prefix, entry);
        //         }
        //     }
        //     modules.insert(
        //         module_name.clone(),
        //         ModuleMetadata {
        //             index: module.index,
        //             name: module_name.clone(),
        //             storage: storage_map,
        //             constants: constant_map,
        //         },
        //     );
        //
        //     if let Some(calls) = module.calls {
        //         let mut call_map = HashMap::new();
        //         for (index, call) in convert(calls)?.into_iter().enumerate() {
        //             let name = convert(call.name)?;
        //             call_map.insert(name, index as u8);
        //         }
        //         modules_with_calls.insert(
        //             module_name.clone(),
        //             ModuleWithCalls {
        //                 index: module.index,
        //                 calls: call_map,
        //             },
        //         );
        //     }
        //     if let Some(events) = module.event {
        //         let mut event_map = HashMap::new();
        //         for (index, event) in convert(events)?.into_iter().enumerate() {
        //             event_map.insert(index as u8, convert_event(event)?);
        //         }
        //         modules_with_events.insert(
        //             module_name.clone(),
        //             ModuleWithEvents {
        //                 index: module.index,
        //                 name: module_name.clone(),
        //                 events: event_map,
        //             },
        //         );
        //     }
        //     let mut error_map = HashMap::new();
        //     for (index, error) in convert(module.errors)?.into_iter().enumerate() {
        //         error_map.insert(index as u8, convert_error(error)?);
        //     }
        //     modules_with_errors.insert(
        //         module_name.clone(),
        //         ModuleWithErrors {
        //             index: module.index,
        //             name: module_name.clone(),
        //             errors: error_map,
        //         },
        //     );
        // }
        // Ok(Metadata {
        //     pallets,
        //
        // })
    }
}
