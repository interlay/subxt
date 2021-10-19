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

use crate::{
    struct_def::StructDef,
    types::TypeGenerator,
};
use codec::Decode;
use darling::FromMeta;
use frame_metadata::{
    v14::RuntimeMetadataV14,
    PalletCallMetadata,
    PalletEventMetadata,
    PalletMetadata,
    RuntimeMetadata,
    RuntimeMetadataPrefixed,
    StorageEntryMetadata,
    StorageEntryModifier,
    StorageEntryType,
    StorageHasher,
};
use heck::SnakeCase as _;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro_error::{
    abort,
    abort_call_site,
};
use quote::{
    format_ident,
    quote,
};
use scale_info::{
    form::PortableForm,
    prelude::string::ToString,
    TypeDef,
};
use std::{
    collections::HashMap,
    fs,
    io::Read,
    path,
};
use syn::spanned::Spanned as _;

pub fn generate_runtime_types<P>(item_mod: syn::ItemMod, path: P) -> TokenStream2
where
    P: AsRef<path::Path>,
{
    let mut file = fs::File::open(&path).unwrap_or_else(|e| {
        abort_call_site!("Failed to open {}: {}", path.as_ref().to_string_lossy(), e)
    });

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .unwrap_or_else(|e| abort_call_site!("Failed to read metadata file: {}", e));

    let metadata = frame_metadata::RuntimeMetadataPrefixed::decode(&mut &bytes[..])
        .unwrap_or_else(|e| abort_call_site!("Failed to decode metadata: {}", e));

    let generator = RuntimeGenerator::new(metadata);
    generator.generate_runtime(item_mod)
}

#[derive(Debug, FromMeta)]
#[darling(rename_all = "snake_case")]
enum Subxt {
    SubstituteType(String),
}

impl Subxt {
    fn substitute_type(&self) -> String {
        match self {
            Self::SubstituteType(path) => path.clone(),
        }
    }
}

pub struct RuntimeGenerator {
    metadata: RuntimeMetadataV14,
}

impl RuntimeGenerator {
    pub fn new(metadata: RuntimeMetadataPrefixed) -> Self {
        match metadata.1 {
            RuntimeMetadata::V14(v14) => Self { metadata: v14 },
            _ => panic!("Unsupported metadata version {:?}", metadata.1),
        }
    }

    pub fn generate_runtime(&self, item_mod: syn::ItemMod) -> TokenStream2 {
        let type_substitutes = Self::parse_type_substitutes(&item_mod);
        let type_gen =
            TypeGenerator::new(&self.metadata.types, "runtime_types", type_substitutes);
        let types_mod = type_gen.generate_types_mod();
        let types_mod_ident = types_mod.ident();
        let pallets_with_mod_names = self
            .metadata
            .pallets
            .iter()
            .map(|pallet| {
                (
                    pallet,
                    format_ident!("{}", pallet.name.to_string().to_snake_case()),
                )
            })
            .collect::<Vec<_>>();
        let modules = pallets_with_mod_names.iter().map(|(pallet, mod_name)| {
            let calls = if let Some(ref calls) = pallet.calls {
                let (call_structs, call_fns) =
                    self.generate_calls(&type_gen, pallet, calls);
                quote! {
                    pub mod calls {
                        use super::#types_mod_ident;
                        #( #call_structs )*

                        pub struct TransactionApi<'a, T: ::subxt::Runtime> {
                            client: &'a ::subxt::Client<T>,
                        }

                        impl<'a, T: ::subxt::Runtime> TransactionApi<'a, T> {
                            pub fn new(client: &'a ::subxt::Client<T>) -> Self {
                                Self { client }
                            }

                            #( #call_fns )*
                        }
                    }
                }
            } else {
                quote!()
            };

            let event = if let Some(ref event) = pallet.event {
                let event_type = type_gen.resolve_type_path(event.ty.id(), &[]);
                let event_structs = self.generate_event_structs(&type_gen, pallet, event);
                quote! {
                    pub type Event = #event_type;
                    pub mod events {
                        use super::#types_mod_ident;
                        #( #event_structs )*
                    }
                }
            } else {
                quote!()
            };

            let (storage_structs, storage_fns) = if let Some(ref storage) = pallet.storage
            {
                let (storage_structs, storage_fns) = storage
                    .entries
                    .iter()
                    .map(|entry| {
                        self.generate_storage_entry_fns(&type_gen, &pallet, entry)
                    })
                    .unzip();
                (storage_structs, storage_fns)
            } else {
                (Vec::new(), Vec::new())
            };

            let storage_mod = quote! {
                pub mod storage {
                    use super::#types_mod_ident;
                    #( #storage_structs )*

                    pub struct StorageApi<'a, T: ::subxt::Runtime> {
                        client: &'a ::subxt::Client<T>,
                    }

                    impl<'a, T: ::subxt::Runtime> StorageApi<'a, T> {
                        pub fn new(client: &'a ::subxt::Client<T>) -> Self {
                            Self { client }
                        }

                        #( #storage_fns )*
                    }
                }
            };

            quote! {
                pub mod #mod_name {
                    use super::#types_mod_ident;
                    #calls
                    #event
                    #storage_mod
                }
            }
        });

        let outer_event_variants = self.metadata.pallets.iter().filter_map(|p| {
            let variant_name = format_ident!("{}", p.name);
            let mod_name = format_ident!("{}", p.name.to_string().to_snake_case());
            let index = proc_macro2::Literal::u8_unsuffixed(p.index);

            p.event.as_ref().map(|_| {
                quote! {
                    #[codec(index = #index)]
                    #variant_name(#mod_name::Event),
                }
            })
        });

        let outer_event = quote! {
            #[derive(Debug, Eq, PartialEq, ::codec::Encode, ::codec::Decode)]
            pub enum Event {
                #( #outer_event_variants )*
            }
        };

        // todo: [AJ] keep all other code items from decorated mod?
        let mod_ident = item_mod.ident;
        let pallets_with_storage =
            pallets_with_mod_names
                .iter()
                .filter_map(|(pallet, pallet_mod_name)| {
                    pallet.storage.as_ref().map(|_| pallet_mod_name)
                });
        let pallets_with_calls =
            pallets_with_mod_names
                .iter()
                .filter_map(|(pallet, pallet_mod_name)| {
                    pallet.calls.as_ref().map(|_| pallet_mod_name)
                });

        quote! {
            #[allow(dead_code, unused_imports, non_camel_case_types)]
            pub mod #mod_ident {
                #outer_event
                #( #modules )*
                #types_mod

                pub struct RuntimeApi<T: ::subxt::Runtime> {
                    pub client: ::subxt::Client<T>,
                }

                impl<T: ::subxt::Runtime> ::core::convert::From<::subxt::Client<T>> for RuntimeApi<T> {
                    fn from(client: ::subxt::Client<T>) -> Self {
                        Self { client }
                    }
                }

                impl<'a, T: ::subxt::Runtime> RuntimeApi<T> {
                    pub fn storage(&'a self) -> StorageApi<'a, T> {
                        StorageApi { client: &self.client }
                    }

                    pub fn tx(&'a self) -> TransactionApi<'a, T> {
                        TransactionApi { client: &self.client }
                    }
                }

                pub struct StorageApi<'a, T: ::subxt::Runtime> {
                    client: &'a ::subxt::Client<T>,
                }

                impl<'a, T: ::subxt::Runtime> StorageApi<'a, T> {
                    #(
                        pub fn #pallets_with_storage(&self) -> #pallets_with_storage::storage::StorageApi<'a, T> {
                            #pallets_with_storage::storage::StorageApi::new(self.client)
                        }
                    )*
                }

                pub struct TransactionApi<'a, T: ::subxt::Runtime> {
                    client: &'a ::subxt::Client<T>,
                }

                impl<'a, T: ::subxt::Runtime> TransactionApi<'a, T> {
                    #(
                        pub fn #pallets_with_calls(&self) -> #pallets_with_calls::calls::TransactionApi<'a, T> {
                            #pallets_with_calls::calls::TransactionApi::new(self.client)
                        }
                    )*
                }
            }
        }
    }

    fn parse_type_substitutes(item_mod: &syn::ItemMod) -> HashMap<String, syn::TypePath> {
        if let Some(ref content) = item_mod.content {
            content
                .1
                .iter()
                .filter_map(|item| {
                    if let syn::Item::Use(use_) = item {
                        let substitute_attrs = use_
                            .attrs
                            .iter()
                            .map(|attr| {
                                let meta = attr.parse_meta().unwrap_or_else(|e| {
                                    abort!(attr.span(), "Error parsing attribute: {}", e)
                                });
                                let substitute_type_args = Subxt::from_meta(&meta)
                                    .unwrap_or_else(|e| {
                                        abort!(
                                            attr.span(),
                                            "Error parsing attribute meta: {}",
                                            e
                                        )
                                    });
                                substitute_type_args
                            })
                            .collect::<Vec<_>>();
                        if substitute_attrs.len() > 1 {
                            abort!(
                                use_.attrs[0].span(),
                                "Duplicate `substitute_type` attributes"
                            )
                        }
                        substitute_attrs.iter().next().map(|attr| {
                            let substitute_type = attr.substitute_type();
                            let use_path = &use_.tree;
                            let use_type_path = syn::parse_quote!( #use_path );
                            (substitute_type.to_string(), use_type_path)
                        })
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            HashMap::new()
        }
    }

    fn generate_calls(
        &self,
        type_gen: &TypeGenerator,
        pallet: &PalletMetadata<PortableForm>,
        call: &PalletCallMetadata<PortableForm>,
    ) -> (Vec<TokenStream2>, Vec<TokenStream2>) {
        let struct_defs =
            self.generate_structs_from_variants(type_gen, call.ty.id(), "Call");
        struct_defs
            .iter()
            .map(|struct_def| {
                let (call_fn_args, call_args): (Vec<_>, Vec<_>) = struct_def
                    .named_fields()
                    .unwrap_or_else(|| {
                        abort_call_site!(
                            "Call variant for type {} must have all named fields",
                            call.ty.id()
                        )
                    })
                    .iter()
                    .map(|(name, ty)| (quote!( #name: impl Into< #ty > ), quote!( #name )))
                    .unzip();

                let pallet_name = &pallet.name;
                let call_struct_name = &struct_def.name;
                let function_name = struct_def.name.to_string().to_snake_case();
                let fn_name = format_ident!("{}", function_name);

                let call_struct = quote! {
                    #struct_def

                    impl ::subxt::Call for #call_struct_name {
                        const PALLET: &'static str = #pallet_name;
                        const FUNCTION: &'static str = #function_name;
                    }
                };
                let client_fn = quote! {
                    pub fn #fn_name(
                        &self,
                        #( #call_fn_args, )*
                    ) -> ::subxt::SubmittableExtrinsic<T, #call_struct_name> {
                        let call = #call_struct_name { #( #call_args, )* };
                        ::subxt::SubmittableExtrinsic::new(self.client, call)
                    }
                };
                (call_struct, client_fn)
            })
            .unzip()
    }

    fn generate_event_structs(
        &self,
        type_gen: &TypeGenerator,
        pallet: &PalletMetadata<PortableForm>,
        event: &PalletEventMetadata<PortableForm>,
    ) -> Vec<TokenStream2> {
        let struct_defs =
            self.generate_structs_from_variants(type_gen, event.ty.id(), "Event");
        struct_defs
            .iter()
            .map(|struct_def| {
                let pallet_name = &pallet.name;
                let event_struct = &struct_def.name;
                let event_name = struct_def.name.to_string();

                let event_struct = quote! {
                    #struct_def

                    impl ::subxt::Event for #event_struct {
                        const PALLET: &'static str = #pallet_name;
                        const EVENT: &'static str = #event_name;
                    }
                };
                event_struct
            })
            .collect()
    }

    fn generate_structs_from_variants(
        &self,
        type_gen: &TypeGenerator,
        type_id: u32,
        error_message_type_name: &str,
    ) -> Vec<StructDef> {
        let ty = self.metadata.types.resolve(type_id).unwrap_or_else(|| {
            abort_call_site!("Failed to resolve {} type", error_message_type_name)
        });
        if let scale_info::TypeDef::Variant(variant) = ty.type_def() {
            variant
                .variants()
                .iter()
                .map(|var| {
                    StructDef::new(
                        var.name(),
                        var.fields(),
                        Some(syn::parse_quote!(pub)),
                        type_gen,
                    )
                })
                .collect()
        } else {
            abort_call_site!(
                "{} type should be an variant/enum type",
                error_message_type_name
            )
        }
    }

    fn generate_storage_entry_fns(
        &self,
        type_gen: &TypeGenerator,
        pallet: &PalletMetadata<PortableForm>,
        storage_entry: &StorageEntryMetadata<PortableForm>,
    ) -> (TokenStream2, TokenStream2) {
        let entry_struct_ident = format_ident!("{}", storage_entry.name);
        let (fields, entry_struct, constructor, key_impl) = match storage_entry.ty {
            StorageEntryType::Plain(_) => {
                let entry_struct = quote!( pub struct #entry_struct_ident; );
                let constructor = quote!( #entry_struct_ident );
                let key_impl = quote!(::subxt::StorageEntryKey::Plain);
                (vec![], entry_struct, constructor, key_impl)
            }
            StorageEntryType::Map {
                ref key,
                ref hashers,
                ..
            } => {
                let key_ty = self.metadata.types.resolve(key.id()).unwrap_or_else(|| {
                    abort_call_site!("Failed to resolve storage key type")
                });
                let hashers = hashers
                    .iter()
                    .map(|hasher| {
                        let hasher = match hasher {
                            StorageHasher::Blake2_128 => "Blake2_128",
                            StorageHasher::Blake2_256 => "Blake2_256",
                            StorageHasher::Blake2_128Concat => "Blake2_128Concat",
                            StorageHasher::Twox128 => "Twox128",
                            StorageHasher::Twox256 => "Twox256",
                            StorageHasher::Twox64Concat => "Twox64Concat",
                            StorageHasher::Identity => "Identity",
                        };
                        let hasher = format_ident!("{}", hasher);
                        quote!( ::subxt::StorageHasher::#hasher )
                    })
                    .collect::<Vec<_>>();
                match key_ty.type_def() {
                    TypeDef::Tuple(tuple) => {
                        let fields = tuple
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(i, f)| {
                                let field_name =
                                    format_ident!("_{}", syn::Index::from(i));
                                let field_type = type_gen.resolve_type_path(f.id(), &[]);
                                (field_name, field_type)
                            })
                            .collect::<Vec<_>>();
                        // toddo: [AJ] use unzip here?
                        let tuple_struct_fields =
                            fields.iter().map(|(_, field_type)| field_type);
                        let field_names = fields.iter().map(|(field_name, _)| field_name);
                        let entry_struct = quote! {
                            pub struct #entry_struct_ident( #( #tuple_struct_fields ),* );
                        };
                        let constructor =
                            quote!( #entry_struct_ident( #( #field_names ),* ) );
                        let keys = (0..tuple.fields().len())
                            .into_iter()
                            .zip(hashers)
                            .map(|(field, hasher)| {
                                let index = syn::Index::from(field);
                                quote!( ::subxt::StorageMapKey::new(&self.#index, #hasher) )
                            });
                        let key_impl = quote! {
                            ::subxt::StorageEntryKey::Map(
                                vec![ #( #keys ),* ]
                            )
                        };
                        (fields, entry_struct, constructor, key_impl)
                    }
                    _ => {
                        let ty_path = type_gen.resolve_type_path(key.id(), &[]);
                        let fields = vec![(format_ident!("_0"), ty_path.clone())];
                        let entry_struct = quote! {
                            pub struct #entry_struct_ident( pub #ty_path );
                        };
                        let constructor = quote!( #entry_struct_ident(_0) );
                        let hasher = hashers.get(0).unwrap_or_else(|| {
                            abort_call_site!("No hasher found for single key")
                        });
                        let key_impl = quote! {
                            ::subxt::StorageEntryKey::Map(
                                vec![ ::subxt::StorageMapKey::new(&self.0, #hasher) ]
                            )
                        };
                        (fields, entry_struct, constructor, key_impl)
                    }
                }
            }
        };
        let pallet_name = &pallet.name;
        let storage_name = &storage_entry.name;
        let fn_name = format_ident!("{}", storage_entry.name.to_snake_case());
        let storage_entry_ty = match storage_entry.ty {
            StorageEntryType::Plain(ref ty) => ty,
            StorageEntryType::Map { ref value, .. } => value,
        };
        let storage_entry_value_ty =
            type_gen.resolve_type_path(storage_entry_ty.id(), &[]);
        let (return_ty, fetch) = match storage_entry.modifier {
            StorageEntryModifier::Default => {
                (quote!( #storage_entry_value_ty ), quote!(fetch_or_default))
            }
            StorageEntryModifier::Optional => {
                (
                    quote!( ::core::option::Option<#storage_entry_value_ty> ),
                    quote!(fetch),
                )
            }
        };

        let storage_entry_type = quote! {
            #entry_struct

            impl ::subxt::StorageEntry for #entry_struct_ident {
                const PALLET: &'static str = #pallet_name;
                const STORAGE: &'static str = #storage_name;
                type Value = #storage_entry_value_ty;
                fn key(&self) -> ::subxt::StorageEntryKey {
                    #key_impl
                }
            }
        };

        let key_args = fields
            .iter()
            .map(|(field_name, field_type)| quote!( #field_name: #field_type )); // todo: [AJ] borrow non build-inf types?
        let client_fn = quote! {
            pub async fn #fn_name(
                &self,
                #( #key_args, )*
                hash: ::core::option::Option<T::Hash>,
            ) -> ::core::result::Result<#return_ty, ::subxt::Error> {
                let entry = #constructor;
                self.client.storage().#fetch(&entry, hash).await
            }
        };

        (storage_entry_type, client_fn)
    }
}
