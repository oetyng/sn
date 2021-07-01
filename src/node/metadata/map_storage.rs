// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{build_client_error_response, build_client_query_response};
use crate::messaging::{
    client::{CmdError, MapDataExchange, MapRead, MapWrite, QueryResponse},
    EndUser, MessageId,
};
use crate::node::{
    data_store::MapDataStore, error::convert_to_error_message, node_ops::NodeDuty, Error, Result,
};
use crate::routing::Prefix;
use crate::types::{
    Error as DtError, Map, MapAction, MapAddress, MapEntryActions, MapPermissionSet, MapValue,
    PublicKey, Result as NdResult,
};
use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    path::Path,
};
use tracing::{debug, info};

/// Operations over the data type Map.
pub(super) struct MapStorage {
    chunks: MapDataStore,
}

impl MapStorage {
    pub(super) async fn new(path: &Path, max_capacity: u64) -> Result<Self> {
        let chunks = MapDataStore::new(path, max_capacity).await?;
        Ok(Self { chunks })
    }

    pub(super) async fn get_data_of(&self, prefix: Prefix) -> Result<MapDataExchange> {
        let store = &self.chunks;
        let keys = self.chunks.keys().await?;

        let mut the_data = BTreeMap::default();

        for data in keys.iter().filter(|address| prefix.matches(address.name())) {
            let data = store.get(&data).await?;
            let _ = the_data.insert(*data.address(), data);
        }

        Ok(MapDataExchange(the_data))
    }

    pub async fn update(&mut self, map_data: MapDataExchange) -> Result<()> {
        debug!("Updating Map DataStore");
        let data_store = &mut self.chunks;
        let MapDataExchange(data) = map_data;

        for (_key, value) in data {
            data_store.put(&value).await?;
        }
        Ok(())
    }

    pub(super) async fn read(
        &self,
        read: &MapRead,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        use MapRead::*;
        match read {
            Get(address) => self.get(*address, msg_id, requester, origin).await,
            GetValue { address, ref key } => {
                self.get_value(*address, key, msg_id, requester, origin)
                    .await
            }
            GetShell(address) => self.get_shell(*address, msg_id, requester, origin).await,
            GetVersion(address) => self.get_version(*address, msg_id, requester, origin).await,
            ListEntries(address) => self.list_entries(*address, msg_id, requester, origin).await,
            ListKeys(address) => self.list_keys(*address, msg_id, requester, origin).await,
            ListValues(address) => self.list_values(*address, msg_id, requester, origin).await,
            ListPermissions(address) => {
                self.list_permissions(*address, msg_id, requester, origin)
                    .await
            }
            ListUserPermissions { address, user } => {
                self.list_user_permissions(*address, *user, msg_id, requester, origin)
                    .await
            }
        }
    }

    pub(super) async fn write(
        &mut self,
        write: MapWrite,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        use MapWrite::*;
        match write {
            New(data) => self.create(&data, msg_id, origin).await,
            Delete(address) => self.delete(address, msg_id, requester, origin).await,
            SetUserPermissions {
                address,
                user,
                ref permissions,
                version,
            } => {
                self.edit_chunk(&address, origin, msg_id, move |mut data| {
                    data.check_permissions(MapAction::ManagePermissions, &requester)?;
                    data.set_user_permissions(user, permissions.clone(), version)?;
                    Ok(data)
                })
                .await
            }
            DelUserPermissions {
                address,
                user,
                version,
            } => {
                self.delete_user_permissions(address, user, version, msg_id, requester, origin)
                    .await
            }
            Edit { address, changes } => {
                self.edit_entries(address, changes, msg_id, requester, origin)
                    .await
            }
        }
    }

    /// Get `Map` from the chunk store and check permissions.
    /// Returns `Some(Result<..>)` if the flow should be continued, returns
    /// `None` if there was a logic error encountered and the flow should be
    /// terminated.
    async fn get_chunk(
        &self,
        address: &MapAddress,
        requester: PublicKey,
        action: MapAction,
    ) -> Result<Map> {
        self.chunks.get(&address).await.and_then(move |map| {
            map.check_permissions(action, &requester)
                .map(move |_| map)
                .map_err(|error| error.into())
        })
    }

    /// Get Map from the chunk store, update it, and overwrite the stored chunk.
    async fn edit_chunk<F>(
        &mut self,
        address: &MapAddress,
        origin: EndUser,
        msg_id: MessageId,
        mutation_fn: F,
    ) -> Result<NodeDuty>
    where
        F: FnOnce(Map) -> NdResult<Map>,
    {
        let result = match self.chunks.get(address).await {
            Ok(data) => match mutation_fn(data) {
                Ok(map) => self.chunks.put(&map).await,
                Err(error) => Err(error.into()),
            },
            Err(error) => Err(error),
        };

        self.ok_or_error(result, msg_id, origin).await
    }

    /// Put Map.
    async fn create(&mut self, data: &Map, msg_id: MessageId, origin: EndUser) -> Result<NodeDuty> {
        let result = if self.chunks.has(data.address()).await {
            Err(Error::DataExists)
        } else {
            self.chunks.put(&data).await
        };
        self.ok_or_error(result, msg_id, origin).await
    }

    async fn delete(
        &mut self,
        address: MapAddress,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let result = match self.chunks.get(&address).await {
            Ok(map) => match map.check_is_owner(&requester) {
                Ok(()) => {
                    info!("Deleting Map");
                    self.chunks.delete(&address).await
                }
                Err(_e) => {
                    info!("Error: Delete Map called by non-owner");
                    Err(Error::NetworkData(DtError::AccessDenied(requester)))
                }
            },
            Err(error) => Err(error),
        };

        self.ok_or_error(result, msg_id, origin).await
    }

    /// Delete Map user permissions.
    async fn delete_user_permissions(
        &mut self,
        address: MapAddress,
        user: PublicKey,
        version: u64,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        self.edit_chunk(&address, origin, msg_id, move |mut data| {
            data.check_permissions(MapAction::ManagePermissions, &requester)?;
            data.del_user_permissions(user, version)?;
            Ok(data)
        })
        .await
    }

    /// Edit Map.
    async fn edit_entries(
        &mut self,
        address: MapAddress,
        actions: MapEntryActions,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        self.edit_chunk(&address, origin, msg_id, move |mut data| {
            data.mutate_entries(actions, &requester)?;
            Ok(data)
        })
        .await
    }

    /// Get entire Map.
    async fn get(
        &self,
        address: MapAddress,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let result = match self.get_chunk(&address, requester, MapAction::Read).await {
            Ok(res) => Ok(res),
            Err(Error::NoSuchChunk(addr)) => return Err(Error::NoSuchChunk(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(NodeDuty::Send(build_client_query_response(
            QueryResponse::GetMap(result),
            msg_id,
            origin,
        )))
    }

    /// Get Map shell.
    async fn get_shell(
        &self,
        address: MapAddress,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let result = match self
            .get_chunk(&address, requester, MapAction::Read)
            .await
            .map(|data| data.shell())
        {
            Ok(res) => Ok(res),
            Err(Error::NoSuchChunk(addr)) => return Err(Error::NoSuchChunk(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(NodeDuty::Send(build_client_query_response(
            QueryResponse::GetMapShell(result),
            msg_id,
            origin,
        )))
    }

    /// Get Map version.
    async fn get_version(
        &self,
        address: MapAddress,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let result = match self
            .get_chunk(&address, requester, MapAction::Read)
            .await
            .map(|data| data.version())
        {
            Ok(res) => Ok(res),
            Err(Error::NoSuchChunk(addr)) => return Err(Error::NoSuchChunk(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(NodeDuty::Send(build_client_query_response(
            QueryResponse::GetMapVersion(result),
            msg_id,
            origin,
        )))
    }

    /// Get Map value.
    async fn get_value(
        &self,
        address: MapAddress,
        key: &[u8],
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let res = self.get_chunk(&address, requester, MapAction::Read).await;
        let result = match res.and_then(|map| {
            map.get(key)
                .cloned()
                .map(MapValue::from)
                .ok_or(Error::NetworkData(DtError::NoSuchEntry))
        }) {
            Ok(res) => Ok(res),
            Err(Error::NoSuchChunk(addr)) => return Err(Error::NoSuchChunk(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(NodeDuty::Send(build_client_query_response(
            QueryResponse::GetMapValue(result),
            msg_id,
            origin,
        )))
    }

    /// Get Map keys.
    async fn list_keys(
        &self,
        address: MapAddress,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let result = match self
            .get_chunk(&address, requester, MapAction::Read)
            .await
            .map(|data| data.keys())
        {
            Ok(res) => Ok(res),
            Err(Error::NoSuchChunk(addr)) => return Err(Error::NoSuchChunk(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(NodeDuty::Send(build_client_query_response(
            QueryResponse::ListMapKeys(result),
            msg_id,
            origin,
        )))
    }

    /// Get Map values.
    async fn list_values(
        &self,
        address: MapAddress,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let res = self.get_chunk(&address, requester, MapAction::Read).await;
        let result = match res.map(|map| map.values()) {
            Ok(res) => Ok(res),
            Err(Error::NoSuchChunk(addr)) => return Err(Error::NoSuchChunk(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(NodeDuty::Send(build_client_query_response(
            QueryResponse::ListMapValues(result),
            msg_id,
            origin,
        )))
    }

    /// Get Map entries.
    async fn list_entries(
        &self,
        address: MapAddress,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let res = self.get_chunk(&address, requester, MapAction::Read).await;
        let result = match res.map(|map| map.entries().clone()) {
            Ok(res) => Ok(res),
            Err(Error::NoSuchChunk(addr)) => return Err(Error::NoSuchChunk(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(NodeDuty::Send(build_client_query_response(
            QueryResponse::ListMapEntries(result),
            msg_id,
            origin,
        )))
    }

    /// Get Map permissions.
    async fn list_permissions(
        &self,
        address: MapAddress,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let result = match self
            .get_chunk(&address, requester, MapAction::Read)
            .await
            .map(|data| data.permissions())
        {
            Ok(res) => Ok(res),
            Err(Error::NoSuchChunk(addr)) => return Err(Error::NoSuchChunk(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(NodeDuty::Send(build_client_query_response(
            QueryResponse::ListMapPermissions(result),
            msg_id,
            origin,
        )))
    }

    /// Get Map user permissions.
    async fn list_user_permissions(
        &self,
        address: MapAddress,
        user: PublicKey,
        msg_id: MessageId,
        requester: PublicKey,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let result = match self
            .get_chunk(&address, requester, MapAction::Read)
            .await
            .and_then(|data| {
                data.user_permissions(&user)
                    .map_err(|e| e.into())
                    .map(MapPermissionSet::clone)
            }) {
            Ok(res) => Ok(res),
            Err(Error::NoSuchChunk(addr)) => return Err(Error::NoSuchChunk(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(NodeDuty::Send(build_client_query_response(
            QueryResponse::ListMapUserPermissions(result),
            msg_id,
            origin,
        )))
    }

    async fn ok_or_error(
        &self,
        result: Result<()>,
        msg_id: MessageId,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        if let Err(error) = result {
            let error = convert_to_error_message(error);
            info!("MapStorage: Writing chunk FAILED!");

            Ok(NodeDuty::Send(build_client_error_response(
                CmdError::Data(error),
                msg_id,
                origin,
            )))
        } else {
            info!("MapStorage: Writing chunk PASSED!");
            Ok(NodeDuty::NoOp)
        }
    }
}

impl Display for MapStorage {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "MapStorage")
    }
}
