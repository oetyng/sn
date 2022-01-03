// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::dbs::{
    convert_to_error_message, Error, EventStore, Result, UsedSpace, SLED_FLUSH_TIME_MS,
};
use crate::types::{
    register::{Action, Register, User},
    PublicKey, RegisterAddress as Address,
};
use crate::{
    messaging::{
        data::{
            DataCmd, OperationId, QueryResponse, RegisterCmd, RegisterDataExchange, RegisterRead,
            RegisterWrite, ServiceMsg,
        },
        AuthorityProof, ServiceAuth, WireMsg,
    },
    types::DataAddress,
};

use dashmap::DashMap;
use sled::Db;
use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    path::Path,
    sync::Arc,
};
use tracing::info;
use xor_name::{Prefix, XorName};

const DATABASE_NAME: &str = "register";

type RegisterOpStore = EventStore<RegisterCmd>;

/// Operations over the data type Register.
// TODO: dont expose this
#[derive(Clone, Debug)]
pub(crate) struct RegisterStorage {
    used_space: UsedSpace,
    registers: Arc<DashMap<XorName, Option<StateEntry>>>,
    db: Db,
}

#[derive(Clone, Debug)]
struct StateEntry {
    state: Register,
    store: RegisterOpStore,
}

impl RegisterStorage {
    /// Create new RegisterStorage
    pub(crate) fn new(path: &Path, max_capacity: u64) -> Result<Self> {
        let db_dir = path.join("db").join(DATABASE_NAME.to_string());

        let db = sled::Config::default()
            .path(&db_dir)
            .flush_every_ms(SLED_FLUSH_TIME_MS)
            .open()
            .map_err(Error::from)?;

        Ok(Self {
            used_space: UsedSpace::new(max_capacity),
            registers: Arc::new(DashMap::new()),
            db,
        })
    }

    /// --- Synching ---

    /// Used for replication of data to new Elders.
    pub(crate) async fn get_data_of(&self, prefix: Prefix) -> Result<RegisterDataExchange> {
        let mut the_data = BTreeMap::default();

        for entry in self.registers.iter() {
            let (key, cache) = entry.pair();
            if let Some(entry) = cache {
                if prefix.matches(entry.state.name()) {
                    let _prev = the_data.insert(*key, entry.store.get_all()?);
                }
            } else {
                let entry = self.load_state(*key)?;
                if prefix.matches(entry.state.name()) {
                    let _prev = the_data.insert(*key, entry.store.get_all()?);
                }
            }
        }

        Ok(RegisterDataExchange(the_data))
    }

    /// On receiving data from Elders when promoted.
    pub(crate) fn update(&self, reg_data: RegisterDataExchange) -> Result<()> {
        debug!("Updating Register store");

        let RegisterDataExchange(data) = reg_data;

        // todo: make outer loop parallel
        for (_, history) in data {
            for op in history {
                let auth = WireMsg::verify_sig(
                    op.auth.clone(),
                    ServiceMsg::Cmd(DataCmd::Register(op.write.clone())),
                )
                .map_err(|_| Error::InvalidSignature(op.auth.public_key))?;
                let _res = self.apply(op, auth)?;
            }
        }

        Ok(())
    }

    /// --- Writing ---

    pub(crate) async fn write(
        &self,
        write: RegisterWrite,
        auth: AuthorityProof<ServiceAuth>,
    ) -> Result<()> {
        // let required_space = std::mem::size_of::<RegisterCmd>() as u64;
        // if !self.used_space.can_consume(required_space).await {
        //     return Err(Error::NotEnoughSpace);
        // }
        let op = RegisterCmd {
            write,
            auth: auth.clone().into_inner(),
        };
        self.apply(op, auth)
    }

    fn apply(&self, op: RegisterCmd, auth: AuthorityProof<ServiceAuth>) -> Result<()> {
        let required_space = std::mem::size_of::<RegisterCmd>();

        let RegisterCmd { write, .. } = op.clone();

        let address = *write.address();
        let key = to_reg_key(&address)?;

        use RegisterWrite::*;
        match write {
            New(map) => {
                if self.registers.contains_key(&key) {
                    return Err(Error::DataExists);
                }
                trace!("Creating new register");
                let mut store = self.load_store(key)?;
                let _res = store.append(op)?;
                let _prev = self
                    .registers
                    .insert(key, Some(StateEntry { state: map, store }));

                self.used_space.increase(required_space);

                Ok(())
            }
            Delete(_) => {
                let result = match self.registers.get_mut(&key) {
                    None => {
                        trace!("Attempting to delete register if it exists");
                        let _res = self.db.drop_tree(key)?;
                        Ok(())
                    }
                    Some(mut entry) => {
                        let (_, cache) = entry.pair_mut();
                        if let Some(entry) = cache {
                            if entry.state.address().is_public() {
                                return Err(Error::CannotDeletePublicData(DataAddress::Register(
                                    address,
                                )));
                            }
                            // TODO - Register::check_permission() doesn't support Delete yet in safe-nd
                            // register.check_permission(action, Some(auth.public_key))?;
                            if auth.public_key != entry.state.owner() {
                                Err(Error::InvalidOwner(auth.public_key))
                            } else {
                                info!("Deleting Register");
                                let _res = self.db.drop_tree(key)?;
                                Ok(())
                            }
                        } else if self.load_store(key).is_ok() {
                            info!("Deleting Register");
                            let _res = self.db.drop_tree(key)?;
                            Ok(())
                        } else {
                            Ok(())
                        }
                    }
                };

                if result.is_ok() {
                    let _prev = self.registers.remove(&key);
                }

                result
            }
            Edit(reg_op) => {
                let mut cache = self
                    .registers
                    .get_mut(&key)
                    .ok_or(Error::NoSuchData(DataAddress::Register(address)))?;
                let entry = if let Some(cached_entry) = cache.as_mut() {
                    cached_entry
                } else {
                    let fresh_entry = self.load_state(key)?;
                    let _prev = cache.replace(fresh_entry);
                    if let Some(entry) = cache.as_mut() {
                        entry
                    } else {
                        return Err(Error::NoSuchData(DataAddress::Register(address)));
                    }
                };

                info!("Editing Register");
                entry
                    .state
                    .check_permissions(Action::Write, Some(auth.public_key))?;
                let result = entry.state.apply_op(reg_op).map_err(Error::NetworkData);

                if result.is_ok() {
                    entry.store.append(op)?;
                    self.used_space.increase(required_space);
                    trace!("Editing Register success!");
                } else {
                    trace!("Editing Register failed!");
                }

                result
            }
        }
    }

    /// --- Reading ---

    pub(crate) fn read(
        &self,
        read: &RegisterRead,
        requester_pk: PublicKey,
    ) -> Result<QueryResponse> {
        trace!("Reading register {:?}", read.dst_address());
        let operation_id = read.operation_id().map_err(|_| Error::NoOperationId)?;
        trace!("Operation of register read: {:?}", operation_id);
        use RegisterRead::*;
        match read {
            Get(address) => self.get(*address, requester_pk, operation_id),
            Read(address) => self.read_register(*address, requester_pk, operation_id),
            GetOwner(address) => self.get_owner(*address, requester_pk, operation_id),
            GetUserPermissions { address, user } => {
                self.get_user_permissions(*address, *user, requester_pk, operation_id)
            }
            GetPolicy(address) => self.get_policy(*address, requester_pk, operation_id),
        }
    }

    /// Get entire Register.
    fn get(
        &self,
        address: Address,
        requester_pk: PublicKey,
        operation_id: OperationId,
    ) -> Result<QueryResponse> {
        let result = match self.get_register(&address, Action::Read, requester_pk) {
            Ok(register) => Ok(register),
            Err(Error::NoSuchData(addr)) => return Err(Error::NoSuchData(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(QueryResponse::GetRegister((result, operation_id)))
    }

    /// Get `Register` from the store and check permissions.
    fn get_register(
        &self,
        address: &Address,
        action: Action,
        requester_pk: PublicKey,
    ) -> Result<Register> {
        let cache = self
            .registers
            .get(&to_reg_key(address)?)
            .ok_or_else(|| Error::NoSuchData(DataAddress::Register(*address)))?;

        let StateEntry { state, .. } = cache
            .as_ref()
            .ok_or_else(|| Error::NoSuchData(DataAddress::Register(*address)))?;

        state
            .check_permissions(action, Some(requester_pk))
            .map_err(Error::from)?;

        Ok(state.clone())
    }

    fn read_register(
        &self,
        address: Address,
        requester_pk: PublicKey,
        operation_id: OperationId,
    ) -> Result<QueryResponse> {
        let result = match self.get_register(&address, Action::Read, requester_pk) {
            Ok(register) => register.read(Some(requester_pk)).map_err(Error::from),
            Err(Error::NoSuchData(addr)) => return Err(Error::NoSuchData(addr)),
            Err(error) => Err(error),
        };

        Ok(QueryResponse::ReadRegister((
            result.map_err(convert_to_error_message),
            operation_id,
        )))
    }

    fn get_owner(
        &self,
        address: Address,
        requester_pk: PublicKey,
        operation_id: OperationId,
    ) -> Result<QueryResponse> {
        let result = match self.get_register(&address, Action::Read, requester_pk) {
            Ok(res) => Ok(res.owner()),
            Err(Error::NoSuchData(addr)) => return Err(Error::NoSuchData(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(QueryResponse::GetRegisterOwner((result, operation_id)))
    }

    fn get_user_permissions(
        &self,
        address: Address,
        user: User,
        requester_pk: PublicKey,
        operation_id: OperationId,
    ) -> Result<QueryResponse> {
        let result = match self
            .get_register(&address, Action::Read, requester_pk)
            .and_then(|register| {
                register
                    .permissions(user, Some(requester_pk))
                    .map_err(Error::from)
            }) {
            Ok(res) => Ok(res),
            Err(Error::NoSuchData(addr)) => return Err(Error::NoSuchData(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(QueryResponse::GetRegisterUserPermissions((
            result,
            operation_id,
        )))
    }

    fn get_policy(
        &self,
        address: Address,
        requester_pk: PublicKey,
        operation_id: OperationId,
    ) -> Result<QueryResponse> {
        let result = match self
            .get_register(&address, Action::Read, requester_pk)
            .and_then(|register| {
                register
                    .policy(Some(requester_pk))
                    .map(|p| p.clone())
                    .map_err(Error::from)
            }) {
            Ok(res) => Ok(res),
            Err(Error::NoSuchData(addr)) => return Err(Error::NoSuchData(addr)),
            Err(error) => Err(convert_to_error_message(error)),
        };

        Ok(QueryResponse::GetRegisterPolicy((result, operation_id)))
    }

    /// Load a register op store
    fn load_store(&self, id: XorName) -> Result<RegisterOpStore> {
        RegisterOpStore::new(id, self.db.clone()).map_err(Error::from)
    }

    fn load_state(&self, key: XorName) -> Result<StateEntry> {
        // read from disk
        let store = self.load_store(key)?;
        let mut reg = None;
        // apply all ops
        use RegisterWrite::*;
        for op in store.get_all()? {
            // first op shall be New
            if let New(register) = op.write {
                reg = Some(register);
            } else if let Some(register) = &mut reg {
                if let Edit(reg_op) = op.write {
                    register.apply_op(reg_op).map_err(Error::NetworkData)?;
                }
            }
        }

        reg.take()
            .ok_or(Error::InvalidStore)
            .map(|state| StateEntry { state, store })
    }
}

/// This also encodes the Public | Private scope,
/// as well as the tag of the Address.
fn to_reg_key(address: &Address) -> Result<XorName> {
    Ok(XorName::from_content(
        DataAddress::Register(*address)
            .encode_to_zbase32()?
            .as_bytes(),
    ))
}

impl Display for RegisterStorage {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "RegisterStorage")
    }
}

#[cfg(test)]
mod test {
    use super::RegisterOpStore;
    use crate::messaging::data::{RegisterCmd, RegisterWrite};
    use crate::messaging::ServiceAuth;
    use crate::node::Result;

    use crate::node::Error;
    use crate::types::{
        register::{PublicPermissions, PublicPolicy, Register, User},
        Keypair,
    };
    use rand::rngs::OsRng;
    use std::collections::BTreeMap;
    use std::path::Path;
    use tempfile::tempdir;
    use xor_name::XorName;

    #[tokio::test(flavor = "multi_thread")]
    async fn appends_and_reads_from_store() -> Result<()> {
        let id = xor_name::XorName::random();
        let tmp_dir = tempdir()?;
        let db_dir = tmp_dir.path().join(Path::new(&"db".to_string()));
        let db = sled::open(db_dir).map_err(|error| {
            trace!("Sled Error: {:?}", error);
            Error::Sled(error)
        })?;
        let mut store = RegisterOpStore::new(id, db)?;

        let authority_keypair1 = Keypair::new_ed25519(&mut OsRng);
        let pk = authority_keypair1.public_key();

        let register_name: XorName = rand::random();
        let register_tag = 43_000u64;

        let mut permissions = BTreeMap::default();
        let user_perms = PublicPermissions::new(true);
        let _prev = permissions.insert(User::Key(pk), user_perms);

        let replica1 = Register::new_public(
            pk,
            register_name,
            register_tag,
            Some(PublicPolicy {
                owner: pk,
                permissions,
            }),
        );

        let write = RegisterWrite::New(replica1);

        let auth = ServiceAuth {
            public_key: pk,
            signature: authority_keypair1.sign(b""),
        };

        let cmd = RegisterCmd { write, auth };

        store.append(cmd.clone())?;

        let events = store.get_all()?;
        assert_eq!(events.len(), 1);

        match events.get(0) {
            Some(found_cmd) => assert_eq!(found_cmd, &cmd),
            None => unreachable!(),
        }

        Ok(())
    }
}
