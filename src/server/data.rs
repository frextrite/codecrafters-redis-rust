use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use crate::{replication::replica_manager::ReplicaManager, storage::expiring_map::ExpiringHashMap};

use super::metadata::{ReplicaInfo, ServerMetadata};

pub struct MasterLiveData {
    pub replication_offset: usize,
}

pub struct SlaveLiveData {
    pub offset: usize,
    pub heartbeat_recv_time: Option<Instant>,
}

pub enum LiveData {
    Master(MasterLiveData),
    Slave(SlaveLiveData),
}

impl LiveData {
    fn new(info: &ReplicaInfo) -> LiveData {
        match info {
            ReplicaInfo::Master(..) => LiveData::Master(MasterLiveData {
                replication_offset: 0,
            }),
            ReplicaInfo::Slave(..) => LiveData::Slave(SlaveLiveData {
                offset: 0,
                heartbeat_recv_time: None,
            }),
        }
    }
}

pub struct Server {
    pub metadata: ServerMetadata,
    pub replica_manager: Mutex<ReplicaManager>,
    pub live_data: Mutex<LiveData>,
    pub store: Mutex<ExpiringHashMap>,
}

impl Server {
    pub fn new(metadata: ServerMetadata) -> Server {
        let live_data = Mutex::new(LiveData::new(&metadata.replica_info));
        Server {
            metadata,
            replica_manager: Mutex::new(ReplicaManager::new()),
            live_data,
            store: Mutex::new(ExpiringHashMap::new()),
        }
    }

    pub fn set(&self, key: &[u8], value: &[u8], expiry: Option<Duration>) {
        self.store.lock().unwrap().set(key, value, expiry);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.store.lock().unwrap().get(key)
    }
}
