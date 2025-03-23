use std::{
    net::TcpStream,
    sync::Mutex,
    time::{Duration, Instant},
};

use crate::{
    network::connection::Connection, replication::replica_manager::ReplicaManager,
    storage::expiring_map::ExpiringHashMap,
};

use super::metadata::{ReplicaInfo, ServerMetadata};

pub struct MasterLiveData {
    pub replication_offset: usize,
    pub replica_manager: ReplicaManager,
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
                replica_manager: ReplicaManager::new(),
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
    pub live_data: Mutex<LiveData>,
    pub store: Mutex<ExpiringHashMap>,
}

impl Server {
    pub fn new(metadata: ServerMetadata) -> Server {
        let live_data = Mutex::new(LiveData::new(&metadata.replica_info));
        Server {
            metadata,
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

    // TODO: handle replica methods without exposing the internals of the replica manager
    pub fn add_replica(&self, stream: TcpStream) {
        if let LiveData::Master(master_data) = &mut *self.live_data.lock().unwrap() {
            let replica = ReplicaManager::new_replica(stream);
            master_data.replica_manager.add_replica(replica);
            println!(
                "INFO: New replica connected. Total replicas: {}",
                master_data.replica_manager.get_connected_replica_count()
            );
        }
    }

    pub fn handle_disconnect(&self, conn: &Connection) {
        if let LiveData::Master(master_data) = &mut *self.live_data.lock().unwrap() {
            master_data.replica_manager.remove_replica(conn);
            println!(
                "INFO: Replica disconnected. Remaining replicas: {}",
                master_data.replica_manager.get_connected_replica_count()
            );
        }
    }

    pub fn propagate_message(&self, message: &[u8]) {
        if let LiveData::Master(master_data) = &mut *self.live_data.lock().unwrap() {
            master_data
                .replica_manager
                .propagate_message_to_replicas(message);
        }
    }

    pub fn update_replica_offset(&self, stream: &TcpStream, offset: usize) {
        if let LiveData::Master(master_data) = &mut *self.live_data.lock().unwrap() {
            master_data
                .replica_manager
                .update_replica_offset(stream, offset);
        }
    }

    pub fn get_replica_count(&self) -> usize {
        if let LiveData::Master(master_data) = &*self.live_data.lock().unwrap() {
            master_data.replica_manager.get_connected_replica_count()
        } else {
            0
        }
    }

    pub fn get_up_to_date_replicas_count(&self, offset: usize) -> usize {
        if let LiveData::Master(master_data) = &*self.live_data.lock().unwrap() {
            master_data
                .replica_manager
                .get_up_to_date_replicas_count(offset)
        } else {
            0
        }
    }
}
