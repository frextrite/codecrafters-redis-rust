use std::{
    collections::HashMap,
    io::Write,
    net::{SocketAddr, TcpStream},
};

use crate::network::connection::Connection;

pub type ReplicaId = usize;

pub struct Replica {
    pub conn: Connection,
    replica_offset: usize,
}

impl Replica {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            conn: Connection::new(stream),
            replica_offset: 0,
        }
    }
}

#[derive(Default)] // clippy
pub struct ReplicaManager {
    replicas: HashMap<SocketAddr, Replica>,
}

impl ReplicaManager {
    pub fn new() -> Self {
        Self {
            replicas: HashMap::new(),
        }
    }

    pub fn add_replica(&mut self, replica: Replica) -> Option<Replica> {
        self.replicas
            .insert(replica.conn.stream.peer_addr().unwrap(), replica)
    }

    pub fn remove_replica(&mut self, conn: &Connection) -> Option<Replica> {
        self.replicas.remove(&conn.stream.peer_addr().unwrap())
    }

    pub fn get_connected_replica_count(&self) -> usize {
        self.replicas.len()
    }

    pub fn propagate_message_to_replicas(&mut self, message: &[u8]) {
        for (_, replica) in self.replicas.iter_mut() {
            let _ = replica.conn.stream.write_all(message);
        }
    }

    pub fn update_replica_offset(&mut self, stream: &TcpStream, offset: usize) {
        let replica = self.replicas.get_mut(&stream.peer_addr().unwrap());
        if let Some(replica) = replica {
            replica.replica_offset = offset;
        }
    }

    pub fn get_up_to_date_replicas_count(&self, offset: usize) -> usize {
        self.replicas
            .values()
            .filter(|replica| replica.replica_offset >= offset)
            .count()
    }
}
