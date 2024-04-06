use std::{
    collections::HashMap,
    io::Write,
    net::{SocketAddr, TcpStream},
};

pub type ReplicaId = usize;

pub struct Replica {
    stream: TcpStream,
}

impl Replica {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
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
            .insert(replica.stream.peer_addr().unwrap(), replica)
    }

    pub fn remove_replica(&mut self, addr: SocketAddr) -> Option<Replica> {
        self.replicas.remove(&addr)
    }

    pub fn propagate_message_to_replicas(&mut self, message: &[u8]) {
        for (_, replica) in self.replicas.iter_mut() {
            let _ = replica.stream.write_all(message);
        }
    }
}
