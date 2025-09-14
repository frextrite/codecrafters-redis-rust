use crate::common::CRLF;

use super::config::Config;

#[derive(Debug)]
pub struct MasterInfo {
    pub replication_id: String,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct SlaveInfo {
    pub master_host: String,
    pub master_port: u16,
}

#[derive(Debug)]
pub enum ReplicaInfo {
    Master(MasterInfo),
    Slave(SlaveInfo),
}

#[derive(Debug)]
pub struct RdbConfig {
    pub dir: String,
    pub dbfilename: String,
}

#[derive(Debug)]
pub struct ServerMetadata {
    pub listening_port: u16,
    pub replica_info: ReplicaInfo,
    pub rdb_config: Option<RdbConfig>,
}

impl ServerMetadata {
    pub fn generate(config: &Config) -> Self {
        let replica_info = match config.master_address() {
            Some((master_host, master_port)) => {
                println!("INFO: starting as slave");
                ReplicaInfo::Slave(SlaveInfo {
                    master_host,
                    master_port,
                })
            }
            None => {
                println!("INFO: starting as master");
                ReplicaInfo::Master(MasterInfo {
                    replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                })
            }
        };
        let rdb_config = match (config.get_data_dir(), config.get_dbfilename()) {
            (Some(dir), Some(dbfilename)) => Some(RdbConfig {
                dir: dir.to_string(),
                dbfilename: dbfilename.to_string(),
            }),
            _ => None,
        };
        ServerMetadata {
            listening_port: config.get_listening_port(),
            replica_info,
            rdb_config,
        }
    }

    pub fn get_replica_info(&self) -> Vec<u8> {
        match &self.replica_info {
            ReplicaInfo::Master(master_info) => {
                format!(
                    "role:master{CRLF}master_replid:{}{CRLF}master_repl_offset:{}",
                    master_info.replication_id,
                    0 // TODO: Add replication offset
                )
                .as_bytes()
                .to_vec()
            }
            ReplicaInfo::Slave(_slave_info) => b"role:slave".to_vec(),
        }
    }
}
