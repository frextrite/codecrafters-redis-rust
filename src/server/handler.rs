use std::io::Write;
use std::time::{Duration, Instant};
use std::{net::TcpStream, sync::Arc};

use crate::parser::command::ReplConfCommand;
use crate::parser::resp::Token;
use crate::replication::rdb::{get_empty_rdb, serialize_rdb};
use crate::replication::replica_manager::Replica;
use crate::server::data::LiveData;
use crate::{parser::command::Command, server::metadata::ReplicaInfo};

use super::data::Server;

pub struct CommandHandler {
    stream: TcpStream,
    state: Arc<Server>,
}

impl CommandHandler {
    pub fn new(stream: TcpStream, state: Arc<Server>) -> Self {
        CommandHandler { stream, state }
    }

    pub fn handle_command(&mut self, command: &Command) -> std::io::Result<()> {
        match command {
            Command::Ping => self.handle_ping(),
            Command::Echo(data) => self.handle_echo(data),
            Command::Get(key) => self.handle_get(key),
            Command::Set { key, value, expiry } => self.handle_set(key, value, *expiry),
            Command::Info(section) => self.handle_info(section),
            Command::ReplConf(replconf_command) => self.handle_replconf(replconf_command),
            Command::Psync => self.handle_psync(),
            Command::Wait {
                replica_count,
                timeout,
            } => self.handle_wait(*replica_count, *timeout),
        }
    }

    fn handle_ping(&mut self) -> std::io::Result<()> {
        println!("DEBUG: received PING command");
        match &self.state.metadata.replica_info {
            ReplicaInfo::Master(_) => {
                let response = Token::SimpleString("PONG".to_string());
                self.write_response(response)?;
            }
            ReplicaInfo::Slave(_) => {
                if let LiveData::Slave(data) = &mut *self.state.live_data.lock().unwrap() {
                    data.heartbeat_recv_time = Some(Instant::now());
                }
            }
        };
        Ok(())
    }

    fn handle_echo(&mut self, data: &[u8]) -> std::io::Result<()> {
        println!("DEBUG: received ECHO command with data {:?}", data);
        let response = Token::BulkString(data.to_vec());
        self.write_response(response)?;
        Ok(())
    }

    fn handle_get(&mut self, key: &[u8]) -> std::io::Result<()> {
        println!("DEBUG: received GET command with key {:?}", key);
        let response;
        {
            let store = self.state.store.lock().unwrap();
            let value = store.get(key);
            response = match value {
                Some(value) => Token::BulkString(value.to_vec()),
                None => Token::BulkString(Vec::new()),
            };
        }
        self.write_response(response)?;
        Ok(())
    }

    fn handle_set(
        &mut self,
        key: &[u8],
        value: &[u8],
        expiry: Option<Duration>,
    ) -> std::io::Result<()> {
        println!(
            "DEBUG: received SET command with key {:?} value {:?} expiry {:?}",
            key, value, expiry
        );
        self.state.set(key, value, expiry);
        // TODO: Only send reply if we are master, and not if we are a replica receiving
        // replicated commands and make this generic instead of adding if conditions
        if let ReplicaInfo::Master(_) = self.state.metadata.replica_info {
            let cmd = Command::Set {
                key: key.to_vec(),
                value: value.to_vec(),
                expiry,
            };
            let token = cmd.to_resp_token();
            let repl_data = token.serialize();

            self.state
                .replica_manager
                .lock()
                .unwrap()
                .propagate_message_to_replicas(repl_data.as_slice());

            if let LiveData::Master(data) = &mut *self.state.live_data.lock().unwrap() {
                data.replication_offset += repl_data.len();
            }

            let response = Token::SimpleString("OK".to_string());
            self.write_response(response)?;
        }
        Ok(())
    }

    fn handle_info(&mut self, section: &Vec<u8>) -> std::io::Result<()> {
        println!("DEBUG: received INFO command with section {:?}", section);
        match section.as_slice() {
            b"replication" => {
                self.write_response(Token::BulkString(self.state.metadata.get_replica_info()))?
            }
            _ => panic!("Not expecting to handle section {:?}", section),
        }
        Ok(())
    }

    fn handle_replconf(&mut self, replconf_command: &ReplConfCommand) -> std::io::Result<()> {
        println!("DEBUG: received REPLCONF command {:?}", replconf_command);
        match self.state.metadata.replica_info {
            ReplicaInfo::Master(_) => match replconf_command {
                ReplConfCommand::Ack(offset) => {
                    println!("DEBUG: received ACK from replica");
                    self.state
                        .replica_manager
                        .lock()
                        .unwrap()
                        .update_replica_offset(&self.stream, *offset);
                }
                ReplConfCommand::ListeningPort(_) | ReplConfCommand::Capa(_) => {
                    println!("DEBUG: sending OK response to REPLCONF");
                    self.write_response(Token::SimpleString("OK".to_string()))?;
                }
                _ => panic!(
                    "Not expecting to handle REPLCONF command {:?} at master",
                    replconf_command
                ),
            },
            ReplicaInfo::Slave(_) => {
                // Send REPLCONF ACK as a response to REPLCONF GETACK
                let offset = if let LiveData::Slave(data) = &*self.state.live_data.lock().unwrap() {
                    data.offset
                } else {
                    panic!("Not expecting to handle REPLCONF command at master");
                };
                let response = Command::ReplConf(ReplConfCommand::Ack(offset)).to_resp_token();
                self.write_response(response)?;
            }
        };
        Ok(())
    }

    fn handle_psync(&mut self) -> std::io::Result<()> {
        println!("DEBUG: received PSYNC command");
        match &self.state.metadata.replica_info {
            ReplicaInfo::Master(info) => {
                // 1. Send the FULLRESYNC response to the replica
                let replication_offset = match &*self.state.live_data.lock().unwrap() {
                    LiveData::Master(data) => data.replication_offset,
                    LiveData::Slave(_) => panic!("Not expecting to handle PSYNC at slave"),
                };
                let response = format!("FULLRESYNC {} {}", info.replication_id, replication_offset);
                self.write_response(Token::SimpleString(response))?;

                // 2. Send the RDB file to the replica
                let rdb_payload = serialize_rdb(&get_empty_rdb()); // TODO: move this to replication module?
                self.stream.write_all(rdb_payload.as_slice())?;

                // 3. Register the replica
                let replica = Replica::new(self.stream.try_clone()?);
                self.state
                    .replica_manager
                    .lock()
                    .unwrap()
                    .add_replica(replica); // TODO: refactor this to use register_replica and handle errors
            }
            ReplicaInfo::Slave(_) => panic!("Not expecting to handle PSYNC at slave"),
        };
        Ok(())
    }

    fn handle_wait(&mut self, replica_count: usize, timeout: Duration) -> std::io::Result<()> {
        println!(
            "DEBUG: received WAIT command with replica_count {:?} and timeout {:?}",
            replica_count, timeout
        );
        match &self.state.metadata.replica_info {
            ReplicaInfo::Master(_) => {
                if replica_count == 0 {
                    self.write_response(Token::Integer(0))?;
                }

                if let ReplicaInfo::Master(..) = self.state.metadata.replica_info {
                    // record the replication offset at the time of receiving the WAIT command
                    let master_offset = match &*self.state.live_data.lock().unwrap() {
                        LiveData::Master(data) => data.replication_offset,
                        _ => panic!("Expected master data in live_data"),
                    };

                    // Send REPLCONF GETACK to all replicas
                    println!(
                        "DEBUG: sending GETACK to {} replicas",
                        self.state
                            .replica_manager
                            .lock()
                            .unwrap()
                            .get_connected_replica_count()
                    );
                    let ack_cmd =
                        Command::ReplConf(ReplConfCommand::GetAck("*".to_string())).to_resp_token();
                    self.state
                        .replica_manager
                        .lock()
                        .unwrap()
                        .propagate_message_to_replicas(ack_cmd.serialize().as_slice());

                    // Synchronously wait for replica_count replicas to acknowledge the offset
                    println!(
                        "DEBUG: sleeping for {:?} before getting replication status",
                        timeout
                    );
                    std::thread::sleep(timeout);

                    let count_replicated = self
                        .state
                        .replica_manager
                        .lock()
                        .unwrap()
                        .get_up_to_date_replicas_count(master_offset);

                    println!(
                        "DEBUG: {} replicas have replicated till the offset {}",
                        count_replicated, master_offset
                    );

                    let response = Token::Integer(count_replicated as i64);
                    println!("DEBUG: sending WAIT response {:?}", response);
                    self.write_response(response)?;
                }
            }
            ReplicaInfo::Slave(_) => panic!("Not expecting to handle WAIT at slave"),
        };
        Ok(())
    }

    fn write_response(&mut self, response: Token) -> std::io::Result<()> {
        self.stream.write_all(&response.serialize())?;
        Ok(())
    }
}
