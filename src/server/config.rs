use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Config {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,
    #[arg(short, long)]
    replicaof: Option<String>,
}

impl Config {
    pub fn new() -> Self {
        Config::parse()
    }

    pub fn get_listening_port(&self) -> u16 {
        self.port
    }

    pub fn master_address(&self) -> Option<(String, u16)> {
        if let Some(ref address) = self.replicaof {
            let parts = address.split_whitespace().collect::<Vec<_>>();
            match parts.as_slice() {
                [host, port] => {
                    if let Ok(port) = port.parse() {
                        return Some((host.to_string(), port));
                    }
                }
                _ => {
                    eprintln!("Invalid replicaof address format. Expected <host> <port>");
                }
            }
        }
        None
    }

    pub fn is_master(&self) -> bool {
        self.replicaof.is_none()
    }
}
