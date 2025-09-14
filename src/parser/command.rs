use std::time::Duration;

use super::resp::parse_buffer;
use super::resp::ParseError;
use super::resp::Result;
use super::resp::Token;

#[derive(Debug, PartialEq)]
pub enum ReplConfCommand {
    Ack(usize),
    GetAck(String),
    ListeningPort(u16),
    Capa(String),
    Other(String),
}

#[derive(Debug, PartialEq)]
pub enum ConfigCommand {
    Get(String),
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    Echo(Vec<u8>),
    Get(Vec<u8>),
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        expiry: Option<Duration>,
    },
    Info(Vec<u8>),
    ReplConf(ReplConfCommand),
    Psync,
    Wait {
        replica_count: usize,
        timeout: Duration,
    },
    Config(ConfigCommand),
}

impl Command {
    pub fn to_resp_token(&self) -> Token {
        match self {
            Command::Set { key, value, expiry } => {
                let mut tokens = vec![
                    Token::BulkString(b"set".to_vec()),
                    Token::BulkString(key.to_vec()),
                    Token::BulkString(value.to_vec()),
                ];
                if let Some(expiry) = *expiry {
                    tokens.push(Token::BulkString(b"px".to_vec()));
                    tokens.push(Token::BulkString(
                        expiry.as_millis().to_string().as_bytes().to_vec(),
                    ));
                }
                Token::Array(tokens)
            }
            Command::ReplConf(replconf_cmd) => {
                let mut tokens = vec![Token::BulkString(b"REPLCONF".to_vec())];
                match replconf_cmd {
                    ReplConfCommand::Ack(offset) => {
                        tokens.push(Token::BulkString(b"ACK".to_vec()));
                        tokens.push(Token::BulkString(offset.to_string().as_bytes().to_vec()));
                    }
                    ReplConfCommand::GetAck(offset) => {
                        tokens.push(Token::BulkString(b"GETACK".to_vec()));
                        tokens.push(Token::BulkString(offset.as_bytes().to_vec()));
                    }
                    _ => unimplemented!(),
                }
                Token::Array(tokens)
            }
            _ => unimplemented!(),
        }
    }
}

pub struct CommandResult {
    pub command: Command,
    pub len: usize,
}

fn compile_ping_command(_: &[Token]) -> Result<Command> {
    Ok(Command::Ping)
}

fn compile_echo_command(tokens: &[Token]) -> Result<Command> {
    match tokens {
        [Token::BulkString(data)] => Ok(Command::Echo(data.clone())),
        _ => Err(ParseError::Invalid)?,
    }
}

fn compile_get_command(tokens: &[Token]) -> Result<Command> {
    match tokens {
        [Token::BulkString(key)] => Ok(Command::Get(key.clone())),
        _ => Err(ParseError::Invalid)?,
    }
}

fn compile_set_command(tokens: &[Token]) -> Result<Command> {
    match tokens {
        [Token::BulkString(key), Token::BulkString(value), rest @ ..] => {
            let mut expiry = None;
            let mut iter = rest.iter();
            while let Some(token) = iter.next() {
                match token {
                    Token::BulkString(arg) => {
                        let arg = std::str::from_utf8(arg)?.to_lowercase();
                        match arg.as_str() {
                            "px" => {
                                if let Some(Token::BulkString(expiry_value)) = iter.next() {
                                    let millis = std::str::from_utf8(expiry_value)?.parse()?;
                                    expiry = Some(Duration::from_millis(millis));
                                } else {
                                    return Err(ParseError::Invalid)?;
                                }
                            }
                            _ => {
                                unimplemented!("Unsupported argument: {}", arg);
                            }
                        }
                    }
                    _ => return Err(ParseError::Invalid)?,
                }
            }
            Ok(Command::Set {
                key: key.clone(),
                value: value.clone(),
                expiry,
            })
        }
        _ => Err(ParseError::Invalid)?,
    }
}

fn compile_info_command(tokens: &[Token]) -> Result<Command> {
    match tokens {
        [Token::BulkString(section)] => Ok(Command::Info(section.clone())),
        _ => Err(ParseError::Invalid)?,
    }
}

fn compile_replconf_command(tokens: &[Token]) -> Result<Command> {
    match tokens {
        [Token::BulkString(replconf_type), rest @ ..] => {
            let replconf_type = std::str::from_utf8(replconf_type)?.to_ascii_lowercase();
            let command = match replconf_type.as_str() {
                "ack" => match rest {
                    [Token::BulkString(offset)] => {
                        ReplConfCommand::Ack(std::str::from_utf8(offset)?.parse()?)
                    }
                    _ => Err(ParseError::Invalid)?,
                },
                "listening-port" => match rest {
                    [Token::BulkString(port)] => {
                        ReplConfCommand::ListeningPort(std::str::from_utf8(port)?.parse()?)
                    }
                    _ => Err(ParseError::Invalid)?,
                },
                "capa" => match rest {
                    [Token::BulkString(capa)] => {
                        ReplConfCommand::Capa(std::str::from_utf8(capa)?.to_string())
                    }
                    _ => Err(ParseError::Invalid)?,
                },
                "getack" => match rest {
                    [Token::BulkString(ack)] => {
                        ReplConfCommand::GetAck(std::str::from_utf8(ack)?.to_string())
                    }
                    _ => Err(ParseError::Invalid)?,
                },
                _ => {
                    assert!(rest.is_empty());
                    // If there are no more tokens, we can safely ignore them
                    // and return the command as Other
                    ReplConfCommand::Other(replconf_type)
                }
            };
            Ok(Command::ReplConf(command))
        }
        _ => Err(ParseError::Invalid)?,
    }
}

fn compile_psync_command(_: &[Token]) -> Result<Command> {
    Ok(Command::Psync)
}

fn compile_wait_command(tokens: &[Token]) -> Result<Command> {
    match tokens {
        [Token::BulkString(replica_count), Token::BulkString(timeout)] => {
            let replica_count = std::str::from_utf8(replica_count)?.parse()?;
            let timeout = Duration::from_millis(std::str::from_utf8(timeout)?.parse()?);
            Ok(Command::Wait {
                replica_count,
                timeout,
            })
        }
        _ => Err(ParseError::Invalid)?,
    }
}

fn compile_config_command(tokens: &[Token]) -> Result<Command> {
    match tokens {
        [Token::BulkString(config_type), rest @ ..] => {
            let config_type = std::str::from_utf8(config_type)?.to_ascii_lowercase();
            let command = match config_type.as_str() {
                "get" => match rest {
                    [Token::BulkString(pattern)] => {
                        ConfigCommand::Get(std::str::from_utf8(pattern)?.to_string())
                    }
                    _ => Err(ParseError::Invalid)?,
                },
                _ => Err(ParseError::Invalid)?,
            };
            Ok(Command::Config(command))
        }
        _ => Err(ParseError::Invalid)?,
    }
}

fn compile_and_get_command(tokens: &[Token]) -> Result<Command> {
    let mut tokens = tokens.iter();
    let command = match tokens.next() {
        Some(Token::BulkString(command)) => {
            let rest = tokens.as_ref();
            let name = std::str::from_utf8(command)?;
            match name.to_lowercase().as_ref() {
                "ping" => compile_ping_command(rest)?,
                "echo" => compile_echo_command(rest)?,
                "get" => compile_get_command(rest)?,
                "set" => compile_set_command(rest)?,
                "info" => compile_info_command(rest)?,
                "replconf" => compile_replconf_command(rest)?,
                "psync" => compile_psync_command(rest)?,
                "wait" => compile_wait_command(rest)?,
                "config" => compile_config_command(rest)?,
                _ => Err(ParseError::Invalid)?,
            }
        }
        _ => Err(ParseError::Invalid)?,
    };
    Ok(command)
}

pub fn parse_command(message: &[u8]) -> Result<CommandResult> {
    let result = parse_buffer(message)?;
    let command = compile_and_get_command(result.tokens.as_slice())?;
    Ok(CommandResult {
        command,
        len: result.len,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ping() {
        let message = b"*1\r\n$4\r\nping\r\n";
        let result = parse_command(message).unwrap();
        assert_eq!(result.command, Command::Ping);
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_echo() {
        let message = b"*2\r\n$4\r\necho\r\n$4\r\ndata\r\n";
        let result = parse_command(message).unwrap();
        assert_eq!(result.command, Command::Echo(b"data".to_vec()));
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_get() {
        let message = b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        let result = parse_command(message).unwrap();
        assert_eq!(result.command, Command::Get(b"key".to_vec()));
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_set() {
        let message =
            b"*5\r\n$3\r\nset\r\n$5\r\nfruit\r\n$5\r\napple\r\n$2\r\npx\r\n$5\r\n65536\r\n";
        let result = parse_command(message).unwrap();
        assert_eq!(
            result.command,
            Command::Set {
                key: b"fruit".to_vec(),
                value: b"apple".to_vec(),
                expiry: Some(Duration::from_millis(65536))
            }
        );
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_set_invalid_expiry() {
        let message = b"*4\r\n$3\r\nset\r\n$5\r\nfruit\r\n$5\r\napple\r\n$2\r\npx\r\n";
        let result = parse_command(message);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_info() {
        let message = b"*2\r\n$4\r\ninfo\r\n$4\r\nkeys\r\n";
        let result = parse_command(message).unwrap();
        assert_eq!(result.command, Command::Info(b"keys".to_vec()));
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_replconf_ack() {
        let message = b"*3\r\n$8\r\nreplconf\r\n$3\r\nack\r\n$2\r\n42\r\n";
        let result = parse_command(message).unwrap();
        assert_eq!(result.command, Command::ReplConf(ReplConfCommand::Ack(42)));
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_replconf_listening_port() {
        let message = b"*3\r\n$8\r\nreplconf\r\n$14\r\nlistening-port\r\n$4\r\n4242\r\n";
        let result = parse_command(message).unwrap();
        assert_eq!(
            result.command,
            Command::ReplConf(ReplConfCommand::ListeningPort(4242))
        );
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_replconf_capa() {
        let message = b"*3\r\n$8\r\nreplconf\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        let result = parse_command(message).unwrap();
        assert_eq!(
            result.command,
            Command::ReplConf(ReplConfCommand::Capa("psync2".to_string()))
        );
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_replconf_getack() {
        let message = b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n";
        let result = parse_command(message);
        assert!(result.is_err());
        assert!(matches!(result.err().unwrap(), ParseError::Invalid));
    }

    #[test]
    fn test_parse_replconf_invalid() {
        let message = b"*3\r\n$8\r\nreplconf\r\n$5\r\ncapa\r\n$6\r\npsync2\r\n";
        let result = parse_command(message);
        assert!(result.is_err());
        assert!(matches!(result.err().unwrap(), ParseError::Invalid));
    }

    #[test]
    fn test_parse_psync() {
        let message = b"*1\r\n$5\r\npsync\r\n";
        let result = parse_command(message).unwrap();
        assert_eq!(result.command, Command::Psync);
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_multiple_commands() {
        let message_part_one = b"*1\r\n$4\r\nping\r\n";
        let message_part_two = b"*2\r\n$4\r\necho\r\n$4\r\ndata\r\n";
        let message_part_three = b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        let message_part_four =
            b"*5\r\n$3\r\nset\r\n$5\r\nfruit\r\n$5\r\napple\r\n$2\r\npx\r\n$5\r\n65536\r\n";
        let message = [
            message_part_one.as_slice(),
            message_part_two.as_slice(),
            message_part_three.as_slice(),
            message_part_four.as_slice(),
        ]
        .concat();

        let message = message.as_slice();
        let result = parse_command(message).unwrap();
        assert_eq!(result.command, Command::Ping);
        assert_eq!(result.len, message_part_one.len());

        let message = &message[result.len..];
        let result = parse_command(message).unwrap();
        assert_eq!(result.command, Command::Echo(b"data".to_vec()));
        assert_eq!(result.len, message_part_two.len());

        let message = &message[result.len..];
        let result = parse_command(message).unwrap();
        assert_eq!(result.command, Command::Get(b"key".to_vec()));
        assert_eq!(result.len, message_part_three.len());

        let message = &message[result.len..];
        let result = parse_command(message).unwrap();
        assert_eq!(
            result.command,
            Command::Set {
                key: b"fruit".to_vec(),
                value: b"apple".to_vec(),
                expiry: Some(Duration::from_millis(65536))
            }
        );
        assert_eq!(result.len, message_part_four.len());
    }
}
