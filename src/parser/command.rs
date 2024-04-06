use std::time::Duration;

use super::resp::parse_buffer;
use super::resp::ParseError;
use super::resp::Result;
use super::resp::Token;

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
    ReplConf,
    Psync,
}

pub struct CommandResult {
    pub command: Command,
    pub len: usize,
}

fn compile_and_get_command(tokens: &[Token]) -> Result<Command> {
    let mut tokens = tokens.iter();
    let command = match tokens.next() {
        Some(Token::BulkString(command)) => {
            let command = std::str::from_utf8(command).map_err(|_| ParseError::Invalid)?;
            match command.to_lowercase().as_ref() {
                "ping" => Command::Ping,
                "echo" => Command::Echo(
                    tokens
                        .next()
                        .ok_or(ParseError::Invalid)?
                        .get_bulk_string_data()?
                        .clone(),
                ),
                "get" => Command::Get(
                    tokens
                        .next()
                        .ok_or(ParseError::Invalid)?
                        .get_bulk_string_data()?
                        .clone(),
                ),
                "set" => {
                    let key = tokens
                        .next()
                        .ok_or(ParseError::Invalid)?
                        .get_bulk_string_data()?
                        .clone();
                    let value = tokens
                        .next()
                        .ok_or(ParseError::Invalid)?
                        .get_bulk_string_data()?
                        .clone();
                    let expiry: Option<Result<Duration>> = tokens.next().map(|token| {
                        let arg = std::str::from_utf8(token.get_bulk_string_data()?)
                            .map_err(|_| ParseError::Invalid)?;
                        assert!(arg.to_lowercase() == "px");
                        let millis = std::str::from_utf8(
                            tokens
                                .next()
                                .ok_or(ParseError::Invalid)?
                                .get_bulk_string_data()?,
                        )
                        .map_err(|_| ParseError::Invalid)?
                        .parse()?;
                        Ok(Duration::from_millis(millis))
                    });
                    let expiry = match expiry {
                        Some(res) => Some(res?),
                        None => None,
                    };
                    Command::Set { key, value, expiry }
                }
                "info" => Command::Info(
                    tokens
                        .next()
                        .ok_or(ParseError::Invalid)?
                        .get_bulk_string_data()?
                        .clone(),
                ),
                "replconf" => Command::ReplConf,
                "psync" => Command::Psync,
                _ => Err(ParseError::Invalid)?,
            }
        }
        _ => Err(ParseError::Invalid)?,
    };
    Ok(command)
}

pub fn parse_message(message: &[u8]) -> Result<CommandResult> {
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
        let result = parse_message(message).unwrap();
        assert_eq!(result.command, Command::Ping);
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_echo() {
        let message = b"*2\r\n$4\r\necho\r\n$4\r\ndata\r\n";
        let result = parse_message(message).unwrap();
        assert_eq!(result.command, Command::Echo(b"data".to_vec()));
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_get() {
        let message = b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        let result = parse_message(message).unwrap();
        assert_eq!(result.command, Command::Get(b"key".to_vec()));
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_set() {
        let message =
            b"*5\r\n$3\r\nset\r\n$5\r\nfruit\r\n$5\r\napple\r\n$2\r\npx\r\n$5\r\n65536\r\n";
        let result = parse_message(message).unwrap();
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
        let result = parse_message(message);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_info() {
        let message = b"*2\r\n$4\r\ninfo\r\n$4\r\nkeys\r\n";
        let result = parse_message(message).unwrap();
        assert_eq!(result.command, Command::Info(b"keys".to_vec()));
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_replconf() {
        let message = b"*1\r\n$8\r\nreplconf\r\n";
        let result = parse_message(message).unwrap();
        assert_eq!(result.command, Command::ReplConf);
        assert_eq!(result.len, message.len());
    }

    #[test]
    fn test_parse_psync() {
        let message = b"*1\r\n$5\r\npsync\r\n";
        let result = parse_message(message).unwrap();
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
        let result = parse_message(message).unwrap();
        assert_eq!(result.command, Command::Ping);
        assert_eq!(result.len, message_part_one.len());

        let message = &message[result.len..];
        let result = parse_message(message).unwrap();
        assert_eq!(result.command, Command::Echo(b"data".to_vec()));
        assert_eq!(result.len, message_part_two.len());

        let message = &message[result.len..];
        let result = parse_message(message).unwrap();
        assert_eq!(result.command, Command::Get(b"key".to_vec()));
        assert_eq!(result.len, message_part_three.len());

        let message = &message[result.len..];
        let result = parse_message(message).unwrap();
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
