use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use anyhow::{bail, Result};

use crate::resp;

pub type Arguments = Vec<resp::Value>;

pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection { stream }
    }

    pub async fn read_value(&mut self) -> Result<resp::Value> {
        let mut buffer = BytesMut::with_capacity(32);
        let bytes_read = self.stream.read_buf(&mut buffer).await?;
        if bytes_read == 0 {
            bail!("client closed connection");
        }

        let (value, leftover_data) = resp::parse_resp(&mut buffer.into())?;
        if leftover_data.len() > 0 {
            println!(
                "[warn] {} leftover bytes after reading command",
                leftover_data.len()
            );
        }
        return Ok(value);
    }

    pub async fn read_command(&mut self) -> Result<(String, Arguments)> {
        let command_value = self.read_value().await?;
        match command_value {
            resp::Value::Array { len, mut elements } => {
                if len < 1 {
                    bail!("invalid command, array should have at least one element")
                }

                let raw_command_name = elements
                    .drain(0..1)
                    .next()
                    .ok_or(anyhow::format_err!("could not get command"))?;
                let command_name = raw_command_name.as_string()?.to_ascii_uppercase();
                Ok((command_name, elements))
            }
            unexpected_value => {
                bail!(
                    "unexpected value {:?} when reading command, expected an array",
                    unexpected_value
                )
            }
        }
    }

    pub async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write_all(bytes).await?;
        Ok(())
    }
}
