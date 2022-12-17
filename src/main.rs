pub mod resp;

use std::net;

use bytes::BytesMut;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn handle_client(socket: &mut TcpStream) {
    println!("accepted new connection");
    loop {
        let mut buffer = BytesMut::with_capacity(32);
        let bytes_read = socket.read_buf(&mut buffer).await.unwrap();
        if bytes_read == 0 {
            println!("client closed connection");
            break;
        }

        println!("read {} bytes", bytes_read);

        match resp::parse_resp(&mut buffer.into()) {
            (resp::Kind::Array { len, elements }, _) => {
                if len < 1 {
                    println!("empty RESP array received");
                    continue;
                }

                println!("element: {:?}", elements);

                match elements.get(0) {
                    Some(resp::Kind::Bulk {
                        data: command_data, ..
                    }) => {
                        match String::from_utf8(command_data.to_vec())
                            .expect("failed to parse command as utf-8 string")
                            .to_uppercase()
                            .as_str()
                        {
                            "PING" => {
                                println!("sending PONG");
                                socket.write_all("+PONG\r\n".as_bytes()).await.unwrap();
                            }
                            "ECHO" => {
                                if elements.len() > 2 {
                                    socket
                                        .write_all(
                                            "-wrong number of arguments for command\r\n".as_bytes(),
                                        )
                                        .await
                                        .unwrap();

                                    continue;
                                }
                                println!("replying to ECHO");
                                match elements.get(1) {
                                    Some(resp::Kind::Bulk {
                                        data: reply_data, ..
                                    }) => {
                                        socket
                                            .write_all(
                                                format!("${}\r\n", reply_data.len()).as_bytes(),
                                            )
                                            .await
                                            .unwrap();
                                        socket.write_all(reply_data).await.unwrap();
                                        socket.write_all(b"\r\n").await.unwrap();
                                    }
                                    Some(value) => {
                                        println!("unexpected RESP value: {:?}", value)
                                    }
                                    None => unreachable!(),
                                }
                            }
                            command => {
                                println!("unhandled command: {}", command);
                                socket.write_all("+OK\r\n".as_bytes()).await.unwrap();
                            }
                        }
                    }
                    Some(value) => {
                        println!("unexpected RESP value: {:?}", value)
                    }
                    None => unreachable!(),
                }
            }
            (value, _) => {
                println!("unexpected RESP value: {:?}", value)
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Logs from your program will appear here!");

    let std_listener = net::TcpListener::bind("127.0.0.1:6379")?;
    let mut listener = TcpListener::from_std(std_listener)?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move { handle_client(&mut socket).await });
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{thread::{spawn}, net::{TcpStream, Shutdown}};
//     use super::main;

//     #[test]
//     fn it_listens_on_port_6379() {
//         let handle = spawn(|| {
//             main().expect("something went wrong");
//         });

//         let connection = TcpStream::connect("127.0.0.1:6379").expect("failed to connect to server");
//         connection.shutdown(Shutdown::Both).expect("failed to close connection");

//         handle.join();
//     }

// }
