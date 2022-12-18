mod connection;
mod resp;

use std::net;

use anyhow::Result;

use tokio::net::{TcpListener, TcpStream};

use connection::Connection;

async fn handle_client(socket: TcpStream) -> Result<()> {
    println!("accepted new connection");

    let mut conn = Connection::new(socket);

    loop {
        let (command, args) = conn.read_command().await?;
        match command.as_str() {
            "PING" => {
                println!("sending PONG");
                conn.write_all("+PONG\r\n".as_bytes()).await?;
            }
            "ECHO" => {
                if args.len() != 1 {
                    conn.write_all("-wrong number of arguments for command\r\n".as_bytes())
                        .await?;

                    continue;
                }
                println!("replying to ECHO");
                match args.get(0) {
                    Some(resp::Value::Bulk {
                        data: reply_data, ..
                    }) => {
                        conn.write_all(format!("${}\r\n", reply_data.len()).as_bytes())
                            .await?;
                        conn.write_all(reply_data).await?;
                        conn.write_all(b"\r\n").await?;
                    }
                    Some(value) => {
                        println!("unexpected RESP value: {:?}", value)
                    }
                    None => unreachable!(),
                }
            }
            _unsupported_command => {
                conn.write_all("-unsupported command\r\n".as_bytes())
                    .await?;
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
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async move { handle_client(socket).await.unwrap() });
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
