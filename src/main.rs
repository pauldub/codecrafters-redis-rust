use std::net;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn handle_client(socket: &mut TcpStream) {
    println!("accepted new connection");
    loop {
        let mut buf: [u8; 32] = [0; 32];

        let bytes_read = socket.read(&mut buf).await.unwrap();
        if bytes_read == 0 {
            println!("client closed connection");
            break;
        }

        println!("read {} bytes", bytes_read);
        println!("got: {:?}", String::from_utf8(buf.to_vec()));

        println!("sending PONG");
        socket.write_all("+PONG\r\n".as_bytes()).await.unwrap();
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
