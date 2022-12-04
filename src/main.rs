use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                loop {
                    let mut buf: [u8; 32] = [0; 32];

                    let read_bytes = stream.read(&mut buf).unwrap();

                    println!("read {} bytes", read_bytes);
                    println!("got: {:?}", String::from_utf8(buf.to_vec()));

                    println!("sending PONG");
                    stream.write_all("+PONG\r\n".as_bytes()).unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
