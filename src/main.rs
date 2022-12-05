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

                    let bytes_read = stream.read(&mut buf).unwrap();
                    if bytes_read == 0 {
                        println!("nothing to read, exiting");
                        break;
                    }

                    println!("read {} bytes", bytes_read);
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
