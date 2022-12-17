use bytes::Bytes;

#[derive(Debug)]
pub enum Kind {
    String(String),
    Integer(i64),
    Bulk { size: u64, data: Vec<u8> },
    Error(String),
    Array { count: u64, elements: Vec<Kind> },
}

pub fn parse(buf: &mut Bytes) -> (Kind, Bytes) {
    if buf.len() < 1 {
        return (
            Kind::Error("empty buffer".to_string()),
            Bytes::split_off(buf, 0),
        );
    }

    match Bytes::split_to(buf, 1)[0] {
        b'+' => {
            let string_end = buf.windows(2).position(|window| window == b"\r\n");
            match string_end {
                Some(pos) => {
                    let string_value =
                        String::from_utf8(Bytes::split_to(buf, pos).to_vec()).unwrap();
                    (Kind::String(string_value), Bytes::split_off(buf, 2))
                }
                None => (
                    Kind::Error(
                        "string parsing failed, could not find '\\r\\n' ending".to_string(),
                    ),
                    Bytes::split_off(buf, 0),
                ),
            }
        }
        kind => (
            Kind::Error(format!(
                "parsing failed, unknown kind: '{}'",
                char::from(kind)
            )),
            Bytes::split_off(buf, 0),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse, Kind};

    use bytes::Bytes;

    #[test]
    fn it_parses_a_string() {
        let mut buffer = Bytes::from("+Test\r\n+Foo\r\n");

        match parse(&mut buffer) {
            (Kind::String(value), rest) => {
                assert_eq!(value, "Test".to_string());
                assert_eq!(rest, Bytes::from("+Foo\r\n"))
            }
            (Kind::Error(err), _) => {
                panic!("parsing error: {}", err);
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        };
    }

    #[test]
    fn it_returns_an_error_on_missing_crlf() {
        let mut buffer = Bytes::from("+Test");
        match parse(&mut buffer) {
            (Kind::Error(err), rest) => {
                assert_eq!(err, "string parsing failed, could not find '\\r\\n' ending");
                assert_eq!(rest, Bytes::from("Test"))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        }
    }

    #[test]
    fn it_returns_an_error_on_empty_input() {
        let mut buffer = Bytes::from("");
        match parse(&mut buffer) {
            (Kind::Error(err), rest) => {
                assert_eq!(err, "empty buffer");
                assert_eq!(rest, Bytes::from(""))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        }
    }

    #[test]
    fn it_returns_an_error_on_unknown_kind() {
        let mut buffer = Bytes::from(")Foo\r\n");
        match parse(&mut buffer) {
            (Kind::Error(err), rest) => {
                assert_eq!(err, "parsing failed, unknown kind: ')'");
                assert_eq!(rest, Bytes::from("Foo\r\n"))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        }
    }
}
