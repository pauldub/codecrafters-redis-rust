use bytes::Bytes;

#[derive(PartialEq, Debug)]
pub enum Kind {
    String(String),
    Number(i64),
    Bulk { size: i64, data: Bytes },
    Error(String),
    Array { len: i64, elements: Vec<Kind> },
}

fn find_crlf(buf: &Bytes) -> Option<usize> {
    return buf.windows(2).position(|window| window == b"\r\n");
}

fn parse_string(buf: &mut Bytes) -> (Kind, Bytes) {
    match find_crlf(&buf) {
        Some(pos) => {
            let string_value = String::from_utf8(Bytes::split_to(buf, pos).to_vec()).unwrap();
            (Kind::String(string_value), Bytes::split_off(buf, 2))
        }
        None => parsing_error(buf, "string parsing failed, could not find '\\r\\n' ending"),
    }
}

fn parse_number(buf: &mut Bytes) -> (Kind, Bytes) {
    match parse_string(buf) {
        (Kind::String(value), rest) => {
            (Kind::Number(i64::from_str_radix(&value, 10).unwrap()), rest)
        }
        (err @ Kind::Error(_), rest) => (err, rest),
        _ => unreachable!(),
    }
}

fn parse_array(buf: &mut Bytes) -> (Kind, Bytes) {
    if buf.len() < 1 {
        return parsing_error(buf, "array parsing failed, missing 'len'");
    }

    match parse_number(buf) {
        (Kind::Number(len), rest) => {
            let mut element_rest = rest;
            let mut elements: Vec<Kind> = vec![];

            for _ in 0..len {
                let (element, next_rest) = parse_resp(&mut element_rest);
                match element {
                    Kind::Error(_) => return (element, next_rest),
                    _ => {
                        element_rest = next_rest;
                        elements.push(element);
                    }
                }
            }

            (Kind::Array { len, elements }, element_rest)
        }
        (err @ Kind::Error(_), rest) => (err, rest),
        _ => unreachable!(),
    }
}

fn parse_bulk_string(buf: &mut Bytes) -> (Kind, Bytes) {
    if buf.len() < 1 {
        return parsing_error(buf, "bulk string parsing failed, missing 'size'");
    }

    match parse_number(buf) {
        (Kind::Number(size), mut rest) => {
            let buffer_size = rest.len() as i64;
            if size > buffer_size as i64 - 2 {
                return parsing_error(&mut rest, &format!("bulk string parsing failed, cannot read {} bytes from buffer of size {} accounting for '\\r\\n' ending", size, buffer_size));
            }

            let data = Bytes::split_to(&mut rest, size.try_into().unwrap());
            match find_crlf(&rest) {
                Some(pos) => (
                    Kind::Bulk { size, data },
                    Bytes::split_off(&mut rest, pos + 2),
                ),
                None => parsing_error(
                    &mut rest,
                    "bulk string failed, could not find '\\r\\n' ending",
                ),
            }
        }
        (err @ Kind::Error(_), rest) => (err, rest),
        _ => unreachable!(),
    }
}

fn parsing_error(buf: &mut Bytes, message: &str) -> (Kind, Bytes) {
    (Kind::Error(message.to_string()), Bytes::split_off(buf, 0))
}

pub fn parse_resp(buf: &mut Bytes) -> (Kind, Bytes) {
    if buf.len() < 1 {
        return parsing_error(buf, "empty buffer");
    }

    match Bytes::split_to(buf, 1)[0] {
        b'+' => parse_string(buf),
        b'*' => parse_array(buf),
        b':' => parse_number(buf),
        b'$' => parse_bulk_string(buf),
        kind => parsing_error(
            buf,
            &format!("parsing failed, unknown kind: '{}'", char::from(kind)),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_resp, Kind};

    use bytes::Bytes;

    #[test]
    fn it_parses_a_string() {
        let mut buffer = Bytes::from("+Test\r\n+Foo\r\n");

        match parse_resp(&mut buffer) {
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
    fn it_parses_an_empty_array() {
        let mut buffer = Bytes::from("*0\r\n");
        match parse_resp(&mut buffer) {
            (Kind::Array { len, elements }, rest) => {
                assert_eq!(len, 0);
                assert_eq!(elements.len(), 0);
                assert_eq!(rest, Bytes::from(""))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        };
    }

    #[test]
    fn it_parses_a_string_array() {
        let mut buffer = Bytes::from("*2\r\n+hello\r\n+world\r\n");
        match parse_resp(&mut buffer) {
            (Kind::Array { len, elements }, rest) => {
                assert_eq!(len, 2);
                assert_eq!(elements.len(), 2);
                assert_eq!(
                    elements,
                    vec![
                        Kind::String("hello".to_string()),
                        Kind::String("world".to_string())
                    ]
                );
                assert_eq!(rest, Bytes::from(""))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        };
    }

    #[test]
    fn it_parses_a_number_array() {
        let mut buffer = Bytes::from("*3\r\n:1\r\n:2\r\n:3\r\n");
        match parse_resp(&mut buffer) {
            (Kind::Array { len, elements }, rest) => {
                assert_eq!(len, 3);
                assert_eq!(elements.len(), 3);
                assert_eq!(
                    elements,
                    vec![Kind::Number(1), Kind::Number(2), Kind::Number(3),]
                );
                assert_eq!(rest, Bytes::from(""))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        };
    }

    #[test]
    fn it_parses_a_mixed_array() {
        let mut buffer = Bytes::from("*2\r\n:1\r\n+hello\r\n");
        match parse_resp(&mut buffer) {
            (Kind::Array { len, elements }, rest) => {
                assert_eq!(len, 2);
                assert_eq!(elements.len(), 2);
                assert_eq!(
                    elements,
                    vec![Kind::Number(1), Kind::String("hello".to_string())]
                );
                assert_eq!(rest, Bytes::from(""))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        };
    }

    #[test]
    fn it_parses_a_bulk_string() {
        let mut buffer = Bytes::from("$5\r\nhello\r\n");

        match parse_resp(&mut buffer) {
            (Kind::Bulk { size, data }, rest) => {
                assert_eq!(size, 5);
                assert_eq!(data, Bytes::from("hello"));
                assert_eq!(rest, Bytes::from(""))
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
    fn it_parses_a_bulk_string_ignoring_extra_data() {
        let mut buffer = Bytes::from("$5\r\nhello world\r\n");

        match parse_resp(&mut buffer) {
            (Kind::Bulk { size, data }, rest) => {
                assert_eq!(size, 5);
                assert_eq!(data, Bytes::from("hello"));
                assert_eq!(rest, Bytes::from(""))
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
    fn it_returns_an_error_if_reading_a_bulk_string_goes_out_of_bound() {
        let mut buffer = Bytes::from("$5\r\nh");

        match parse_resp(&mut buffer) {
            (Kind::Error(err), _) => {
                assert_eq!(
                    err,
                    "bulk string parsing failed, cannot read 5 bytes from buffer of size 1 accounting for '\\r\\n' ending"
                );
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        };
    }

    #[test]
    fn it_returns_an_error_if_reading_a_bulk_string_goes_out_of_bound_accounting_for_ending() {
        let mut buffer = Bytes::from("$5\r\nh\r\n");

        match parse_resp(&mut buffer) {
            (Kind::Error(err), _) => {
                assert_eq!(
                    err,
                    "bulk string parsing failed, cannot read 5 bytes from buffer of size 3 accounting for '\\r\\n' ending"
                );
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        };
    }

    #[test]
    fn it_returns_an_error_on_invalid_length_array() {
        let mut buffer = Bytes::from("*2\r\n+hello\r\n");
        match parse_resp(&mut buffer) {
            (Kind::Error(err), rest) => {
                assert_eq!(err, "empty buffer");
                assert_eq!(rest, Bytes::from(""))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        };
    }

    #[test]
    fn it_parses_a_number() {
        let mut buffer = Bytes::from(":1000\r\n");

        match parse_resp(&mut buffer) {
            (Kind::Number(value), rest) => {
                assert_eq!(value, 1000);
                assert_eq!(rest, Bytes::from(""))
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
    fn it_parses_a_negative_number() {
        let mut buffer = Bytes::from(":-1000\r\n");

        match parse_resp(&mut buffer) {
            (Kind::Number(value), rest) => {
                assert_eq!(value, -1000);
                assert_eq!(rest, Bytes::from(""))
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
        match parse_resp(&mut buffer) {
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
        match parse_resp(&mut buffer) {
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
        match parse_resp(&mut buffer) {
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
