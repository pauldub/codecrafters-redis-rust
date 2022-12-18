use anyhow::{bail, Result};
use bytes::Bytes;

#[derive(PartialEq, Debug)]
pub enum Value {
    String(String),
    Number(i64),
    Bulk { size: i64, data: Bytes },
    Error(String),
    Array { len: i64, elements: Vec<Value> },
}

impl Value {
    pub fn as_string(&self) -> Result<String> {
        match self {
            Value::String(value) => Ok(value.clone()),
            Value::Bulk { data, .. } => {
                let value = String::from_utf8(data.to_vec())?;
                Ok(value)
            }
            unexpected_value => bail!("value {:?} cannot be converted to string", unexpected_value),
        }
    }
}

fn find_crlf(buf: &Bytes) -> Option<usize> {
    return buf.windows(2).position(|window| window == b"\r\n");
}

fn parse_string(buf: &mut Bytes) -> (Value, Bytes) {
    match find_crlf(&buf) {
        Some(pos) => {
            let string_value = String::from_utf8(Bytes::split_to(buf, pos).to_vec()).unwrap();
            (Value::String(string_value), Bytes::split_off(buf, 2))
        }
        None => parsing_error(buf, "string parsing failed, could not find '\\r\\n' ending"),
    }
}

fn parse_number(buf: &mut Bytes) -> (Value, Bytes) {
    match parse_string(buf) {
        (Value::String(value), rest) => (
            Value::Number(i64::from_str_radix(&value, 10).unwrap()),
            rest,
        ),
        (err @ Value::Error(_), rest) => (err, rest),
        _ => unreachable!(),
    }
}

fn parse_array(buf: &mut Bytes) -> (Value, Bytes) {
    if buf.len() < 1 {
        return parsing_error(buf, "array parsing failed, missing 'len'");
    }

    match parse_number(buf) {
        (Value::Number(len), rest) => {
            let mut element_rest = rest;
            let mut elements: Vec<Value> = vec![];

            for _ in 0..len {
                let (element, next_rest) = parse_resp(&mut element_rest);
                match element {
                    Value::Error(_) => return (element, next_rest),
                    _ => {
                        element_rest = next_rest;
                        elements.push(element);
                    }
                }
            }

            (Value::Array { len, elements }, element_rest)
        }
        (err @ Value::Error(_), rest) => (err, rest),
        _ => unreachable!(),
    }
}

fn parse_bulk_string(buf: &mut Bytes) -> (Value, Bytes) {
    if buf.len() < 1 {
        return parsing_error(buf, "bulk string parsing failed, missing 'size'");
    }

    match parse_number(buf) {
        (Value::Number(size), mut rest) => {
            let buffer_size = rest.len() as i64;
            if size > buffer_size as i64 - 2 {
                return parsing_error(&mut rest, &format!("bulk string parsing failed, cannot read {} bytes from buffer of size {} accounting for '\\r\\n' ending", size, buffer_size));
            }

            let data = Bytes::split_to(&mut rest, size.try_into().unwrap());
            match find_crlf(&rest) {
                Some(pos) => (
                    Value::Bulk { size, data },
                    Bytes::split_off(&mut rest, pos + 2),
                ),
                None => parsing_error(
                    &mut rest,
                    "bulk string failed, could not find '\\r\\n' ending",
                ),
            }
        }
        (err @ Value::Error(_), rest) => (err, rest),
        _ => unreachable!(),
    }
}

fn parsing_error(buf: &mut Bytes, message: &str) -> (Value, Bytes) {
    (Value::Error(message.to_string()), Bytes::split_off(buf, 0))
}

pub fn parse_resp(buf: &mut Bytes) -> (Value, Bytes) {
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
    use super::{parse_resp, Value};

    use bytes::Bytes;

    #[test]
    fn it_parses_a_string() {
        let mut buffer = Bytes::from("+Test\r\n+Foo\r\n");

        match parse_resp(&mut buffer) {
            (Value::String(value), rest) => {
                assert_eq!(value, "Test".to_string());
                assert_eq!(rest, Bytes::from("+Foo\r\n"))
            }
            (Value::Error(err), _) => {
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
            (Value::Array { len, elements }, rest) => {
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
            (Value::Array { len, elements }, rest) => {
                assert_eq!(len, 2);
                assert_eq!(elements.len(), 2);
                assert_eq!(
                    elements,
                    vec![
                        Value::String("hello".to_string()),
                        Value::String("world".to_string())
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
            (Value::Array { len, elements }, rest) => {
                assert_eq!(len, 3);
                assert_eq!(elements.len(), 3);
                assert_eq!(
                    elements,
                    vec![Value::Number(1), Value::Number(2), Value::Number(3),]
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
            (Value::Array { len, elements }, rest) => {
                assert_eq!(len, 2);
                assert_eq!(elements.len(), 2);
                assert_eq!(
                    elements,
                    vec![Value::Number(1), Value::String("hello".to_string())]
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
            (Value::Bulk { size, data }, rest) => {
                assert_eq!(size, 5);
                assert_eq!(data, Bytes::from("hello"));
                assert_eq!(rest, Bytes::from(""))
            }
            (Value::Error(err), _) => {
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
            (Value::Bulk { size, data }, rest) => {
                assert_eq!(size, 5);
                assert_eq!(data, Bytes::from("hello"));
                assert_eq!(rest, Bytes::from(""))
            }
            (Value::Error(err), _) => {
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
            (Value::Error(err), _) => {
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
            (Value::Error(err), _) => {
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
            (Value::Error(err), rest) => {
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
            (Value::Number(value), rest) => {
                assert_eq!(value, 1000);
                assert_eq!(rest, Bytes::from(""))
            }
            (Value::Error(err), _) => {
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
            (Value::Number(value), rest) => {
                assert_eq!(value, -1000);
                assert_eq!(rest, Bytes::from(""))
            }
            (Value::Error(err), _) => {
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
            (Value::Error(err), rest) => {
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
            (Value::Error(err), rest) => {
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
            (Value::Error(err), rest) => {
                assert_eq!(err, "parsing failed, unknown kind: ')'");
                assert_eq!(rest, Bytes::from("Foo\r\n"))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        }
    }
}
