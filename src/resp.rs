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

type ParserState = (Value, Bytes);

fn parse_string(buf: &mut Bytes) -> Result<ParserState> {
    match find_crlf(&buf) {
        Some(pos) => {
            let string_value = String::from_utf8(Bytes::split_to(buf, pos).to_vec())?;
            Ok((Value::String(string_value), Bytes::split_off(buf, 2)))
        }
        None => bail!("string parsing failed, could not find '\\r\\n' ending"),
    }
}

fn parse_number(buf: &mut Bytes) -> Result<ParserState> {
    match parse_string(buf)? {
        (Value::String(value), rest) => Ok((
            Value::Number(i64::from_str_radix(&value, 10).unwrap()),
            rest,
        )),
        _ => bail!("number parsing failed, unexpected value type"),
    }
}

fn parse_array(buf: &mut Bytes) -> Result<ParserState> {
    if buf.len() < 1 {
        bail!("array parsing failed, missing 'len'");
    }

    match parse_number(buf)? {
        (Value::Number(len), rest) => {
            let mut leftover_data = rest;
            let mut elements: Vec<Value> = vec![];

            for _ in 0..len {
                let (element, element_leftover_data) = parse_resp(&mut leftover_data)?;
                leftover_data = element_leftover_data;
                elements.push(element);
            }

            Ok((Value::Array { len, elements }, leftover_data))
        }
        _ => bail!("array parsing failed, could not parse 'len' as a number"),
    }
}

fn parse_bulk_string(buf: &mut Bytes) -> Result<ParserState> {
    if buf.len() < 1 {
        bail!("bulk string parsing failed, missing 'size'");
    }

    match parse_number(buf)? {
        (Value::Number(size), mut rest) => {
            let buffer_size = rest.len() as i64;
            if size > buffer_size as i64 - 2 {
                bail!("bulk string parsing failed, cannot read {} bytes from buffer of size {} accounting for '\\r\\n' ending", size, buffer_size);
            }

            let data = Bytes::split_to(&mut rest, size.try_into()?);
            let end_pos = find_crlf(&rest).ok_or(anyhow::format_err!(
                "bulk string failed, could not find '\\r\\n' ending"
            ))?;

            Ok((
                Value::Bulk { size, data },
                Bytes::split_off(&mut rest, end_pos + 2),
            ))
        }
        _ => bail!("bulk string parsing failed, could not parse 'size' as a number"),
    }
}

fn parsing_error(buf: &mut Bytes, message: &str) -> Result<ParserState> {
    Ok((Value::Error(message.to_string()), Bytes::split_off(buf, 0)))
}

pub fn parse_resp(buf: &mut Bytes) -> Result<ParserState> {
    if buf.len() < 1 {
        bail!("empty buffer");
    }

    match Bytes::split_to(buf, 1)[0] {
        b'+' => parse_string(buf),
        b'*' => parse_array(buf),
        b':' => parse_number(buf),
        b'$' => parse_bulk_string(buf),
        kind => bail!("parsing failed, unknown kind: '{}'", char::from(kind)),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_resp, Value};

    use anyhow::Result;
    use bytes::Bytes;

    #[test]
    fn it_parses_a_string() -> Result<()> {
        let mut buffer = Bytes::from("+Test\r\n+Foo\r\n");

        match parse_resp(&mut buffer)? {
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

        Ok(())
    }

    #[test]
    fn it_parses_an_empty_array() -> Result<()> {
        let mut buffer = Bytes::from("*0\r\n");
        match parse_resp(&mut buffer)? {
            (Value::Array { len, elements }, rest) => {
                assert_eq!(len, 0);
                assert_eq!(elements.len(), 0);
                assert_eq!(rest, Bytes::from(""))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        };
        Ok(())
    }

    #[test]
    fn it_parses_a_string_array() -> Result<()> {
        let mut buffer = Bytes::from("*2\r\n+hello\r\n+world\r\n");
        match parse_resp(&mut buffer)? {
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

        Ok(())
    }

    #[test]
    fn it_parses_a_number_array() -> Result<()> {
        let mut buffer = Bytes::from("*3\r\n:1\r\n:2\r\n:3\r\n");
        match parse_resp(&mut buffer)? {
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

        Ok(())
    }

    #[test]
    fn it_parses_a_mixed_array() -> Result<()> {
        let mut buffer = Bytes::from("*2\r\n:1\r\n+hello\r\n");
        match parse_resp(&mut buffer)? {
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

        Ok(())
    }

    #[test]
    fn it_parses_a_bulk_string() -> Result<()> {
        let mut buffer = Bytes::from("$5\r\nhello\r\n");

        match parse_resp(&mut buffer)? {
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

        Ok(())
    }

    #[test]
    fn it_parses_a_bulk_string_ignoring_extra_data() -> Result<()> {
        let mut buffer = Bytes::from("$5\r\nhello world\r\n");

        match parse_resp(&mut buffer)? {
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

        Ok(())
    }

    #[test]
    fn it_returns_an_error_if_reading_a_bulk_string_goes_out_of_bound() -> Result<()> {
        let mut buffer = Bytes::from("$5\r\nh");

        match parse_resp(&mut buffer)? {
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

        Ok(())
    }

    #[test]
    fn it_returns_an_error_if_reading_a_bulk_string_goes_out_of_bound_accounting_for_ending(
    ) -> Result<()> {
        let mut buffer = Bytes::from("$5\r\nh\r\n");

        match parse_resp(&mut buffer)? {
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

        Ok(())
    }

    #[test]
    fn it_returns_an_error_on_invalid_length_array() -> Result<()> {
        let mut buffer = Bytes::from("*2\r\n+hello\r\n");
        match parse_resp(&mut buffer)? {
            (Value::Error(err), rest) => {
                assert_eq!(err, "empty buffer");
                assert_eq!(rest, Bytes::from(""))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        };

        Ok(())
    }

    #[test]
    fn it_parses_a_number() -> Result<()> {
        let mut buffer = Bytes::from(":1000\r\n");

        match parse_resp(&mut buffer)? {
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

        Ok(())
    }

    #[test]
    fn it_parses_a_negative_number() -> Result<()> {
        let mut buffer = Bytes::from(":-1000\r\n");

        match parse_resp(&mut buffer)? {
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

        Ok(())
    }

    #[test]
    fn it_returns_an_error_on_missing_crlf() -> Result<()> {
        let mut buffer = Bytes::from("+Test");
        match parse_resp(&mut buffer)? {
            (Value::Error(err), rest) => {
                assert_eq!(err, "string parsing failed, could not find '\\r\\n' ending");
                assert_eq!(rest, Bytes::from("Test"))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        }

        Ok(())
    }

    #[test]
    fn it_returns_an_error_on_empty_input() -> Result<()> {
        let mut buffer = Bytes::from("");
        match parse_resp(&mut buffer)? {
            (Value::Error(err), rest) => {
                assert_eq!(err, "empty buffer");
                assert_eq!(rest, Bytes::from(""))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        }

        Ok(())
    }

    #[test]
    fn it_returns_an_error_on_unknown_kind() -> Result<()> {
        let mut buffer = Bytes::from(")Foo\r\n");
        match parse_resp(&mut buffer)? {
            (Value::Error(err), rest) => {
                assert_eq!(err, "parsing failed, unknown kind: ')'");
                assert_eq!(rest, Bytes::from("Foo\r\n"))
            }
            (kind, rest) => {
                panic!("unexpected kind: {:?} read_bytes: {:?}", kind, rest)
            }
        }

        Ok(())
    }
}
