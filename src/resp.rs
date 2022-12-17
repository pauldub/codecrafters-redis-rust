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
                    Bytes::split_to(buf, 0),
                ),
            }
        }
        kind => (
            Kind::Error(format!(
                "parsing failed, unknown type: '{}'",
                char::from(kind)
            )),
            buf.clone(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse, Kind};

    use bytes::Bytes;

    #[test]
    fn it_parses_a_string() {
        let mut buffer = Bytes::from("+Test\r\n");

        match parse(&mut buffer) {
            (Kind::String(value), rest) => {
                assert_eq!(value, "Test".to_string());
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
}
