use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    store::{SetCondition, SetExpiry, SetOptions, Store},
};

const OK: &[u8] = b"OK";

enum BatchCondition {
    Always,
    Nx,
    Xx,
}

enum BatchExpiry {
    None,
    Ex(u64),
    Px(u64),
    ExAt(u64),
    PxAt(u64),
    KeepTtl,
}

#[inline]
pub fn mget(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'mget' command",
        ));
    }

    let mut out: SmallVec<[Response; 16]> = SmallVec::with_capacity(args.len().min(65_536));
    for frame in args {
        let key = arg_bytes(frame)?;
        let value = store.get(key).cloned();
        if let Some(ref value) = value {
            ensure_string_value(value)?;
        }
        out.push(Response::Value(value));
    }

    Ok(Response::Array(Box::new(out)))
}

#[inline]
pub fn mset(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'mset' command",
        ));
    }

    let mut index = 0usize;
    while index < args.len() {
        let key = parse_key(arg_bytes(&args[index])?)?;
        let value = SenkoValue::encode_attempt(arg_bytes(&args[index + 1])?);
        let _ = store.set(
            key,
            value,
            SetOptions {
                condition: SetCondition::Always,
                expiry: SetExpiry::None,
                get_old: false,
            },
        );
        index += 2;
    }

    Ok(Response::Simple(OK))
}

#[inline]
pub fn msetnx(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'msetnx' command",
        ));
    }

    let mut index = 0usize;
    while index < args.len() {
        let key = arg_bytes(&args[index])?;
        if store.get(key).is_some() {
            return Ok(Response::Integer(0));
        }
        index += 2;
    }

    let mut index = 0usize;
    while index < args.len() {
        let key = parse_key(arg_bytes(&args[index])?)?;
        let value = SenkoValue::encode_attempt(arg_bytes(&args[index + 1])?);
        let _ = store.set(
            key,
            value,
            SetOptions {
                condition: SetCondition::Always,
                expiry: SetExpiry::None,
                get_old: false,
            },
        );
        index += 2;
    }

    Ok(Response::Integer(1))
}

#[inline]
pub fn msetex(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'msetex' command",
        ));
    }

    let numkeys = parse_usize(arg_bytes(&args[0])?).map_err(|_| {
        SenkoError::ProtocolMessage(CompactString::new(
            "ERR numkeys value is not an integer or out of range",
        ))
    })?;

    if numkeys == 0 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR numkeys should be greater than 0",
        )));
    }

    let pair_args = numkeys
        .checked_mul(2)
        .ok_or_else(|| SenkoError::Protocol("syntax error"))?;

    if args.len() < 1 + pair_args {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR numkeys does not match number of key-value pairs",
        )));
    }

    let mut condition = BatchCondition::Always;
    let mut condition_set = false;
    let mut expiry = BatchExpiry::None;
    let mut expiry_set = false;

    let mut option_index = 1 + pair_args;
    while option_index < args.len() {
        let token = arg_bytes(&args[option_index])?;

        if is_opt(token, b"NX") || is_opt(token, b"XX") {
            if condition_set {
                return Err(SenkoError::Protocol("syntax error"));
            }
            condition_set = true;
            condition = if is_opt(token, b"NX") {
                BatchCondition::Nx
            } else {
                BatchCondition::Xx
            };
            option_index += 1;
            continue;
        }

        if is_opt(token, b"EX")
            || is_opt(token, b"PX")
            || is_opt(token, b"EXAT")
            || is_opt(token, b"PXAT")
            || is_opt(token, b"KEEPTTL")
        {
            if expiry_set {
                return Err(SenkoError::Protocol("syntax error"));
            }
            expiry_set = true;
            if is_opt(token, b"KEEPTTL") {
                expiry = BatchExpiry::KeepTtl;
                option_index += 1;
                continue;
            }
            option_index += 1;
            if option_index >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            let value = parse_positive_u64(arg_bytes(&args[option_index])?, "msetex")?;
            expiry = if is_opt(token, b"EX") {
                BatchExpiry::Ex(value)
            } else if is_opt(token, b"PX") {
                BatchExpiry::Px(value)
            } else if is_opt(token, b"EXAT") {
                BatchExpiry::ExAt(value)
            } else {
                BatchExpiry::PxAt(value)
            };
            option_index += 1;
            continue;
        }

        return Err(SenkoError::Protocol("syntax error"));
    }

    let mut precheck_index = 0usize;
    let mut present = 0usize;
    while precheck_index < pair_args {
        let key = arg_bytes(&args[1 + precheck_index])?;
        if store.get(key).is_some() {
            present += 1;
        }
        precheck_index += 2;
    }

    match condition {
        BatchCondition::Nx if present > 0 => return Ok(Response::Integer(0)),
        BatchCondition::Xx if present != numkeys => return Ok(Response::Integer(0)),
        _ => {}
    }

    let store_expiry = match expiry {
        BatchExpiry::None => SetExpiry::None,
        BatchExpiry::Ex(value) => SetExpiry::Ex(value),
        BatchExpiry::Px(value) => SetExpiry::Px(value),
        BatchExpiry::ExAt(value) => SetExpiry::ExAt(value),
        BatchExpiry::PxAt(value) => SetExpiry::PxAt(value),
        BatchExpiry::KeepTtl => SetExpiry::KeepTtl,
    };

    let mut set_index = 0usize;
    while set_index < pair_args {
        let key = parse_key(arg_bytes(&args[1 + set_index])?)?;
        let value = SenkoValue::encode_attempt(arg_bytes(&args[1 + set_index + 1])?);
        let _ = store.set(
            key,
            value,
            SetOptions {
                condition: SetCondition::Always,
                expiry: store_expiry,
                get_old: false,
            },
        );
        set_index += 2;
    }

    Ok(Response::Integer(numkeys as i64))
}

fn arg_bytes<'a>(frame: &'a Frame<'_>) -> SenkoResult<&'a [u8]> {
    match frame {
        Frame::BulkString(bytes) | Frame::SimpleString(bytes) => Ok(bytes),
        Frame::VerbatimString { data, .. } => Ok(data),
        _ => Err(SenkoError::WrongType {
            expected: "string",
            actual: frame_type_name(frame),
        }),
    }
}

fn parse_key(raw: &[u8]) -> SenkoResult<CompactString> {
    let key = std::str::from_utf8(raw).map_err(|_| SenkoError::Protocol("invalid UTF-8 key"))?;
    Ok(CompactString::new(key))
}

fn parse_positive_u64(raw: &[u8], cmd: &'static str) -> SenkoResult<u64> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            SenkoError::ProtocolMessage(CompactString::new(format!(
                "invalid expire time in '{cmd}' command"
            )))
        })?;
    Ok(value)
}

fn parse_usize(raw: &[u8]) -> Result<usize, ()> {
    std::str::from_utf8(raw)
        .map_err(|_| ())?
        .parse::<usize>()
        .map_err(|_| ())
}

fn is_opt(input: &[u8], expected_upper: &[u8]) -> bool {
    input.eq_ignore_ascii_case(expected_upper)
}

fn ensure_string_value(value: &SenkoValue) -> SenkoResult<()> {
    match value {
        SenkoValue::Raw(_) | SenkoValue::Int(_) | SenkoValue::Float(_) => Ok(()),
        SenkoValue::Hash(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "hash",
        }),
        SenkoValue::List(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "list",
        }),
        SenkoValue::Set(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "set",
        }),
        SenkoValue::Stream(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "stream",
        }),
        SenkoValue::ZSet(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "zset",
        }),
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "MBbloom--",
        }),
        #[cfg(feature = "prob")]
        SenkoValue::CuckooFilter(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "cuckooFilter",
        }),
        #[cfg(feature = "prob")]
        SenkoValue::CountMinSketch(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "CMSk--",
        }),
        #[cfg(feature = "prob")]
        SenkoValue::TopK(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "topk",
        }),
        #[cfg(feature = "prob")]
        SenkoValue::TDigest(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "TDIS-TYPE",
        }),
        #[cfg(feature = "json")]
        SenkoValue::Json(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "json",
        }),
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "vectorset",
        }),
    }
}

fn frame_type_name(frame: &Frame<'_>) -> &'static str {
    match frame {
        Frame::SimpleString(_) => "simple-string",
        Frame::SimpleError(_) => "simple-error",
        Frame::Integer(_) => "integer",
        Frame::BulkString(_) => "bulk-string",
        Frame::Array(_) => "array",
        Frame::Null => "null",
        Frame::Boolean(_) => "boolean",
        Frame::Double(_) => "double",
        Frame::BigNumber(_) => "big-number",
        Frame::BlobError(_) => "blob-error",
        Frame::VerbatimString { .. } => "verbatim-string",
        Frame::Map(_) => "map",
        Frame::Set(_) => "set",
        Frame::Push(_) => "push",
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use compact_str::CompactString;
    use senko_core::SenkoValue;
    use senko_proto::Frame;

    use crate::commands::{Response, multi};
    use crate::store::{SetExpiry, SetOptions, Store};

    fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(input)
    }

    #[test]
    fn mget_existing_missing_expired() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("a"),
            SenkoValue::from(1_i64),
            SetOptions::default(),
        );
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis() as u64);
        let _ = store.set(
            CompactString::from("b"),
            SenkoValue::from(2_i64),
            SetOptions {
                condition: crate::store::SetCondition::Always,
                expiry: SetExpiry::PxAt(now),
                get_old: false,
            },
        );

        let response = multi::mget(&mut store, &[bs(b"a"), bs(b"missing"), bs(b"b")]).unwrap();
        assert_eq!(
            response,
            Response::Array(Box::new(smallvec::smallvec![
                Response::Value(Some(SenkoValue::Int(1))),
                Response::Value(None),
                Response::Value(None),
            ]))
        );
    }

    #[test]
    fn msetnx_fails_if_any_exists() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("k1"),
            SenkoValue::from(1_i64),
            SetOptions::default(),
        );

        let resp = multi::msetnx(&mut store, &[bs(b"k1"), bs(b"x"), bs(b"k2"), bs(b"y")]).unwrap();
        assert_eq!(resp, Response::Integer(0));
        assert_eq!(store.get(b"k2"), None);
    }

    #[test]
    fn msetex_nx_all_clear_vs_present() {
        let mut store = Store::default();
        let ok = multi::msetex(
            &mut store,
            &[
                bs(b"2"),
                bs(b"a"),
                bs(b"1"),
                bs(b"b"),
                bs(b"2"),
                bs(b"NX"),
                bs(b"PX"),
                bs(b"5000"),
            ],
        )
        .unwrap();
        assert_eq!(ok, Response::Integer(2));

        let blocked = multi::msetex(
            &mut store,
            &[
                bs(b"2"),
                bs(b"a"),
                bs(b"3"),
                bs(b"c"),
                bs(b"4"),
                bs(b"NX"),
                bs(b"PX"),
                bs(b"5000"),
            ],
        )
        .unwrap();
        assert_eq!(blocked, Response::Integer(0));
        assert_eq!(store.get(b"c"), None);
    }

    #[test]
    fn msetex_expiry_variants() {
        let mut store = Store::default();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis() as u64);
        let exat = (now / 1000 + 60).to_string().into_bytes();
        let pxat = (now + 60_000).to_string().into_bytes();

        assert_eq!(
            multi::msetex(
                &mut store,
                &[bs(b"1"), bs(b"ex"), bs(b"1"), bs(b"EX"), bs(b"5")]
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            multi::msetex(
                &mut store,
                &[bs(b"1"), bs(b"px"), bs(b"1"), bs(b"PX"), bs(b"100")]
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            multi::msetex(
                &mut store,
                &[bs(b"1"), bs(b"exat"), bs(b"1"), bs(b"EXAT"), bs(&exat)]
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            multi::msetex(
                &mut store,
                &[bs(b"1"), bs(b"pxat"), bs(b"1"), bs(b"PXAT"), bs(&pxat)]
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            multi::msetex(
                &mut store,
                &[bs(b"1"), bs(b"keepttl"), bs(b"1"), bs(b"KEEPTTL")]
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn large_batch_10k_keys() {
        let mut store = Store::default();
        let mut frames = Vec::with_capacity(20_000);
        for i in 0..10_000usize {
            frames.push(Frame::BulkString(
                Box::leak(format!("k{i}").into_boxed_str()).as_bytes(),
            ));
            frames.push(Frame::BulkString(
                Box::leak(format!("{i}").into_boxed_str()).as_bytes(),
            ));
        }
        assert_eq!(
            multi::mset(&mut store, &frames).unwrap(),
            Response::Simple(b"OK")
        );

        assert!(store.get(b"k0").is_some());
        assert!(store.get(b"k9999").is_some());
    }
}
