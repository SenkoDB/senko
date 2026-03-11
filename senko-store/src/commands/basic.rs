use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    commands::Response,
    store::{SetCondition, SetExpiry, SetOptions, Store},
};

const OK: &[u8] = b"OK";

enum ParsedCondition {
    Always,
    Nx,
    Xx,
    IfEq(Bytes),
    IfNe(Bytes),
    IfDeq([u8; 16]),
    IfDne([u8; 16]),
}

enum ParsedExpiry {
    None,
    Ex(u64),
    Px(u64),
    ExAt(u64),
    PxAt(u64),
    KeepTtl,
    Persist,
}

#[inline]
pub fn get(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'get' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let value = match store.get_cloned(key) {
        None => None,
        Some(value @ (SenkoValue::Raw(_) | SenkoValue::Int(_) | SenkoValue::Float(_))) => {
            Some(value)
        }
        Some(other) => {
            ensure_string_value(&other)?;
            unreachable!("ensure_string_value returned Ok for non-string value");
        }
    };
    Ok(Response::Value(value))
}

#[inline]
pub fn info(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if !args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'info' command",
        ));
    }
    Ok(Response::Value(Some(SenkoValue::from(Bytes::from(
        store.info().into_bytes(),
    )))))
}

#[inline]
pub fn set(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'set' command",
        ));
    }

    let key_bytes = arg_bytes(&args[0])?;
    let key = parse_key(key_bytes)?;
    let value_bytes = arg_bytes(&args[1])?;

    let mut condition = ParsedCondition::Always;
    let mut condition_set = false;
    let mut expiry = ParsedExpiry::None;
    let mut expiry_set = false;
    let mut get_old = false;

    let mut index = 2usize;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if is_opt(token, b"GET") {
            if get_old {
                return Err(syntax_error());
            }
            get_old = true;
            index += 1;
            continue;
        }

        if is_opt(token, b"NX")
            || is_opt(token, b"XX")
            || is_opt(token, b"IFEQ")
            || is_opt(token, b"IFNE")
            || is_opt(token, b"IFDEQ")
            || is_opt(token, b"IFDNE")
        {
            if condition_set {
                return Err(syntax_error());
            }
            condition_set = true;
            if is_opt(token, b"NX") {
                condition = ParsedCondition::Nx;
                index += 1;
                continue;
            }
            if is_opt(token, b"XX") {
                condition = ParsedCondition::Xx;
                index += 1;
                continue;
            }

            index += 1;
            if index >= args.len() {
                return Err(syntax_error());
            }
            let value = arg_bytes(&args[index])?;
            if is_opt(token, b"IFEQ") {
                condition = ParsedCondition::IfEq(Bytes::copy_from_slice(value));
            } else if is_opt(token, b"IFNE") {
                condition = ParsedCondition::IfNe(Bytes::copy_from_slice(value));
            } else if is_opt(token, b"IFDEQ") {
                condition = ParsedCondition::IfDeq(parse_digest(value)?);
            } else {
                condition = ParsedCondition::IfDne(parse_digest(value)?);
            }
            index += 1;
            continue;
        }

        if is_opt(token, b"EX")
            || is_opt(token, b"PX")
            || is_opt(token, b"EXAT")
            || is_opt(token, b"PXAT")
            || is_opt(token, b"KEEPTTL")
        {
            if expiry_set {
                return Err(syntax_error());
            }
            expiry_set = true;

            if is_opt(token, b"KEEPTTL") {
                expiry = ParsedExpiry::KeepTtl;
                index += 1;
                continue;
            }

            index += 1;
            if index >= args.len() {
                return Err(syntax_error());
            }
            let raw = arg_bytes(&args[index])?;
            let parsed = parse_positive_u64(raw, "set")?;
            expiry = if is_opt(token, b"EX") {
                ParsedExpiry::Ex(parsed)
            } else if is_opt(token, b"PX") {
                ParsedExpiry::Px(parsed)
            } else if is_opt(token, b"EXAT") {
                ParsedExpiry::ExAt(parsed)
            } else {
                ParsedExpiry::PxAt(parsed)
            };
            index += 1;
            continue;
        }

        return Err(syntax_error());
    }

    if !digest_condition_matches(store, key_bytes, &condition)? {
        let old_value = if get_old {
            store.get(key_bytes).cloned()
        } else {
            None
        };
        return Ok(Response::Value(old_value));
    }

    let store_condition = match condition {
        ParsedCondition::Always | ParsedCondition::IfDeq(_) | ParsedCondition::IfDne(_) => {
            SetCondition::Always
        }
        ParsedCondition::Nx => SetCondition::NX,
        ParsedCondition::Xx => SetCondition::XX,
        ParsedCondition::IfEq(value) => SetCondition::IfEq(value),
        ParsedCondition::IfNe(value) => SetCondition::IfNe(value),
    };

    let store_expiry = match expiry {
        ParsedExpiry::None | ParsedExpiry::Persist => SetExpiry::None,
        ParsedExpiry::Ex(value) => SetExpiry::Ex(value),
        ParsedExpiry::Px(value) => SetExpiry::Px(value),
        ParsedExpiry::ExAt(value) => SetExpiry::ExAt(value),
        ParsedExpiry::PxAt(value) => SetExpiry::PxAt(value),
        ParsedExpiry::KeepTtl => SetExpiry::KeepTtl,
    };

    let set_result = store.set(
        key,
        SenkoValue::encode_attempt(value_bytes),
        SetOptions {
            condition: store_condition,
            expiry: store_expiry,
            get_old,
        },
    );

    if !set_result.applied {
        return Ok(Response::Value(if get_old {
            set_result.old_value
        } else {
            None
        }));
    }
    if get_old {
        return Ok(Response::Value(set_result.old_value));
    }
    Ok(Response::Simple(OK))
}

#[inline]
pub fn setnx(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'setnx' command",
        ));
    }
    let key = parse_key(arg_bytes(&args[0])?)?;
    let value = SenkoValue::encode_attempt(arg_bytes(&args[1])?);
    let result = store.set(
        key,
        value,
        SetOptions {
            condition: SetCondition::NX,
            expiry: SetExpiry::None,
            get_old: false,
        },
    );
    Ok(Response::Integer(result.applied as i64))
}

#[inline]
pub fn setex(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'setex' command",
        ));
    }
    let key = parse_key(arg_bytes(&args[0])?)?;
    let seconds = parse_positive_u64(arg_bytes(&args[1])?, "setex")?;
    let value = SenkoValue::encode_attempt(arg_bytes(&args[2])?);
    let _ = store.set(
        key,
        value,
        SetOptions {
            condition: SetCondition::Always,
            expiry: SetExpiry::Ex(seconds),
            get_old: false,
        },
    );
    Ok(Response::Simple(OK))
}

#[inline]
pub fn psetex(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'psetex' command",
        ));
    }
    let key = parse_key(arg_bytes(&args[0])?)?;
    let milliseconds = parse_positive_u64(arg_bytes(&args[1])?, "psetex")?;
    let value = SenkoValue::encode_attempt(arg_bytes(&args[2])?);
    let _ = store.set(
        key,
        value,
        SetOptions {
            condition: SetCondition::Always,
            expiry: SetExpiry::Px(milliseconds),
            get_old: false,
        },
    );
    Ok(Response::Simple(OK))
}

#[inline]
pub fn getset(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'getset' command",
        ));
    }
    let key_bytes = arg_bytes(&args[0])?;
    let old = store.get(key_bytes).cloned();
    if let Some(ref value) = old {
        ensure_string_value(value)?;
    }
    let key = parse_key(key_bytes)?;
    let value = SenkoValue::encode_attempt(arg_bytes(&args[1])?);
    let _ = store.set(key, value, SetOptions::default());
    Ok(Response::Value(old))
}

#[inline]
pub fn getdel(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'getdel' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let old = store.get(key).cloned();
    if let Some(ref value) = old {
        ensure_string_value(value)?;
        let _ = store.delete(key);
    }
    Ok(Response::Value(old))
}

#[inline]
pub fn getex(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'getex' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let mut expiry = ParsedExpiry::None;
    let mut expiry_set = false;

    let mut index = 1usize;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if expiry_set {
            return Err(syntax_error());
        }
        expiry_set = true;

        if is_opt(token, b"PERSIST") {
            expiry = ParsedExpiry::Persist;
            index += 1;
            continue;
        }

        if !(is_opt(token, b"EX")
            || is_opt(token, b"PX")
            || is_opt(token, b"EXAT")
            || is_opt(token, b"PXAT"))
        {
            return Err(syntax_error());
        }

        index += 1;
        if index >= args.len() {
            return Err(syntax_error());
        }
        let value = parse_positive_u64(arg_bytes(&args[index])?, "getex")?;
        expiry = if is_opt(token, b"EX") {
            ParsedExpiry::Ex(value)
        } else if is_opt(token, b"PX") {
            ParsedExpiry::Px(value)
        } else if is_opt(token, b"EXAT") {
            ParsedExpiry::ExAt(value)
        } else {
            ParsedExpiry::PxAt(value)
        };
        index += 1;
    }

    let value = store.get(key).cloned();
    let Some(value) = value else {
        return Ok(Response::Value(None));
    };
    ensure_string_value(&value)?;

    match expiry {
        ParsedExpiry::None => {}
        ParsedExpiry::Persist => store.remove_expiry(key),
        ParsedExpiry::Ex(seconds) => {
            store.set_expiry(key, now_ms().saturating_add(seconds.saturating_mul(1_000)));
        }
        ParsedExpiry::Px(milliseconds) => {
            store.set_expiry(key, now_ms().saturating_add(milliseconds));
        }
        ParsedExpiry::ExAt(seconds) => {
            store.set_expiry(key, seconds.saturating_mul(1_000));
        }
        ParsedExpiry::PxAt(milliseconds) => {
            store.set_expiry(key, milliseconds);
        }
        ParsedExpiry::KeepTtl => return Err(syntax_error()),
    }

    Ok(Response::Value(Some(value)))
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

fn is_opt(input: &[u8], expected_upper: &[u8]) -> bool {
    input.eq_ignore_ascii_case(expected_upper)
}

fn syntax_error() -> SenkoError {
    SenkoError::Protocol("syntax error")
}

fn digest_condition_matches(
    store: &mut Store,
    key: &[u8],
    condition: &ParsedCondition,
) -> SenkoResult<bool> {
    match condition {
        ParsedCondition::IfDeq(expected) => {
            let Some(value) = store.get(key) else {
                return Ok(false);
            };
            ensure_string_value(value)?;
            Ok(digest_16_hex(value.as_bytes().as_ref()) == *expected)
        }
        ParsedCondition::IfDne(expected) => {
            let Some(value) = store.get(key) else {
                return Ok(true);
            };
            ensure_string_value(value)?;
            Ok(digest_16_hex(value.as_bytes().as_ref()) != *expected)
        }
        _ => Ok(true),
    }
}

fn parse_digest(raw: &[u8]) -> SenkoResult<[u8; 16]> {
    if raw.len() != 16 {
        return Err(SenkoError::Protocol(
            "must be exactly 16 hexadecimal characters",
        ));
    }
    let mut out = [0u8; 16];
    for (index, byte) in raw.iter().enumerate() {
        out[index] = byte.to_ascii_lowercase();
    }
    Ok(out)
}

fn digest_16_hex(data: &[u8]) -> [u8; 16] {
    let mut out = [0u8; 16];
    let rendered = format!("{:016x}", xxh3_64(data));
    out.copy_from_slice(rendered.as_bytes());
    out
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use senko_core::SenkoValue;
    use senko_proto::Frame;

    use crate::{
        commands::{Response, basic},
        store::Store,
    };

    fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(input)
    }

    #[test]
    fn get_happy_path_and_null() {
        let mut store = Store::default();
        let set_args = [bs(b"key"), bs(b"value")];
        assert_eq!(
            basic::set(&mut store, &set_args).unwrap(),
            Response::Simple(b"OK")
        );

        let get_args = [bs(b"key")];
        assert_eq!(
            basic::get(&mut store, &get_args).unwrap(),
            Response::Value(Some(SenkoValue::from(Bytes::from_static(b"value"))))
        );

        let missing = [bs(b"missing")];
        assert_eq!(
            basic::get(&mut store, &missing).unwrap(),
            Response::Value(None)
        );
    }

    #[test]
    fn set_nx_xx_and_get_flag() {
        let mut store = Store::default();
        let args = [bs(b"key"), bs(b"1"), bs(b"NX")];
        assert_eq!(
            basic::set(&mut store, &args).unwrap(),
            Response::Simple(b"OK")
        );

        let blocked = [bs(b"key"), bs(b"2"), bs(b"NX")];
        assert_eq!(
            basic::set(&mut store, &blocked).unwrap(),
            Response::Value(None)
        );

        let get_old = [bs(b"key"), bs(b"3"), bs(b"XX"), bs(b"GET")];
        assert_eq!(
            basic::set(&mut store, &get_old).unwrap(),
            Response::Value(Some(SenkoValue::Int(1)))
        );
    }

    #[test]
    fn set_option_conflicts_are_errors() {
        let mut store = Store::default();
        let conflict = [bs(b"k"), bs(b"v"), bs(b"NX"), bs(b"XX")];
        assert!(basic::set(&mut store, &conflict).is_err());

        let conflict_ttl = [bs(b"k"), bs(b"v"), bs(b"EX"), bs(b"1"), bs(b"PX"), bs(b"1")];
        assert!(basic::set(&mut store, &conflict_ttl).is_err());
    }

    #[test]
    fn set_digest_conditions_work() {
        let mut store = Store::default();
        basic::set(&mut store, &[bs(b"key"), bs(b"abc")]).unwrap();

        let digest = super::digest_16_hex(b"abc");
        let success = [bs(b"key"), bs(b"def"), bs(b"IFDEQ"), bs(&digest)];
        assert_eq!(
            basic::set(&mut store, &success).unwrap(),
            Response::Simple(b"OK")
        );

        let fail = [bs(b"key"), bs(b"ghi"), bs(b"IFDEQ"), bs(&digest)];
        assert_eq!(
            basic::set(&mut store, &fail).unwrap(),
            Response::Value(None)
        );

        let dne = [bs(b"key"), bs(b"jkl"), bs(b"IFDNE"), bs(&digest)];
        assert_eq!(
            basic::set(&mut store, &dne).unwrap(),
            Response::Simple(b"OK")
        );
    }

    #[test]
    fn setnx_setex_psetex_and_getex_paths() {
        let mut store = Store::default();
        assert_eq!(
            basic::setnx(&mut store, &[bs(b"a"), bs(b"1")]).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            basic::setnx(&mut store, &[bs(b"a"), bs(b"2")]).unwrap(),
            Response::Integer(0)
        );

        assert_eq!(
            basic::setex(&mut store, &[bs(b"b"), bs(b"5"), bs(b"x")]).unwrap(),
            Response::Simple(b"OK")
        );
        assert_eq!(
            basic::psetex(&mut store, &[bs(b"c"), bs(b"5000"), bs(b"y")]).unwrap(),
            Response::Simple(b"OK")
        );

        assert!(matches!(
            basic::getex(&mut store, &[bs(b"b"), bs(b"PX"), bs(b"250")]).unwrap(),
            Response::Value(Some(_))
        ));
        assert!(matches!(
            basic::getex(&mut store, &[bs(b"b"), bs(b"PERSIST")]).unwrap(),
            Response::Value(Some(_))
        ));
    }

    #[test]
    fn getset_and_getdel_are_atomic_style() {
        let mut store = Store::default();
        basic::set(&mut store, &[bs(b"k"), bs(b"1")]).unwrap();

        assert_eq!(
            basic::getset(&mut store, &[bs(b"k"), bs(b"2")]).unwrap(),
            Response::Value(Some(SenkoValue::Int(1)))
        );
        assert_eq!(
            basic::getdel(&mut store, &[bs(b"k")]).unwrap(),
            Response::Value(Some(SenkoValue::Int(2)))
        );
        assert_eq!(
            basic::get(&mut store, &[bs(b"k")]).unwrap(),
            Response::Value(None)
        );
    }

    #[test]
    fn wrong_arity_and_type_errors() {
        let mut store = Store::default();
        assert!(basic::get(&mut store, &[]).is_err());
        assert!(basic::setnx(&mut store, &[bs(b"a")]).is_err());
        assert!(basic::setex(&mut store, &[bs(b"a"), bs(b"1")]).is_err());

        let wrong_type = [Frame::Integer(1), bs(b"value")];
        assert!(matches!(
            basic::setnx(&mut store, &wrong_type),
            Err(senko_core::SenkoError::WrongType { .. })
        ));
    }
}
