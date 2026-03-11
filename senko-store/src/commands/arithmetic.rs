use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;

use crate::{
    arithmetic::{
        checked_add_i64, checked_sub_i64, float_nan_inf_error, float_value_error,
        format_f64_no_scientific, integer_range_error, parse_f64, parse_i64_fast, value_as_f64,
        value_as_i64,
    },
    commands::Response,
    store::{SetExpiry, SetOptions, Store},
};

#[inline]
pub fn incr(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'incr' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    increment_i64(store, key, 1, true)
}

#[inline]
pub fn incrby(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'incrby' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let increment = parse_i64_fast(arg_bytes(&args[1])?).ok_or_else(integer_range_error)?;
    increment_i64(store, key, increment, true)
}

#[inline]
pub fn decr(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'decr' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    increment_i64(store, key, 1, false)
}

#[inline]
pub fn decrby(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'decrby' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let decrement = parse_i64_fast(arg_bytes(&args[1])?).ok_or_else(integer_range_error)?;
    increment_i64(store, key, decrement, false)
}

#[inline]
pub fn incrbyfloat(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'incrbyfloat' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let increment = parse_f64(arg_bytes(&args[1])?).ok_or_else(float_value_error)?;
    if !increment.is_finite() {
        return Err(float_nan_inf_error());
    }

    let current = store.get(key).cloned();
    let base = match current.as_ref() {
        None => 0.0,
        Some(value) => value_as_f64(value)?,
    };

    let result = base + increment;
    if !result.is_finite() {
        return Err(float_nan_inf_error());
    }

    let key_owned = parse_key(key)?;
    let _ = store.set(
        key_owned,
        SenkoValue::Float(result),
        SetOptions {
            condition: crate::store::SetCondition::Always,
            expiry: if current.is_some() {
                SetExpiry::KeepTtl
            } else {
                SetExpiry::None
            },
            get_old: false,
        },
    );

    let formatted = format_f64_no_scientific(result);
    Ok(Response::Value(Some(SenkoValue::Raw(Bytes::from(
        formatted,
    )))))
}

fn increment_i64(store: &mut Store, key: &[u8], delta: i64, add: bool) -> SenkoResult<Response> {
    let current = store.get(key).cloned();
    let base = match current.as_ref() {
        None => 0,
        Some(value) => value_as_i64(value)?,
    };

    let result = if add {
        checked_add_i64(base, delta)?
    } else {
        checked_sub_i64(base, delta)?
    };

    let key_owned = parse_key(key)?;
    let _ = store.set(
        key_owned,
        SenkoValue::Int(result),
        SetOptions {
            condition: crate::store::SetCondition::Always,
            expiry: if current.is_some() {
                SetExpiry::KeepTtl
            } else {
                SetExpiry::None
            },
            get_old: false,
        },
    );

    Ok(Response::Integer(result))
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
    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_core::SenkoValue;
    use senko_proto::Frame;

    use crate::{
        commands::{Response, arithmetic},
        store::{SetExpiry, SetOptions, Store},
    };

    fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(input)
    }

    #[test]
    fn incr_missing_key_is_one() {
        let mut store = Store::default();
        let res = arithmetic::incr(&mut store, &[bs(b"k")]).unwrap();
        assert_eq!(res, Response::Integer(1));
        assert_eq!(store.get(b"k"), Some(&SenkoValue::Int(1)));
    }

    #[test]
    fn incr_decr_overflow_boundaries() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("max"),
            SenkoValue::Int(i64::MAX),
            SetOptions::default(),
        );
        assert!(arithmetic::incr(&mut store, &[bs(b"max")]).is_err());

        let _ = store.set(
            CompactString::from("min"),
            SenkoValue::Int(i64::MIN),
            SetOptions::default(),
        );
        assert!(arithmetic::decr(&mut store, &[bs(b"min")]).is_err());
    }

    #[test]
    fn incrby_on_raw_42() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("n"),
            SenkoValue::Raw(Bytes::from_static(b"42")),
            SetOptions::default(),
        );
        let res = arithmetic::incrby(&mut store, &[bs(b"n"), bs(b"8")]).unwrap();
        assert_eq!(res, Response::Integer(50));
    }

    #[test]
    fn incrbyfloat_precision() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("f"),
            SenkoValue::Raw(Bytes::from_static(b"10.50")),
            SetOptions::default(),
        );
        let res = arithmetic::incrbyfloat(&mut store, &[bs(b"f"), bs(b"0.1")]).unwrap();
        assert_eq!(
            res,
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"10.6"))))
        );
    }

    #[test]
    fn incrbyfloat_nan_inf_rejection() {
        let mut store = Store::default();
        assert!(arithmetic::incrbyfloat(&mut store, &[bs(b"f"), bs(b"nan")]).is_err());
        assert!(arithmetic::incrbyfloat(&mut store, &[bs(b"f"), bs(b"inf")]).is_err());
    }

    #[test]
    fn type_error_when_key_arg_non_string() {
        let mut store = Store::default();
        let args = [Frame::Integer(1), bs(b"1")];
        assert!(matches!(
            arithmetic::incrby(&mut store, &args),
            Err(senko_core::SenkoError::WrongType { .. })
        ));
    }

    #[test]
    fn ttl_preserved_for_existing_key() {
        let mut store = Store::default();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);

        let _ = store.set(
            CompactString::from("ttl"),
            SenkoValue::Int(5),
            SetOptions {
                condition: crate::store::SetCondition::Always,
                expiry: SetExpiry::PxAt(now + 5_000),
                get_old: false,
            },
        );
        let before = store.ttl_ms(b"ttl").unwrap();
        let _ = arithmetic::incr(&mut store, &[bs(b"ttl")]).unwrap();
        let after = store.ttl_ms(b"ttl").unwrap();
        assert!(before >= 0 && after >= 0);
    }
}
