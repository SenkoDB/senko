use std::borrow::Cow;

use bytes::Bytes;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use subtle::ConstantTimeEq;
use xxhash_rust::xxh3::xxh3_64;

use crate::commands::Response;
use crate::store::Store;

const DIGEST_HEX_LENGTH: usize = 16;

enum DelexCondition {
    Always,
    IfEq(Bytes),
    IfNe(Bytes),
    IfDeq([u8; DIGEST_HEX_LENGTH]),
    IfDne([u8; DIGEST_HEX_LENGTH]),
}

#[inline]
pub fn digest(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'digest' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let Some(value) = store.get(key).cloned() else {
        return Ok(Response::Value(None));
    };

    let data = value_wire_bytes(&value);
    let hex = digest_hex_16(data.as_ref());
    Ok(Response::Value(Some(SenkoValue::Raw(
        Bytes::copy_from_slice(&hex),
    ))))
}

#[inline]
pub fn delex(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 && args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'delex' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let condition = if args.len() == 1 {
        DelexCondition::Always
    } else {
        let flag = arg_bytes(&args[1])?;
        let value = arg_bytes(&args[2])?;
        if is_opt(flag, b"IFEQ") {
            DelexCondition::IfEq(Bytes::copy_from_slice(value))
        } else if is_opt(flag, b"IFNE") {
            DelexCondition::IfNe(Bytes::copy_from_slice(value))
        } else if is_opt(flag, b"IFDEQ") {
            DelexCondition::IfDeq(normalize_hex(value)?)
        } else if is_opt(flag, b"IFDNE") {
            DelexCondition::IfDne(normalize_hex(value)?)
        } else {
            return Err(SenkoError::Protocol("Invalid condition"));
        }
    };

    let Some(value) = store.get(key).cloned() else {
        return Ok(Response::Integer(0));
    };

    let should_delete = match condition {
        DelexCondition::Always => true,
        DelexCondition::IfEq(expected) => value_wire_bytes(&value).as_ref() == expected.as_ref(),
        DelexCondition::IfNe(expected) => value_wire_bytes(&value).as_ref() != expected.as_ref(),
        DelexCondition::IfDeq(expected) => {
            let data = value_wire_bytes(&value);
            let actual = digest_hex_16(data.as_ref());
            ct_eq_hex(&actual, &expected)
        }
        DelexCondition::IfDne(expected) => {
            let data = value_wire_bytes(&value);
            let actual = digest_hex_16(data.as_ref());
            !ct_eq_hex(&actual, &expected)
        }
    };

    if !should_delete {
        return Ok(Response::Integer(0));
    }

    Ok(Response::Integer(store.delete(key) as i64))
}

fn value_wire_bytes(value: &SenkoValue) -> Cow<'_, [u8]> {
    match value {
        SenkoValue::Raw(bytes) => Cow::Borrowed(bytes.as_ref()),
        SenkoValue::Int(v) => Cow::Owned(v.to_string().into_bytes()),
        SenkoValue::Float(v) => Cow::Owned(v.to_string().into_bytes()),
        SenkoValue::Hash(_) => Cow::Borrowed(b"[hash]"),
        SenkoValue::List(_) => Cow::Borrowed(b"[list]"),
        SenkoValue::Set(_) => Cow::Borrowed(b"[set]"),
        SenkoValue::Stream(_) => Cow::Borrowed(b"[stream]"),
        SenkoValue::ZSet(_) => Cow::Borrowed(b"[zset]"),
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_) => Cow::Borrowed(b"[bloom]"),
        #[cfg(feature = "prob")]
        SenkoValue::CuckooFilter(_) => Cow::Borrowed(b"[cuckoo]"),
        #[cfg(feature = "prob")]
        SenkoValue::CountMinSketch(_) => Cow::Borrowed(b"[cms]"),
        #[cfg(feature = "prob")]
        SenkoValue::TopK(_) => Cow::Borrowed(b"[topk]"),
        #[cfg(feature = "prob")]
        SenkoValue::TDigest(_) => Cow::Borrowed(b"[tdigest]"),
        #[cfg(feature = "json")]
        SenkoValue::Json(_) => Cow::Borrowed(b"[json]"),
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => Cow::Borrowed(b"[vectorset]"),
    }
}

fn digest_hex_16(data: &[u8]) -> [u8; DIGEST_HEX_LENGTH] {
    let mut out = [0u8; DIGEST_HEX_LENGTH];
    let rendered = format!("{:016x}", xxh3_64(data));
    out.copy_from_slice(rendered.as_bytes());
    out
}

fn normalize_hex(input: &[u8]) -> SenkoResult<[u8; DIGEST_HEX_LENGTH]> {
    if input.len() != DIGEST_HEX_LENGTH {
        return Err(SenkoError::Protocol(
            "must be exactly 16 hexadecimal characters",
        ));
    }
    let mut out = [0u8; DIGEST_HEX_LENGTH];
    for (index, byte) in input.iter().enumerate() {
        out[index] = byte.to_ascii_lowercase();
    }
    Ok(out)
}

fn ct_eq_hex(left: &[u8; DIGEST_HEX_LENGTH], right: &[u8; DIGEST_HEX_LENGTH]) -> bool {
    left.ct_eq(right).into()
}

fn is_opt(input: &[u8], expected_upper: &[u8]) -> bool {
    input.eq_ignore_ascii_case(expected_upper)
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

    use crate::commands::{Response, conditional};
    use crate::store::{SetOptions, Store};

    fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(input)
    }

    #[test]
    fn digest_known_value_matches_xxh3_hex() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("k"),
            SenkoValue::Raw(Bytes::from_static(b"hello")),
            SetOptions::default(),
        );

        let res = conditional::digest(&mut store, &[bs(b"k")]).unwrap();
        let expected_hex = super::digest_hex_16(b"hello");
        assert_eq!(
            res,
            Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(&expected_hex))))
        );
    }

    #[test]
    fn digest_integer_encoded_key() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("n"),
            SenkoValue::Int(42),
            SetOptions::default(),
        );

        let res = conditional::digest(&mut store, &[bs(b"n")]).unwrap();
        let expected_hex = super::digest_hex_16(b"42");
        assert_eq!(
            res,
            Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(&expected_hex))))
        );
    }

    #[test]
    fn delex_ifeq_behaves() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("k"),
            SenkoValue::Raw(Bytes::from_static(b"v")),
            SetOptions::default(),
        );
        assert_eq!(
            conditional::delex(&mut store, &[bs(b"k"), bs(b"IFEQ"), bs(b"v")]).unwrap(),
            Response::Integer(1)
        );

        let _ = store.set(
            CompactString::from("k"),
            SenkoValue::Raw(Bytes::from_static(b"v")),
            SetOptions::default(),
        );
        assert_eq!(
            conditional::delex(&mut store, &[bs(b"k"), bs(b"IFEQ"), bs(b"x")]).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn delex_ifdeq_matches_computed_digest() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("k"),
            SenkoValue::Raw(Bytes::from_static(b"value")),
            SetOptions::default(),
        );
        let digest = super::digest_hex_16(b"value");
        assert_eq!(
            conditional::delex(&mut store, &[bs(b"k"), bs(b"IFDEQ"), bs(&digest)]).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn delex_ifdne_mismatch_deletes() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("k"),
            SenkoValue::Raw(Bytes::from_static(b"value")),
            SetOptions::default(),
        );
        let wrong = [b'0'; super::DIGEST_HEX_LENGTH];
        assert_eq!(
            conditional::delex(&mut store, &[bs(b"k"), bs(b"IFDNE"), bs(&wrong)]).unwrap(),
            Response::Integer(1)
        );
    }
}
