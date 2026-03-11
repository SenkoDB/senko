use bytes::{Bytes, BytesMut};
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;

use crate::{
    commands::Response,
    store::{SetCondition, SetExpiry, SetOptions, Store},
};

const MAX_STRING_SIZE: usize = 512 * 1024 * 1024;

#[inline]
pub fn append(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'append' command",
        ));
    }

    let key_bytes = arg_bytes(&args[0])?;
    let key = parse_key(key_bytes)?;
    let append_value = arg_bytes(&args[1])?;

    let current = store.get(key_bytes).cloned();
    let mut base = materialize_value_bytes(current.as_ref())?;

    let new_len = base
        .len()
        .checked_add(append_value.len())
        .ok_or_else(size_error)?;
    if new_len > MAX_STRING_SIZE {
        return Err(size_error());
    }

    ensure_growth(&mut base, new_len);
    base.extend_from_slice(append_value);

    let final_value = if base.len() <= 20 {
        SenkoValue::encode_attempt(base.as_ref())
    } else {
        SenkoValue::Raw(base.freeze())
    };

    let _ = store.set(
        key,
        final_value,
        SetOptions {
            condition: SetCondition::Always,
            expiry: if current.is_some() {
                SetExpiry::KeepTtl
            } else {
                SetExpiry::None
            },
            get_old: false,
        },
    );

    Ok(Response::Integer(new_len as i64))
}

#[inline]
pub fn strlen(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'strlen' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let len = match store.get(key) {
        None => 0,
        Some(SenkoValue::Raw(raw)) => raw.len(),
        Some(SenkoValue::Int(value)) => int_len(*value),
        Some(SenkoValue::Float(value)) => value.to_string().len(),
        Some(SenkoValue::Hash(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "hash",
            });
        }
        Some(SenkoValue::List(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "list",
            });
        }
        Some(SenkoValue::Set(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "set",
            });
        }
        Some(SenkoValue::Stream(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "stream",
            });
        }
        Some(SenkoValue::ZSet(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "zset",
            });
        }
        #[cfg(feature = "prob")]
        Some(SenkoValue::BloomFilter(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "MBbloom--",
            });
        }
        #[cfg(feature = "prob")]
        Some(SenkoValue::CuckooFilter(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "cuckooFilter",
            });
        }
        #[cfg(feature = "prob")]
        Some(SenkoValue::CountMinSketch(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "CMSk--",
            });
        }
        #[cfg(feature = "prob")]
        Some(SenkoValue::TopK(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "topk",
            });
        }
        #[cfg(feature = "prob")]
        Some(SenkoValue::TDigest(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "TDIS-TYPE",
            });
        }
        #[cfg(feature = "json")]
        Some(SenkoValue::Json(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "json",
            });
        }
        #[cfg(feature = "vector")]
        Some(SenkoValue::VectorSet(_)) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "vectorset",
            });
        }
    };

    Ok(Response::Integer(len as i64))
}

#[inline]
pub fn getrange(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'getrange' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let start = parse_i64(arg_bytes(&args[1])?)?;
    let end = parse_i64(arg_bytes(&args[2])?)?;

    let Some(value) = store.get(key).cloned() else {
        return Ok(Response::Value(Some(SenkoValue::Raw(Bytes::new()))));
    };

    let out = match value {
        SenkoValue::Raw(raw) => {
            slice_range(raw.as_ref(), start, end).map_or_else(Bytes::new, |(s, e)| raw.slice(s..e))
        }
        SenkoValue::Int(int) => {
            let bytes = int.to_string().into_bytes();
            slice_owned(bytes, start, end)
        }
        SenkoValue::Float(float) => {
            let bytes = float.to_string().into_bytes();
            slice_owned(bytes, start, end)
        }
        SenkoValue::Hash(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "hash",
            });
        }
        SenkoValue::List(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "list",
            });
        }
        SenkoValue::Set(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "set",
            });
        }
        SenkoValue::Stream(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "stream",
            });
        }
        SenkoValue::ZSet(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "zset",
            });
        }
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "MBbloom--",
            });
        }
        #[cfg(feature = "prob")]
        SenkoValue::CuckooFilter(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "cuckooFilter",
            });
        }
        #[cfg(feature = "prob")]
        SenkoValue::CountMinSketch(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "CMSk--",
            });
        }
        #[cfg(feature = "prob")]
        SenkoValue::TopK(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "topk",
            });
        }
        #[cfg(feature = "prob")]
        SenkoValue::TDigest(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "TDIS-TYPE",
            });
        }
        #[cfg(feature = "json")]
        SenkoValue::Json(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "json",
            });
        }
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => {
            return Err(SenkoError::WrongType {
                expected: "string",
                actual: "vectorset",
            });
        }
    };

    Ok(Response::Value(Some(SenkoValue::Raw(out))))
}

#[inline]
pub fn setrange(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'setrange' command",
        ));
    }

    let key_bytes = arg_bytes(&args[0])?;
    let key = parse_key(key_bytes)?;
    let offset = parse_non_negative_usize(arg_bytes(&args[1])?)?;
    let insert = arg_bytes(&args[2])?;

    let current = store.get(key_bytes).cloned();
    if insert.is_empty() {
        let len = current.as_ref().map(value_len).unwrap_or(0);
        return Ok(Response::Integer(len as i64));
    }

    let mut base = materialize_value_bytes(current.as_ref())?;

    let needed = offset.checked_add(insert.len()).ok_or_else(size_error)?;
    if needed > MAX_STRING_SIZE {
        return Err(size_error());
    }

    if base.len() < offset {
        ensure_growth(&mut base, offset);
        base.resize(offset, 0);
    }

    ensure_growth(&mut base, needed);
    if base.len() < needed {
        base.resize(needed, 0);
    }
    base[offset..offset + insert.len()].copy_from_slice(insert);

    let new_len = base.len() as i64;
    let _ = store.set(
        key,
        SenkoValue::Raw(base.freeze()),
        SetOptions {
            condition: SetCondition::Always,
            expiry: if current.is_some() {
                SetExpiry::KeepTtl
            } else {
                SetExpiry::None
            },
            get_old: false,
        },
    );

    Ok(Response::Integer(new_len))
}

#[inline]
pub fn substr(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    getrange(store, args)
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

fn parse_i64(raw: &[u8]) -> SenkoResult<i64> {
    let text = std::str::from_utf8(raw)
        .map_err(|_| SenkoError::Protocol("value is not an integer or out of range"))?;
    text.parse::<i64>()
        .map_err(|_| SenkoError::Protocol("value is not an integer or out of range"))
}

fn parse_non_negative_usize(raw: &[u8]) -> SenkoResult<usize> {
    let value = parse_i64(raw)?;
    if value < 0 {
        return Err(SenkoError::Protocol("ERR offset is out of range"));
    }
    usize::try_from(value).map_err(|_| SenkoError::Protocol("ERR offset is out of range"))
}

fn size_error() -> SenkoError {
    SenkoError::Protocol("ERR string exceeds maximum allowed size (512MB)")
}

fn materialize_value_bytes(value: Option<&SenkoValue>) -> SenkoResult<BytesMut> {
    match value {
        None => Ok(BytesMut::new()),
        Some(SenkoValue::Raw(raw)) => Ok(match raw.clone().try_into_mut() {
            Ok(mut_ref) => mut_ref,
            Err(shared) => {
                let mut out = BytesMut::with_capacity(shared.len().max(1));
                out.extend_from_slice(shared.as_ref());
                out
            }
        }),
        Some(SenkoValue::Int(int)) => {
            let rendered = int.to_string();
            let mut out = BytesMut::with_capacity(rendered.len());
            out.extend_from_slice(rendered.as_bytes());
            Ok(out)
        }
        Some(SenkoValue::Float(float)) => {
            let rendered = float.to_string();
            let mut out = BytesMut::with_capacity(rendered.len());
            out.extend_from_slice(rendered.as_bytes());
            Ok(out)
        }
        Some(SenkoValue::Hash(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "hash",
        }),
        Some(SenkoValue::List(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "list",
        }),
        Some(SenkoValue::Set(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "set",
        }),
        Some(SenkoValue::Stream(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "stream",
        }),
        Some(SenkoValue::ZSet(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "zset",
        }),
        #[cfg(feature = "prob")]
        Some(SenkoValue::BloomFilter(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "MBbloom--",
        }),
        #[cfg(feature = "prob")]
        Some(SenkoValue::CuckooFilter(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "cuckooFilter",
        }),
        #[cfg(feature = "prob")]
        Some(SenkoValue::CountMinSketch(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "CMSk--",
        }),
        #[cfg(feature = "prob")]
        Some(SenkoValue::TopK(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "topk",
        }),
        #[cfg(feature = "prob")]
        Some(SenkoValue::TDigest(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "TDIS-TYPE",
        }),
        #[cfg(feature = "json")]
        Some(SenkoValue::Json(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "json",
        }),
        #[cfg(feature = "vector")]
        Some(SenkoValue::VectorSet(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "vectorset",
        }),
    }
}

fn ensure_growth(buf: &mut BytesMut, needed_len: usize) {
    if buf.capacity() >= needed_len {
        return;
    }
    let target = needed_len.max(buf.capacity().saturating_mul(2)).max(1);
    buf.reserve(target.saturating_sub(buf.capacity()));
}

fn int_len(value: i64) -> usize {
    if value == 0 {
        return 1;
    }
    let negative = value < 0;
    let mut n = value.unsigned_abs();
    let mut digits = 0usize;
    while n > 0 {
        n /= 10;
        digits += 1;
    }
    digits + negative as usize
}

fn value_len(value: &SenkoValue) -> usize {
    match value {
        SenkoValue::Raw(raw) => raw.len(),
        SenkoValue::Int(value) => int_len(*value),
        SenkoValue::Float(value) => value.to_string().len(),
        SenkoValue::Hash(_) => 0,
        SenkoValue::List(_) => 0,
        SenkoValue::Set(_) => 0,
        SenkoValue::Stream(_) => 0,
        SenkoValue::ZSet(_) => 0,
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_) => 0,
        #[cfg(feature = "prob")]
        SenkoValue::CuckooFilter(_) => 0,
        #[cfg(feature = "prob")]
        SenkoValue::CountMinSketch(_) => 0,
        #[cfg(feature = "prob")]
        SenkoValue::TopK(_) => 0,
        #[cfg(feature = "prob")]
        SenkoValue::TDigest(_) => 0,
        #[cfg(feature = "json")]
        SenkoValue::Json(value) => SenkoValue::Json(value.clone()).as_bytes().len(),
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => 0,
    }
}

fn slice_range(data: &[u8], start: i64, end: i64) -> Option<(usize, usize)> {
    let len = data.len() as i64;
    if len == 0 {
        return None;
    }

    if start < 0 && end < 0 && start > end {
        return None;
    }

    let mut s = if start < 0 { len + start } else { start };
    let mut e = if end < 0 { len + end } else { end };

    if s < 0 {
        s = 0;
    }
    if e < 0 {
        e = 0;
    }
    if s >= len {
        return None;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e {
        return None;
    }

    Some((s as usize, e as usize + 1))
}

fn slice_owned(mut owned: Vec<u8>, start: i64, end: i64) -> Bytes {
    if let Some((s, e)) = slice_range(&owned, start, end) {
        Bytes::copy_from_slice(&owned[s..e])
    } else {
        owned.clear();
        Bytes::new()
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

    use crate::commands::strops::MAX_STRING_SIZE;
    use crate::{
        commands::{Response, arithmetic, strops},
        store::{SetCondition, SetExpiry, SetOptions, Store},
    };

    fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(input)
    }

    #[test]
    fn append_to_int_key_after_incr() {
        let mut store = Store::default();
        let _ = arithmetic::incr(&mut store, &[bs(b"foo")]).unwrap();
        let res = strops::append(&mut store, &[bs(b"foo"), bs(b"bar")]).unwrap();
        assert_eq!(res, Response::Integer(4));
        assert_eq!(
            store.get(b"foo"),
            Some(&SenkoValue::Raw(Bytes::from_static(b"1bar")))
        );
    }

    #[test]
    fn strlen_integer_without_materialization_behavior() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("num"),
            SenkoValue::Int(1234567890),
            SetOptions::default(),
        );
        let res = strops::strlen(&mut store, &[bs(b"num")]).unwrap();
        assert_eq!(res, Response::Integer(10));
    }

    #[test]
    fn getrange_negative_out_of_range_and_start_gt_end() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("k"),
            SenkoValue::Raw(Bytes::from_static(b"abcdef")),
            SetOptions::default(),
        );

        assert_eq!(
            strops::getrange(&mut store, &[bs(b"k"), bs(b"-2"), bs(b"-1")]).unwrap(),
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"ef"))))
        );
        assert_eq!(
            strops::getrange(&mut store, &[bs(b"k"), bs(b"100"), bs(b"200")]).unwrap(),
            Response::Value(Some(SenkoValue::Raw(Bytes::new())))
        );
        assert_eq!(
            strops::getrange(&mut store, &[bs(b"k"), bs(b"3"), bs(b"1")]).unwrap(),
            Response::Value(Some(SenkoValue::Raw(Bytes::new())))
        );
    }

    #[test]
    fn setrange_with_gap_zero_padding() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("gap"),
            SenkoValue::Raw(Bytes::from_static(b"abc")),
            SetOptions::default(),
        );

        let res = strops::setrange(&mut store, &[bs(b"gap"), bs(b"5"), bs(b"Z")]).unwrap();
        assert_eq!(res, Response::Integer(6));
        assert_eq!(
            store.get(b"gap"),
            Some(&SenkoValue::Raw(Bytes::from_static(b"abc\0\0Z")))
        );
    }

    #[test]
    fn setrange_on_missing_key() {
        let mut store = Store::default();
        let res = strops::setrange(&mut store, &[bs(b"new"), bs(b"2"), bs(b"xy")]).unwrap();
        assert_eq!(res, Response::Integer(4));
        assert_eq!(
            store.get(b"new"),
            Some(&SenkoValue::Raw(Bytes::from_static(b"\0\0xy")))
        );
    }

    #[test]
    fn setrange_exceeding_512mb() {
        let mut store = Store::default();
        let huge = (MAX_STRING_SIZE as i64).to_string();
        let err = strops::setrange(&mut store, &[bs(b"huge"), bs(huge.as_bytes()), bs(b"x")]);
        assert!(err.is_err());
    }

    #[test]
    fn substr_aliases_getrange() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("s"),
            SenkoValue::Raw(Bytes::from_static(b"hello")),
            SetOptions::default(),
        );
        assert_eq!(
            strops::substr(&mut store, &[bs(b"s"), bs(b"1"), bs(b"3")]).unwrap(),
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"ell"))))
        );
    }

    #[test]
    fn ttl_preserved_existing_key() {
        let mut store = Store::default();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);

        let _ = store.set(
            CompactString::from("ttl"),
            SenkoValue::Raw(Bytes::from_static(b"a")),
            SetOptions {
                condition: SetCondition::Always,
                expiry: SetExpiry::PxAt(now + 10_000),
                get_old: false,
            },
        );
        let before = store.ttl_ms(b"ttl").unwrap();
        let _ = strops::append(&mut store, &[bs(b"ttl"), bs(b"b")]).unwrap();
        let after = store.ttl_ms(b"ttl").unwrap();
        assert!(before >= 0 && after >= 0);
    }
}
