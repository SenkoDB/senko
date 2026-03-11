use bytes::Bytes;
use senko_core::{SenkoError, SenkoResult, SenkoValue, SetEncoding, SetObject};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{commands::Response, pattern::glob_match, store::Store};

#[inline]
pub fn sscan(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'sscan' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    ensure_set_type_or_missing(store, key)?;
    let cursor = parse_u64(arg_bytes(&args[1])?)?;
    let mut idx = 2usize;
    let mut pattern: Option<&[u8]> = None;
    let mut count: usize = 10;

    while idx < args.len() {
        let token = arg_bytes(&args[idx])?;
        if token.eq_ignore_ascii_case(b"MATCH") {
            idx += 1;
            if idx >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            pattern = Some(arg_bytes(&args[idx])?);
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"COUNT") {
            idx += 1;
            if idx >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            count = parse_usize(arg_bytes(&args[idx])?)?.max(1);
            idx += 1;
            continue;
        }
        return Err(SenkoError::Protocol("syntax error"));
    }

    let (next, members) = if let Some(set) = store.get_set(key) {
        sscan_step(set, cursor, count, pattern)
    } else {
        (0, Vec::new())
    };

    let mut top = SmallVec::<[Response; 16]>::new();
    top.push(Response::Value(Some(SenkoValue::Raw(Bytes::from(
        next.to_string().into_bytes(),
    )))));
    let values = members
        .into_iter()
        .map(|member| Response::Value(Some(SenkoValue::Raw(Bytes::from(member)))))
        .collect();
    top.push(Response::Array(Box::new(values)));
    Ok(Response::Array(Box::new(top)))
}

pub fn sscan_step(
    set: &SetObject,
    cursor: u64,
    count: usize,
    pattern: Option<&[u8]>,
) -> (u64, Vec<Vec<u8>>) {
    if set.is_empty() {
        return (0, Vec::new());
    }

    match &set.inner {
        SetEncoding::Intset(intset) => {
            if cursor != 0 {
                return (0, Vec::new());
            }
            let out = intset
                .iter()
                .map(|value| value.to_string().into_bytes())
                .filter(|member| pattern.is_none_or(|pattern| glob_match(pattern, member)))
                .collect();
            (0, out)
        }
        SetEncoding::Listpack(_) => {
            if cursor != 0 {
                return (0, Vec::new());
            }
            let out = set
                .iter()
                .map(|member| member.into_owned())
                .filter(|member| pattern.is_none_or(|pattern| glob_match(pattern, member)))
                .collect();
            (0, out)
        }
        SetEncoding::Hashtable(_) => {
            let len = set.len().max(1);
            let modulo = len.next_power_of_two().max(1);
            let mut cur = cursor % modulo as u64;
            let mut scanned = 0usize;
            let mut wrapped = false;
            let mut out = Vec::new();

            while scanned < count {
                if let Some(member) = set.iter().nth(cur as usize) {
                    let bytes = member.into_owned();
                    if pattern.is_none_or(|pattern| glob_match(pattern, &bytes)) {
                        out.push(bytes);
                    }
                    scanned += 1;
                }
                cur = reverse_binary_next(cur, modulo as u64);
                if cur == 0 {
                    wrapped = true;
                    break;
                }
            }
            (if wrapped { 0 } else { cur }, out)
        }
    }
}

fn reverse_binary_next(cursor: u64, modulo: u64) -> u64 {
    if modulo <= 1 {
        return 0;
    }
    let bits = modulo.trailing_zeros().max(1);
    let mask = (1u64 << bits) - 1;
    let low = cursor & mask;
    let rev = reverse_low_bits(low, bits);
    let next = rev.wrapping_add(1) & mask;
    reverse_low_bits(next, bits) & mask
}

fn reverse_low_bits(mut value: u64, bits: u32) -> u64 {
    let mut out = 0u64;
    let mut i = 0;
    while i < bits {
        out = (out << 1) | (value & 1);
        value >>= 1;
        i += 1;
    }
    out
}

fn ensure_set_type_or_missing(store: &mut Store, key: &[u8]) -> SenkoResult<()> {
    if let Some(value) = store.get(key).cloned()
        && !matches!(value, SenkoValue::Set(_))
    {
        return Err(SenkoError::WrongType {
            expected: "set",
            actual: actual_type(&value),
        });
    }
    Ok(())
}

fn actual_type(value: &SenkoValue) -> &'static str {
    match value {
        SenkoValue::Raw(_) | SenkoValue::Int(_) | SenkoValue::Float(_) => "string",
        SenkoValue::Hash(_) => "hash",
        SenkoValue::List(_) => "list",
        SenkoValue::Set(_) => "set",
        SenkoValue::Stream(_) => "stream",
        SenkoValue::ZSet(_) => "zset",
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_) => "MBbloom--",
        #[cfg(feature = "prob")]
        SenkoValue::CuckooFilter(_) => "cuckooFilter",
        #[cfg(feature = "prob")]
        SenkoValue::CountMinSketch(_) => "CMSk--",
        #[cfg(feature = "prob")]
        SenkoValue::TopK(_) => "topk",
        #[cfg(feature = "prob")]
        SenkoValue::TDigest(_) => "TDIS-TYPE",
        #[cfg(feature = "json")]
        SenkoValue::Json(_) => "json",
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => "vectorset",
    }
}

fn parse_u64(raw: &[u8]) -> SenkoResult<u64> {
    std::str::from_utf8(raw)
        .map_err(|_| SenkoError::Protocol("invalid cursor"))?
        .parse::<u64>()
        .map_err(|_| SenkoError::Protocol("invalid cursor"))
}

fn parse_usize(raw: &[u8]) -> SenkoResult<usize> {
    std::str::from_utf8(raw)
        .map_err(|_| SenkoError::Protocol("syntax error"))?
        .parse::<usize>()
        .map_err(|_| SenkoError::Protocol("syntax error"))
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
    use std::collections::HashSet;

    use compact_str::CompactString;
    use senko_core::{SenkoValue, SetObject};
    use senko_proto::Frame;

    use super::sscan;
    use crate::commands::Response;
    use crate::store::Store;

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn members(response: Response) -> (String, HashSet<Vec<u8>>) {
        let Response::Array(top) = response else {
            panic!("expected array")
        };
        let Response::Value(Some(SenkoValue::Raw(cursor))) = &top[0] else {
            panic!("expected cursor")
        };
        let Response::Array(values) = &top[1] else {
            panic!("expected values")
        };
        let out = values
            .iter()
            .map(|value| match value {
                Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        (String::from_utf8(cursor.to_vec()).unwrap(), out)
    }

    #[test]
    fn intset_scans_all_at_once() {
        let mut store = Store::default();
        let mut set = SetObject::default();
        let _ = set.add(b"1");
        let _ = set.add(b"2");
        let _ = store.set(
            CompactString::from("s"),
            SenkoValue::Set(Box::new(set)),
            Default::default(),
        );
        let (cursor, values) =
            members(sscan(&mut store, &[bs(b"s"), bs(b"0"), bs(b"COUNT"), bs(b"1")]).unwrap());
        assert_eq!(cursor, "0");
        assert_eq!(values, HashSet::from([b"1".to_vec(), b"2".to_vec()]));
    }

    #[test]
    fn hashtable_scan_pages_and_terminates() {
        let mut store = Store::default();
        let mut set = SetObject::default();
        for i in 0..200 {
            let _ = set.add(format!("v{i}").as_bytes());
        }
        let _ = store.set(
            CompactString::from("s"),
            SenkoValue::Set(Box::new(set)),
            Default::default(),
        );
        let mut cursor = b"0".to_vec();
        let mut seen = HashSet::new();
        loop {
            let response = sscan(
                &mut store,
                &[
                    bs(b"s"),
                    Frame::BulkString(cursor.as_slice()),
                    bs(b"COUNT"),
                    bs(b"7"),
                ],
            )
            .unwrap();
            let (next, values) = members(response);
            seen.extend(values);
            cursor = next.into_bytes();
            if cursor == b"0" {
                break;
            }
        }
        assert_eq!(seen.len(), 200);
    }

    #[test]
    fn match_filters() {
        let mut store = Store::default();
        let mut set = SetObject::default();
        for value in [b"foo".as_slice(), b"bar", b"fizz"] {
            let _ = set.add(value);
        }
        let _ = store.set(
            CompactString::from("s"),
            SenkoValue::Set(Box::new(set)),
            Default::default(),
        );
        let (_, values) =
            members(sscan(&mut store, &[bs(b"s"), bs(b"0"), bs(b"MATCH"), bs(b"f*")]).unwrap());
        assert_eq!(values, HashSet::from([b"foo".to_vec(), b"fizz".to_vec()]));
    }
}
