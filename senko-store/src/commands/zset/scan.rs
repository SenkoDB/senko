use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue, ZSetEncoding, ZSetObject};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    commands::zset::basic::{arg_bytes, ensure_zset_type_or_missing, formatted_score_value},
    pattern::glob_match,
    store::Store,
};

#[inline]
pub fn zscan(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zscan' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let cursor = parse_cursor(arg_bytes(&args[1])?)?;

    let mut idx = 2usize;
    let mut pattern: Option<&[u8]> = None;
    let mut count = 10usize;
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

    let (next, page) = if let Some(zset) = store.get_zset(key) {
        zscan_step(zset, cursor, count, pattern)
    } else {
        (ScanCursor::zero(), Vec::new())
    };

    let mut top = SmallVec::<[Response; 16]>::new();
    top.push(Response::Value(Some(SenkoValue::Raw(Bytes::from(
        encode_cursor(next).into_bytes(),
    )))));
    let mut values = SmallVec::<[Response; 16]>::new();
    for (member, score) in page {
        values.push(Response::Value(Some(SenkoValue::Raw(
            Bytes::copy_from_slice(member.as_bytes()),
        ))));
        values.push(Response::Value(Some(formatted_score_value(score))));
    }
    top.push(Response::Array(Box::new(values)));
    Ok(Response::Array(Box::new(top)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScanCursor {
    generation: u64,
    offset: usize,
}

impl ScanCursor {
    fn zero() -> Self {
        Self {
            generation: 0,
            offset: 0,
        }
    }
}

fn zscan_step(
    zset: &ZSetObject,
    cursor: ScanCursor,
    count: usize,
    pattern: Option<&[u8]>,
) -> (ScanCursor, Vec<(CompactString, f64)>) {
    match &zset.inner {
        ZSetEncoding::Listpack(_) => {
            if cursor.offset != 0 {
                return (ScanCursor::zero(), Vec::new());
            }
            let entries = zset
                .range_by_rank(0, -1, false, None)
                .filter(|(_, member)| pattern.is_none_or(|p| glob_match(p, member.as_bytes())))
                .map(|(score, member)| (member, score))
                .collect();
            (ScanCursor::zero(), entries)
        }
        ZSetEncoding::BPTree { .. } => {
            let generation = zset.generation();
            let mut offset = if cursor.generation == 0 || cursor.generation == generation {
                cursor.offset
            } else {
                0
            };
            let entries: Vec<_> = zset.range_by_rank(offset as i64, -1, false, None).collect();
            let mut scanned = 0usize;
            let mut out = Vec::new();
            for (score, member) in entries {
                scanned += 1;
                if pattern.is_none_or(|p| glob_match(p, member.as_bytes())) {
                    out.push((member, score));
                    if out.len() >= count {
                        break;
                    }
                }
            }
            offset += scanned;
            let next = if offset >= zset.len() {
                ScanCursor::zero()
            } else {
                ScanCursor { generation, offset }
            };
            (next, out)
        }
    }
}

fn encode_cursor(cursor: ScanCursor) -> String {
    if cursor.offset == 0 {
        "0".to_owned()
    } else {
        format!("{}:{}", cursor.generation, cursor.offset)
    }
}

fn parse_cursor(raw: &[u8]) -> SenkoResult<ScanCursor> {
    if raw == b"0" {
        return Ok(ScanCursor::zero());
    }
    let text = std::str::from_utf8(raw).map_err(|_| SenkoError::Protocol("invalid cursor"))?;
    let Some((generation, offset)) = text.split_once(':') else {
        let offset = text
            .parse::<usize>()
            .map_err(|_| SenkoError::Protocol("invalid cursor"))?;
        return Ok(ScanCursor {
            generation: 0,
            offset,
        });
    };
    Ok(ScanCursor {
        generation: generation
            .parse::<u64>()
            .map_err(|_| SenkoError::Protocol("invalid cursor"))?,
        offset: offset
            .parse::<usize>()
            .map_err(|_| SenkoError::Protocol("invalid cursor"))?,
    })
}

fn parse_usize(raw: &[u8]) -> SenkoResult<usize> {
    std::str::from_utf8(raw)
        .map_err(|_| SenkoError::Protocol("syntax error"))?
        .parse::<usize>()
        .map_err(|_| SenkoError::Protocol("syntax error"))
}

#[cfg(test)]
mod tests {
    use senko_proto::Frame;

    use super::*;
    use crate::commands::zset::basic::zadd;

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn page(response: Response) -> (Vec<u8>, Vec<Vec<u8>>) {
        let Response::Array(top) = response else {
            panic!("expected array");
        };
        let Response::Value(Some(SenkoValue::Raw(cursor))) = &top[0] else {
            panic!("expected cursor")
        };
        let Response::Array(values) = &top[1] else {
            panic!("expected values")
        };
        let bytes = values
            .iter()
            .map(|item| match item {
                Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        (cursor.to_vec(), bytes)
    }

    #[test]
    fn zscan_full_iteration() {
        let mut store = Store::default();
        let mut args = vec![bs(b"zs")];
        let score_bytes: Vec<&'static [u8]> = (0..130)
            .map(|i| Box::leak(i.to_string().into_boxed_str()).as_bytes())
            .collect();
        let member_bytes: Vec<&'static [u8]> = (0..130)
            .map(|i| Box::leak(format!("m{i:03}").into_boxed_str()).as_bytes())
            .collect();
        for i in 0..130 {
            args.push(Frame::BulkString(score_bytes[i]));
            args.push(Frame::BulkString(member_bytes[i]));
        }
        let _ = zadd(&mut store, &args).unwrap();

        let mut cursor = b"0".to_vec();
        let mut seen = 0usize;
        loop {
            let leaked = Box::leak(cursor.clone().into_boxed_slice());
            let (next, values) = page(
                zscan(
                    &mut store,
                    &[bs(b"zs"), Frame::BulkString(leaked), bs(b"COUNT"), bs(b"2")],
                )
                .unwrap(),
            );
            seen += values.len() / 2;
            if next == b"0".to_vec() {
                break;
            }
            cursor = next;
        }
        assert_eq!(seen, 130);
    }

    #[test]
    fn zscan_match_filters_members() {
        let mut store = Store::default();
        let _ = zadd(
            &mut store,
            &[
                bs(b"zs"),
                bs(b"1"),
                bs(b"ax"),
                bs(b"2"),
                bs(b"by"),
                bs(b"3"),
                bs(b"az"),
            ],
        )
        .unwrap();
        let (_, values) =
            page(zscan(&mut store, &[bs(b"zs"), bs(b"0"), bs(b"MATCH"), bs(b"a*")]).unwrap());
        assert_eq!(
            values,
            vec![b"ax".to_vec(), b"1".to_vec(), b"az".to_vec(), b"3".to_vec()]
        );
    }

    #[test]
    fn zscan_generation_mismatch_restarts() {
        let mut store = Store::default();
        let mut args = vec![bs(b"zs")];
        let score_bytes: Vec<&'static [u8]> = (0..130)
            .map(|i| Box::leak(i.to_string().into_boxed_str()).as_bytes())
            .collect();
        let member_bytes: Vec<&'static [u8]> = (0..130)
            .map(|i| Box::leak(format!("m{i:03}").into_boxed_str()).as_bytes())
            .collect();
        for i in 0..130 {
            args.push(Frame::BulkString(score_bytes[i]));
            args.push(Frame::BulkString(member_bytes[i]));
        }
        let _ = zadd(&mut store, &args).unwrap();
        let (cursor, _) =
            page(zscan(&mut store, &[bs(b"zs"), bs(b"0"), bs(b"COUNT"), bs(b"1")]).unwrap());
        let _ = zadd(&mut store, &[bs(b"zs"), bs(b"4"), bs(b"d")]).unwrap();
        let leaked = Box::leak(cursor.into_boxed_slice());
        let (next, values) = page(
            zscan(
                &mut store,
                &[bs(b"zs"), Frame::BulkString(leaked), bs(b"COUNT"), bs(b"1")],
            )
            .unwrap(),
        );
        assert_ne!(next, b"0".to_vec());
        assert_eq!(values.len(), 2);
    }
}
