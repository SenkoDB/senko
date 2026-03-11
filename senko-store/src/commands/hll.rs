use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    hll::{Hll, HllError, PfDebugSubcommand},
    store::{SetCondition, SetExpiry, SetOptions, Store},
};

#[inline]
pub fn pfadd(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'pfadd' command",
        ));
    }
    let key_bytes = arg_bytes(&args[0])?;
    let key = parse_key(key_bytes)?;
    let current = store.get_cloned(key_bytes);
    let mut hll = hll_from_value(current.as_ref())?;
    let existed = current.is_some();
    let mut changed = !existed;
    for frame in &args[1..] {
        changed |= hll.add(arg_bytes(frame)?);
    }
    store_hll(store, key, existed, &hll)?;
    Ok(Response::Integer(changed as i64))
}

#[inline]
pub fn pfcount(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'pfcount' command",
        ));
    }
    if args.len() == 1 {
        let key_bytes = arg_bytes(&args[0])?;
        let current = store.get_cloned(key_bytes);
        let existed = current.is_some();
        let mut hll = hll_from_value(current.as_ref())?;
        let count = hll.count();
        if existed {
            let key = parse_key(key_bytes)?;
            store_hll(store, key, true, &hll)?;
        }
        return Ok(Response::Integer(count as i64));
    }

    let mut merged = Hll::new();
    merged.to_dense();
    for frame in args {
        let key = arg_bytes(frame)?;
        if let Some(value) = store.get_cloned(key) {
            let hll = hll_from_value(Some(&value))?;
            merged.merge_from(&hll);
        }
    }
    Ok(Response::Integer(merged.count() as i64))
}

#[inline]
pub fn pfmerge(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'pfmerge' command",
        ));
    }

    let dest_bytes = arg_bytes(&args[0])?;
    let dest = parse_key(dest_bytes)?;
    let dest_exists = store.exists(dest_bytes);
    let mut merged = Hll::new();
    merged.to_dense();
    if args.len() == 1 {
        store_hll(store, dest, dest_exists, &merged)?;
        return Ok(Response::Simple(b"OK"));
    }

    for frame in &args[1..] {
        let key = arg_bytes(frame)?;
        if let Some(value) = store.get_cloned(key) {
            let hll = hll_from_value(Some(&value))?;
            merged.merge_from(&hll);
        }
    }
    store_hll(store, dest, dest_exists, &merged)?;
    Ok(Response::Simple(b"OK"))
}

#[inline]
pub fn pfdebug(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'pfdebug' command",
        ));
    }
    if arg_bytes(&args[0])?.eq_ignore_ascii_case(b"SIMD") {
        if args.len() != 2 {
            return Err(SenkoError::Protocol(
                "wrong number of arguments for 'pfdebug' command",
            ));
        }
        let mode = arg_bytes(&args[1])?;
        if !mode.eq_ignore_ascii_case(b"ON") && !mode.eq_ignore_ascii_case(b"OFF") {
            return Err(SenkoError::Protocol("syntax error"));
        }
        return Ok(Response::Simple(b"OK"));
    }
    let sub = parse_pfdebug_subcommand(arg_bytes(&args[0])?)?;

    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'pfdebug' command",
        ));
    }
    let key_bytes = arg_bytes(&args[1])?;
    let key = parse_key(key_bytes)?;
    let current = store.get_cloned(key_bytes);
    let mut hll = hll_from_value(current.as_ref())?;
    match sub {
        PfDebugSubcommand::GetReg => Ok(Response::Array(Box::new(SmallVec::from_iter(
            hll.get_registers()
                .into_iter()
                .map(|reg| Response::Integer(reg as i64)),
        )))),
        PfDebugSubcommand::Decode => {
            let desc = hll.describe();
            Ok(Response::Value(Some(SenkoValue::Raw(Bytes::from(
                format!(
                    "{} payload={} zeros={} max={}",
                    desc.encoding, desc.payload_len, desc.zero_registers, desc.max_register
                ),
            )))))
        }
        PfDebugSubcommand::Encode => {
            hll.reencode();
            store_hll(store, key, current.is_some(), &hll)?;
            Ok(Response::Simple(b"OK"))
        }
        PfDebugSubcommand::ToDense => {
            hll.to_dense();
            store_hll(store, key, current.is_some(), &hll)?;
            Ok(Response::Simple(b"OK"))
        }
        PfDebugSubcommand::Encoding => Ok(Response::Value(Some(SenkoValue::Raw(
            Bytes::from_static(hll.encoding_name().as_bytes()),
        )))),
        PfDebugSubcommand::SimdOn | PfDebugSubcommand::SimdOff => unreachable!(),
    }
}

#[inline]
pub fn pfselftest(_store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if !args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'pfselftest' command",
        ));
    }
    let checkpoints = [100usize, 1_000, 10_000, 100_000];
    let mut hll = Hll::new();
    let mut next = 0usize;
    for i in 1..=100_000usize {
        let _ = hll.add(format!("selftest:{i}").as_bytes());
        if next < checkpoints.len() && i == checkpoints[next] {
            let estimate = hll.count() as i64;
            let truth = i as i64;
            let error = (estimate - truth).unsigned_abs() as f64 / truth as f64;
            if i >= 1_000 && error > 0.015 {
                return Err(SenkoError::Protocol("PFSELFTEST failed"));
            }
            next += 1;
        }
    }
    Ok(Response::Simple(b"OK"))
}

fn parse_pfdebug_subcommand(raw: &[u8]) -> SenkoResult<PfDebugSubcommand> {
    if raw.eq_ignore_ascii_case(b"GETREG") {
        Ok(PfDebugSubcommand::GetReg)
    } else if raw.eq_ignore_ascii_case(b"DECODE") {
        Ok(PfDebugSubcommand::Decode)
    } else if raw.eq_ignore_ascii_case(b"ENCODE") {
        Ok(PfDebugSubcommand::Encode)
    } else if raw.eq_ignore_ascii_case(b"TODENSE") {
        Ok(PfDebugSubcommand::ToDense)
    } else if raw.eq_ignore_ascii_case(b"ENCODING") {
        Ok(PfDebugSubcommand::Encoding)
    } else {
        Err(SenkoError::Protocol("syntax error"))
    }
}

fn hll_from_value(value: Option<&SenkoValue>) -> SenkoResult<Hll> {
    match value {
        None => Ok(Hll::new()),
        Some(SenkoValue::Raw(bytes)) => Hll::from_bytes(Some(bytes)).map_err(map_hll_error),
        Some(SenkoValue::Int(value)) => {
            let rendered = value.to_string();
            Hll::parse(rendered.as_bytes()).map_err(map_hll_error)
        }
        Some(SenkoValue::Float(value)) => {
            let rendered = value.to_string();
            Hll::parse(rendered.as_bytes()).map_err(map_hll_error)
        }
        Some(SenkoValue::Hash(_)) => Err(wrong_type("hash")),
        Some(SenkoValue::List(_)) => Err(wrong_type("list")),
        Some(SenkoValue::Set(_)) => Err(wrong_type("set")),
        Some(SenkoValue::Stream(_)) => Err(wrong_type("stream")),
        Some(SenkoValue::ZSet(_)) => Err(wrong_type("zset")),
        #[cfg(feature = "prob")]
        Some(SenkoValue::BloomFilter(_)) => Err(wrong_type("MBbloom--")),
        #[cfg(feature = "prob")]
        Some(SenkoValue::CuckooFilter(_)) => Err(wrong_type("cuckooFilter")),
        #[cfg(feature = "prob")]
        Some(SenkoValue::CountMinSketch(_)) => Err(wrong_type("CMSk--")),
        #[cfg(feature = "prob")]
        Some(SenkoValue::TopK(_)) => Err(wrong_type("topk")),
        #[cfg(feature = "prob")]
        Some(SenkoValue::TDigest(_)) => Err(wrong_type("TDIS-TYPE")),
        #[cfg(feature = "json")]
        Some(SenkoValue::Json(_)) => Err(wrong_type("json")),
        #[cfg(feature = "vector")]
        Some(SenkoValue::VectorSet(_)) => Err(wrong_type("vectorset")),
    }
}

fn map_hll_error(error: HllError) -> SenkoError {
    match error {
        HllError::WrongType => {
            SenkoError::Protocol("ERR WRONGTYPE Key is not a valid HyperLogLog string value")
        }
        HllError::InvalidObject => SenkoError::Protocol("INVALIDOBJ Corrupted HLL object detected"),
    }
}

fn wrong_type(actual: &'static str) -> SenkoError {
    SenkoError::WrongType {
        expected: "string",
        actual,
    }
}

fn store_hll(store: &mut Store, key: CompactString, existed: bool, hll: &Hll) -> SenkoResult<()> {
    let _ = store.set(
        key,
        SenkoValue::Raw(hll.to_bytes()),
        SetOptions {
            condition: SetCondition::Always,
            expiry: if existed {
                SetExpiry::KeepTtl
            } else {
                SetExpiry::None
            },
            get_old: false,
        },
    );
    Ok(())
}

fn parse_key(raw: &[u8]) -> SenkoResult<CompactString> {
    let key = std::str::from_utf8(raw).map_err(|_| SenkoError::Protocol("invalid UTF-8 key"))?;
    Ok(CompactString::new(key))
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
    use senko_proto::Frame;

    use crate::{
        Store,
        commands::{Response, hll as hllcmd},
        hll::{estimate_counter, reset_estimate_counter},
    };

    fn bs<'a>(value: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(value)
    }

    fn int_of(response: Response) -> i64 {
        match response {
            Response::Integer(value) => value,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn pfadd_pfcount_roundtrip() {
        let mut store = Store::default();
        assert_eq!(
            int_of(hllcmd::pfadd(&mut store, &[bs(b"h"), bs(b"a"), bs(b"b")]).unwrap()),
            1
        );
        let count = int_of(hllcmd::pfcount(&mut store, &[bs(b"h")]).unwrap());
        assert!((count - 2).abs() <= 1);
    }

    #[test]
    fn pfcount_uses_cached_value() {
        let mut store = Store::default();
        let _ = hllcmd::pfadd(&mut store, &[bs(b"h"), bs(b"a"), bs(b"b"), bs(b"c")]).unwrap();
        reset_estimate_counter();
        let _ = hllcmd::pfcount(&mut store, &[bs(b"h")]).unwrap();
        let first = estimate_counter();
        assert!(first >= 1);
        let _ = hllcmd::pfcount(&mut store, &[bs(b"h")]).unwrap();
        assert_eq!(estimate_counter(), first);
    }

    #[test]
    fn pfmerge_creates_empty_dest() {
        let mut store = Store::default();
        assert_eq!(
            hllcmd::pfmerge(&mut store, &[bs(b"dst")]).unwrap(),
            Response::Simple(b"OK")
        );
        assert_eq!(
            int_of(hllcmd::pfcount(&mut store, &[bs(b"dst")]).unwrap()),
            0
        );
    }
}
