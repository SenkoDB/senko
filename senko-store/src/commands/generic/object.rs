use bytes::Bytes;
use senko_core::{SenkoError, SenkoResult, SenkoValue, SetEncoding, ZSetEncoding};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    Response,
    hll::Hll,
    store::{Entry, Store, current_unix_ms},
};

const HELP_LINES: [&[u8]; 11] = [
    b"OBJECT <subcommand> [<arg> [value] [opt] ...]. subcommands are:",
    b"ENCODING <key>",
    b"    Return the kind of internal representation the Redis object uses.",
    b"FREQ <key>",
    b"    Return the access frequency index of the key. ...",
    b"HELP",
    b"    Return subcommand help summary.",
    b"IDLETIME <key>",
    b"    Return the idle time of the key, that is the approximated ...",
    b"REFCOUNT <key>",
    b"    Return the reference count of the object stored at <key>.",
];

#[inline]
pub fn object(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'object' command",
        ));
    }
    let sub = arg_bytes(&args[0])?;
    if sub.eq_ignore_ascii_case(b"ENCODING") {
        return object_encoding(store, &args[1..]);
    }
    if sub.eq_ignore_ascii_case(b"IDLETIME") {
        return object_idletime(store, &args[1..]);
    }
    if sub.eq_ignore_ascii_case(b"FREQ") {
        return object_freq(store, &args[1..]);
    }
    if sub.eq_ignore_ascii_case(b"REFCOUNT") {
        return object_refcount(store, &args[1..]);
    }
    if sub.eq_ignore_ascii_case(b"HELP") {
        return object_help(store, &args[1..]);
    }
    Err(SenkoError::ProtocolMessage(format_unknown_subcommand(sub)))
}

#[inline]
pub fn object_encoding(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'object|encoding' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let Some(entry) = store.clone_entry(key) else {
        return Ok(Response::Value(None));
    };
    Ok(Response::Value(Some(SenkoValue::Raw(Bytes::from_static(
        encoding_name(&entry),
    )))))
}

#[inline]
pub fn object_idletime(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'object|idletime' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let Some(entry) = store.clone_entry(key) else {
        return Ok(Response::Value(None));
    };
    let now_coarse = (current_unix_ms() / 10_000) as u32;
    let idle_seconds = now_coarse.saturating_sub(entry.lru_clock.get()) as i64 * 10;
    Ok(Response::Integer(idle_seconds))
}

#[inline]
pub fn object_freq(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'object|freq' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    if store.clone_entry(key).is_none() {
        return Ok(Response::Value(None));
    }
    // FUTURE: implement LFU counter for allkeys-lfu eviction.
    Ok(Response::Integer(0))
}

#[inline]
pub fn object_refcount(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'object|refcount' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    if store.clone_entry(key).is_none() {
        return Ok(Response::Value(None));
    }
    Ok(Response::Integer(1))
}

#[inline]
pub fn object_help(_store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if !args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'object|help' command",
        ));
    }
    Ok(Response::Array(Box::new(SmallVec::from_iter(
        HELP_LINES
            .into_iter()
            .map(|line| Response::Value(Some(SenkoValue::Raw(Bytes::from_static(line))))),
    ))))
}

fn encoding_name(entry: &Entry) -> &'static [u8] {
    match &entry.value {
        SenkoValue::Int(_) => b"int",
        SenkoValue::Raw(raw) => {
            if Hll::parse(raw.as_ref()).is_ok() {
                b"raw"
            } else if raw.len() <= 44 {
                b"embstr"
            } else {
                b"raw"
            }
        }
        SenkoValue::Float(_) => b"embstr",
        SenkoValue::Hash(hash) => {
            if hash.is_listpack() {
                b"listpack"
            } else {
                b"hashtable"
            }
        }
        SenkoValue::List(list) => {
            if list.node_count <= 1 {
                b"listpack"
            } else {
                b"quicklist"
            }
        }
        SenkoValue::Set(set) => match &set.inner {
            SetEncoding::Intset(_) => b"intset",
            SetEncoding::Listpack(_) => b"listpack",
            SetEncoding::Hashtable(_) => b"hashtable",
        },
        SenkoValue::ZSet(zset) => match &zset.inner {
            ZSetEncoding::Listpack(_) => b"listpack",
            ZSetEncoding::BPTree { .. } => b"skiplist",
        },
        SenkoValue::Stream(_) => b"stream",
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_) => b"MBbloom--",
        #[cfg(feature = "prob")]
        SenkoValue::CuckooFilter(_) => b"cuckooFilter",
        #[cfg(feature = "prob")]
        SenkoValue::CountMinSketch(_) => b"CMSk--",
        #[cfg(feature = "prob")]
        SenkoValue::TopK(_) => b"topk",
        #[cfg(feature = "prob")]
        SenkoValue::TDigest(_) => b"TDIS-TYPE",
        #[cfg(feature = "json")]
        SenkoValue::Json(_) => b"json",
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => b"vectorset",
    }
}

fn format_unknown_subcommand(sub: &[u8]) -> compact_str::CompactString {
    compact_str::CompactString::new(format!(
        "ERR unknown subcommand '{}' for 'object' command",
        String::from_utf8_lossy(sub)
    ))
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
    use std::cell::Cell;

    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_core::{
        HashObject, QuickList, SenkoError, SenkoValue, SetObject, StreamObject, ZAddOptions,
        ZSetObject,
    };
    use senko_proto::Frame;

    use super::{
        object, object_encoding, object_freq, object_help, object_idletime, object_refcount,
    };
    use crate::{Response, Store};

    fn bs<'a>(value: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(value)
    }

    fn raw(response: Response) -> Option<Vec<u8>> {
        match response {
            Response::Value(Some(SenkoValue::Raw(bytes))) => Some(bytes.to_vec()),
            Response::Value(None) => None,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn object_encoding_for_string_variants() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("int"),
            SenkoValue::Int(42),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("short"),
            SenkoValue::from(Bytes::from_static(b"hello")),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("long"),
            SenkoValue::from(Bytes::from(vec![b'x'; 45])),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("float"),
            SenkoValue::Float(1.5),
            Default::default(),
        );

        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"int")]).unwrap()).unwrap(),
            b"int".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"short")]).unwrap()).unwrap(),
            b"embstr".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"long")]).unwrap()).unwrap(),
            b"raw".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"float")]).unwrap()).unwrap(),
            b"embstr".to_vec()
        );
    }

    #[test]
    fn object_encoding_for_collections() {
        let mut store = Store::default();

        let mut hash_lp = HashObject::default();
        for i in 0..5 {
            let _ = hash_lp.set(
                CompactString::new(format!("f{i}")),
                SenkoValue::from(Bytes::from_static(b"v")),
                None,
            );
        }
        let _ = store.set(
            CompactString::new("h1"),
            SenkoValue::Hash(Box::new(hash_lp)),
            Default::default(),
        );

        let mut hash_ht = HashObject::default();
        for i in 0..200 {
            let _ = hash_ht.set(
                CompactString::new(format!("f{i}")),
                SenkoValue::from(Bytes::from_static(b"v")),
                None,
            );
        }
        let _ = store.set(
            CompactString::new("h2"),
            SenkoValue::Hash(Box::new(hash_ht)),
            Default::default(),
        );

        let mut zset = ZSetObject::default();
        for i in 0..200 {
            let _ = zset.add(
                i as f64,
                CompactString::new(format!("m{i}")),
                ZAddOptions::default(),
            );
        }
        let _ = store.set(
            CompactString::new("z"),
            SenkoValue::ZSet(Box::new(zset)),
            Default::default(),
        );

        let mut intset = SetObject::default();
        let _ = intset.add(b"1");
        let _ = intset.add(b"2");
        let _ = store.set(
            CompactString::new("s1"),
            SenkoValue::Set(Box::new(intset)),
            Default::default(),
        );

        let mut set_lp = SetObject::default();
        let _ = set_lp.add(b"a");
        let _ = set_lp.add(b"b");
        let _ = store.set(
            CompactString::new("s2"),
            SenkoValue::Set(Box::new(set_lp)),
            Default::default(),
        );

        let mut set_ht = SetObject::default();
        for i in 0..200 {
            let _ = set_ht.add(format!("m{i}").as_bytes());
        }
        let _ = store.set(
            CompactString::new("s3"),
            SenkoValue::Set(Box::new(set_ht)),
            Default::default(),
        );

        let mut one_node = QuickList::default();
        one_node.push_back(b"a");
        let _ = store.set(
            CompactString::new("l1"),
            SenkoValue::List(Box::new(one_node)),
            Default::default(),
        );

        let mut multi_node = QuickList::default();
        for _ in 0..130 {
            multi_node.push_back(b"a");
        }
        let _ = store.set(
            CompactString::new("l2"),
            SenkoValue::List(Box::new(multi_node)),
            Default::default(),
        );

        let _ = store.set(
            CompactString::new("stream"),
            SenkoValue::Stream(Box::new(StreamObject::new())),
            Default::default(),
        );

        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"h1")]).unwrap()).unwrap(),
            b"listpack".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"h2")]).unwrap()).unwrap(),
            b"hashtable".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"z")]).unwrap()).unwrap(),
            b"skiplist".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"s1")]).unwrap()).unwrap(),
            b"intset".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"s2")]).unwrap()).unwrap(),
            b"listpack".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"s3")]).unwrap()).unwrap(),
            b"hashtable".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"l1")]).unwrap()).unwrap(),
            b"listpack".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"l2")]).unwrap()).unwrap(),
            b"quicklist".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"stream")]).unwrap()).unwrap(),
            b"stream".to_vec()
        );
        assert_eq!(
            raw(object_encoding(&mut store, &[bs(b"missing")]).unwrap()),
            None
        );
    }

    #[test]
    fn object_idletime_freq_refcount_and_help() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("k"),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
        if let Some(entry) = store.get_mut(b"k") {
            entry.lru_clock = Cell::new(entry.lru_clock.get().saturating_sub(1));
        }
        let Response::Integer(idle) = object_idletime(&mut store, &[bs(b"k")]).unwrap() else {
            panic!()
        };
        assert!(idle >= 10);
        assert_eq!(
            object_freq(&mut store, &[bs(b"k")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            object_refcount(&mut store, &[bs(b"k")]).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            object_refcount(&mut store, &[bs(b"missing")]).unwrap(),
            Response::Value(None)
        );

        let Response::Array(help) = object_help(&mut store, &[]).unwrap() else {
            panic!()
        };
        assert!(help.len() >= 6);
    }

    #[test]
    fn object_unknown_subcommand_errors() {
        let mut store = Store::default();
        let err = object(&mut store, &[bs(b"wat")]).unwrap_err();
        assert!(
            matches!(err, SenkoError::ProtocolMessage(message) if message.as_str() == "ERR unknown subcommand 'wat' for 'object' command")
        );
    }
}
