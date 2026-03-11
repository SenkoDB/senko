use bytes::{Bytes, BytesMut};
use compact_str::CompactString;
use crc::{Algorithm, Crc};
use senko_core::{SenkoError, SenkoResult, SenkoValue, StreamId, StreamRefMode};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    Response,
    store::{Store, current_unix_ms},
};

const RDB_VERSION: u16 = 10;
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
const RDB_TYPE_HASH_LISTPACK: u8 = 16;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_SET_INTSET: u8 = 11;
const RDB_TYPE_SET_LISTPACK: u8 = 20;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
const RDB_TYPE_ZSET_2: u8 = 5;
const RDB_TYPE_STREAM_LISTPACKS: u8 = 19;

const CRC64_REDIS: Algorithm<u64> = Algorithm {
    width: 64,
    poly: 0xAD93D23594C935A9,
    init: 0,
    refin: true,
    refout: true,
    xorout: 0,
    check: 0,
    residue: 0,
};

#[inline]
pub fn dump(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'dump' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let Some(entry) = store.clone_entry(key) else {
        return Ok(Response::Value(None));
    };
    Ok(Response::Value(Some(SenkoValue::Raw(dump_value(
        &entry.value,
        entry.expires_at,
    )))))
}

#[inline]
pub fn restore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'restore' command",
        ));
    }
    let key = parse_key(arg_bytes(&args[0])?)?;
    let ttl_raw = arg_bytes(&args[1])?;
    let data = arg_bytes(&args[2])?;
    let mut replace = false;
    let mut absttl = false;
    let mut idx = 3usize;
    while idx < args.len() {
        let token = arg_bytes(&args[idx])?;
        if token.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"ABSTTL") {
            absttl = true;
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"IDLETIME") || token.eq_ignore_ascii_case(b"FREQ") {
            idx += 1;
            if idx >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            let _ = parse_u64(arg_bytes(&args[idx])?)?;
            idx += 1;
            continue;
        }
        return Err(SenkoError::Protocol("syntax error"));
    }

    if !replace && store.type_name(key.as_bytes()).is_some() {
        return Err(SenkoError::Protocol(
            "BUSYKEY Target key name already exists",
        ));
    }

    let ttl = parse_u64(ttl_raw)?;
    let value = restore_value(data)?;
    let _ = store.set(key.clone(), value, Default::default());
    if ttl == 0 {
        store.remove_expiry(key.as_bytes());
    } else {
        let expires_at = if absttl {
            ttl
        } else {
            current_unix_ms().saturating_add(ttl)
        };
        store.set_expiry(key.as_bytes(), expires_at);
    }
    Ok(Response::Simple(b"OK"))
}

#[inline]
pub fn r#move(_store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'move' command",
        ));
    }
    let _ = arg_bytes(&args[0])?;
    let db = parse_i64(arg_bytes(&args[1])?)?;
    // FUTURE: multi-DB support in Phase 2.
    if db == 0 {
        Ok(Response::Integer(0))
    } else {
        Err(SenkoError::Protocol("ERR DB index is out of range"))
    }
}

#[inline]
pub fn migrate(_store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 6 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'migrate' command",
        ));
    }
    for arg in args {
        let _ = arg_bytes(arg)?;
    }
    // FUTURE: implement MIGRATE when cross-node networking exists.
    Err(SenkoError::Protocol(
        "ERR MIGRATE not supported in Senko Phase 1",
    ))
}

#[inline]
pub fn wait(_store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'wait' command",
        ));
    }
    let _ = parse_i64(arg_bytes(&args[0])?)?;
    let _ = parse_i64(arg_bytes(&args[1])?)?;
    // FUTURE: return actual replica count after replication is implemented.
    Ok(Response::Integer(0))
}

#[inline]
pub fn waitaof(_store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'waitaof' command",
        ));
    }
    let _ = parse_i64(arg_bytes(&args[0])?)?;
    let _ = parse_i64(arg_bytes(&args[1])?)?;
    let _ = parse_i64(arg_bytes(&args[2])?)?;
    // FUTURE: return actual local AOF / replica durability counts.
    Ok(Response::Array(Box::new(SmallVec::from_iter([
        Response::Integer(0),
        Response::Integer(0),
    ]))))
}

pub fn dump_value(value: &SenkoValue, _ttl_ms: Option<u64>) -> Bytes {
    let mut out = BytesMut::new();
    write_value(&mut out, value);
    out.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let crc = Crc::<u64>::new(&CRC64_REDIS).checksum(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out.freeze()
}

pub fn dump_string(value: &SenkoValue) -> Bytes {
    let bytes = value.as_bytes();
    let mut out = BytesMut::new();
    write_len(&mut out, bytes.len() as u64);
    out.extend_from_slice(bytes.as_ref());
    out.freeze()
}

pub fn restore_value(data: &[u8]) -> Result<SenkoValue, SenkoError> {
    if data.len() < 11 {
        return Err(invalid_dump_error());
    }
    let checksum_offset = data.len() - 8;
    let version_offset = checksum_offset - 2;
    let body = &data[..version_offset];
    let version = u16::from_le_bytes(
        data.get(version_offset..checksum_offset)
            .ok_or_else(invalid_dump_error)?
            .try_into()
            .map_err(|_| invalid_dump_error())?,
    );
    if version > RDB_VERSION {
        return Err(invalid_dump_error());
    }
    let checksum = u64::from_le_bytes(
        data[checksum_offset..]
            .try_into()
            .map_err(|_| invalid_dump_error())?,
    );
    let expected = Crc::<u64>::new(&CRC64_REDIS).checksum(&data[..checksum_offset]);
    if checksum != expected {
        return Err(invalid_dump_error());
    }
    let mut cursor = 0usize;
    read_value(body, &mut cursor)
}

fn write_value(out: &mut BytesMut, value: &SenkoValue) {
    match value {
        SenkoValue::Raw(_) | SenkoValue::Int(_) | SenkoValue::Float(_) => {
            out.extend_from_slice(&[RDB_TYPE_STRING]);
            out.extend_from_slice(&dump_string(value));
        }
        #[cfg(feature = "json")]
        SenkoValue::Json(_) => {
            out.extend_from_slice(&[RDB_TYPE_STRING]);
            out.extend_from_slice(&dump_string(value));
        }
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => {
            out.extend_from_slice(&[RDB_TYPE_STRING]);
            out.extend_from_slice(&dump_string(value));
        }
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_)
        | SenkoValue::CuckooFilter(_)
        | SenkoValue::CountMinSketch(_)
        | SenkoValue::TopK(_)
        | SenkoValue::TDigest(_) => {
            out.extend_from_slice(&[RDB_TYPE_STRING]);
            out.extend_from_slice(&dump_string(value));
        }
        SenkoValue::List(list) => {
            out.extend_from_slice(&[RDB_TYPE_LIST_QUICKLIST_2]);
            write_len(out, list.len());
            for item in list.iter() {
                write_blob(out, item);
            }
        }
        SenkoValue::Hash(hash) => {
            out.extend_from_slice(&[if hash.is_listpack() {
                RDB_TYPE_HASH_LISTPACK
            } else {
                RDB_TYPE_HASH
            }]);
            let entries: Vec<_> = hash.iter_live(current_unix_ms()).collect();
            write_len(out, entries.len() as u64);
            for (field, value) in entries {
                write_blob(out, field.as_bytes());
                write_blob(out, value.value.as_bytes().as_ref());
            }
        }
        SenkoValue::Set(set) => {
            let tag = match &set.inner {
                senko_core::SetEncoding::Intset(_) => RDB_TYPE_SET_INTSET,
                senko_core::SetEncoding::Listpack(_) => RDB_TYPE_SET_LISTPACK,
                senko_core::SetEncoding::Hashtable(_) => RDB_TYPE_SET,
            };
            out.extend_from_slice(&[tag]);
            let members: Vec<_> = set.iter().map(|member| member.into_owned()).collect();
            write_len(out, members.len() as u64);
            for member in members {
                write_blob(out, &member);
            }
        }
        SenkoValue::ZSet(zset) => {
            out.extend_from_slice(&[match &zset.inner {
                senko_core::ZSetEncoding::Listpack(_) => RDB_TYPE_ZSET_LISTPACK,
                senko_core::ZSetEncoding::BPTree { .. } => RDB_TYPE_ZSET_2,
            }]);
            let entries: Vec<_> = zset.range_by_rank(0, -1, false, None).collect();
            write_len(out, entries.len() as u64);
            for (score, member) in entries {
                write_blob(out, member.as_bytes());
                out.extend_from_slice(&score.to_le_bytes());
            }
        }
        SenkoValue::Stream(stream) => {
            out.extend_from_slice(&[RDB_TYPE_STREAM_LISTPACKS]);
            let entries: Vec<_> = stream
                .tree
                .range(StreamId::ZERO, StreamId::MAX, None)
                .collect();
            write_len(out, entries.len() as u64);
            for (id, fields) in entries {
                out.extend_from_slice(&id.ms.to_le_bytes());
                out.extend_from_slice(&id.seq.to_le_bytes());
                write_len(out, fields.len() as u64);
                for (field, value) in fields {
                    write_blob(out, &field);
                    write_blob(out, &value);
                }
            }
        }
    }
}

fn read_value(data: &[u8], cursor: &mut usize) -> Result<SenkoValue, SenkoError> {
    let tag = *data.get(*cursor).ok_or_else(invalid_dump_error)?;
    *cursor += 1;
    match tag {
        RDB_TYPE_STRING => Ok(SenkoValue::encode_attempt(
            read_blob(data, cursor)?.as_ref(),
        )),
        RDB_TYPE_STREAM_LISTPACKS if looks_like_list(data, *cursor) => {
            let len = read_len(data, cursor)? as usize;
            let mut list = senko_core::QuickList::default();
            for _ in 0..len {
                let value = read_blob(data, cursor)?;
                list.push_back(&value);
            }
            Ok(SenkoValue::List(Box::new(list)))
        }
        RDB_TYPE_LIST_QUICKLIST_2 => {
            let len = read_len(data, cursor)? as usize;
            let mut list = senko_core::QuickList::default();
            for _ in 0..len {
                let value = read_blob(data, cursor)?;
                list.push_back(&value);
            }
            Ok(SenkoValue::List(Box::new(list)))
        }
        RDB_TYPE_HASH_LISTPACK | RDB_TYPE_HASH => {
            let len = read_len(data, cursor)? as usize;
            let mut hash = senko_core::HashObject::default();
            for _ in 0..len {
                let field = read_blob(data, cursor)?;
                let value = read_blob(data, cursor)?;
                let _ = hash.set(parse_key(&field)?, SenkoValue::encode_attempt(&value), None);
            }
            Ok(SenkoValue::Hash(Box::new(hash)))
        }
        RDB_TYPE_SET_INTSET | RDB_TYPE_SET_LISTPACK | RDB_TYPE_SET => {
            let len = read_len(data, cursor)? as usize;
            let mut set = senko_core::SetObject::default();
            for _ in 0..len {
                let member = read_blob(data, cursor)?;
                let _ = set.add(&member);
            }
            Ok(SenkoValue::Set(Box::new(set)))
        }
        RDB_TYPE_ZSET_LISTPACK | RDB_TYPE_ZSET_2 => {
            let len = read_len(data, cursor)? as usize;
            let mut zset = senko_core::ZSetObject::default();
            for _ in 0..len {
                let member = read_blob(data, cursor)?;
                let score_bytes: [u8; 8] = data
                    .get(*cursor..*cursor + 8)
                    .ok_or_else(invalid_dump_error)?
                    .try_into()
                    .map_err(|_| invalid_dump_error())?;
                *cursor += 8;
                let _ = zset.add(
                    f64::from_le_bytes(score_bytes),
                    parse_key(&member)?,
                    Default::default(),
                );
            }
            Ok(SenkoValue::ZSet(Box::new(zset)))
        }
        RDB_TYPE_STREAM_LISTPACKS => {
            let len = read_len(data, cursor)? as usize;
            let mut stream = senko_core::StreamObject::new();
            for _ in 0..len {
                let ms = u64::from_le_bytes(
                    data.get(*cursor..*cursor + 8)
                        .ok_or_else(invalid_dump_error)?
                        .try_into()
                        .map_err(|_| invalid_dump_error())?,
                );
                *cursor += 8;
                let seq = u64::from_le_bytes(
                    data.get(*cursor..*cursor + 8)
                        .ok_or_else(invalid_dump_error)?
                        .try_into()
                        .map_err(|_| invalid_dump_error())?,
                );
                *cursor += 8;
                let field_count = read_len(data, cursor)? as usize;
                let mut fields = Vec::with_capacity(field_count);
                for _ in 0..field_count {
                    fields.push((read_blob(data, cursor)?, read_blob(data, cursor)?));
                }
                let refs = fields
                    .iter()
                    .map(|(field, value)| (field.as_slice(), value.as_slice()))
                    .collect::<Vec<_>>();
                let _ = stream.tree.insert_with_mode(
                    StreamId { ms, seq },
                    &refs,
                    StreamRefMode::KeepRef,
                );
            }
            Ok(SenkoValue::Stream(Box::new(stream)))
        }
        _ => Err(invalid_dump_error()),
    }
}

fn looks_like_list(data: &[u8], cursor: usize) -> bool {
    let Ok(len) = read_len_peek(data, cursor) else {
        return true;
    };
    let mut offset = cursor + 8;
    for _ in 0..len {
        let Ok(item_len) = read_len_peek(data, offset) else {
            return false;
        };
        offset = offset.saturating_add(8).saturating_add(item_len as usize);
        if offset > data.len() {
            return false;
        }
    }
    true
}

fn read_len_peek(data: &[u8], cursor: usize) -> Result<u64, SenkoError> {
    let raw: [u8; 8] = data
        .get(cursor..cursor + 8)
        .ok_or_else(invalid_dump_error)?
        .try_into()
        .map_err(|_| invalid_dump_error())?;
    Ok(u64::from_le_bytes(raw))
}

fn write_blob(out: &mut BytesMut, value: &[u8]) {
    write_len(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn read_blob(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>, SenkoError> {
    let len = read_len(data, cursor)? as usize;
    let bytes = data
        .get(*cursor..*cursor + len)
        .ok_or_else(invalid_dump_error)?;
    *cursor += len;
    Ok(bytes.to_vec())
}

fn write_len(out: &mut BytesMut, len: u64) {
    out.extend_from_slice(&len.to_le_bytes());
}

fn read_len(data: &[u8], cursor: &mut usize) -> Result<u64, SenkoError> {
    let raw: [u8; 8] = data
        .get(*cursor..*cursor + 8)
        .ok_or_else(invalid_dump_error)?
        .try_into()
        .map_err(|_| invalid_dump_error())?;
    *cursor += 8;
    Ok(u64::from_le_bytes(raw))
}

fn invalid_dump_error() -> SenkoError {
    SenkoError::Protocol("ERR DUMP payload version or checksum are wrong")
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

fn parse_u64(raw: &[u8]) -> SenkoResult<u64> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or(SenkoError::Protocol(
            "value is not an integer or out of range",
        ))
}

fn parse_i64(raw: &[u8]) -> SenkoResult<i64> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or(SenkoError::Protocol(
            "value is not an integer or out of range",
        ))
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
    use senko_core::{HashObject, QuickList, SenkoValue, SetObject, ZAddOptions, ZSetObject};
    use senko_proto::Frame;

    use super::{dump, migrate, r#move, restore, wait, waitaof};
    use crate::{Response, Store};

    fn bs<'a>(value: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(value)
    }

    fn roundtrip(store: &mut Store, key: &'static [u8], new_key: &'static [u8]) {
        let Response::Value(Some(SenkoValue::Raw(payload))) = dump(store, &[bs(key)]).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            restore(
                store,
                &[bs(new_key), bs(b"0"), Frame::BulkString(payload.as_ref())]
            )
            .unwrap(),
            Response::Simple(b"OK")
        );
    }

    #[test]
    fn dump_restore_round_trip_for_core_types() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("s"),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
        let mut hash = HashObject::default();
        let _ = hash.set(
            CompactString::new("f"),
            SenkoValue::from(Bytes::from_static(b"v")),
            None,
        );
        let _ = store.set(
            CompactString::new("h"),
            SenkoValue::Hash(Box::new(hash)),
            Default::default(),
        );
        let mut list = QuickList::default();
        list.push_back(b"1");
        list.push_back(b"2");
        let _ = store.set(
            CompactString::new("l"),
            SenkoValue::List(Box::new(list)),
            Default::default(),
        );
        let mut set = SetObject::default();
        let _ = set.add(b"1");
        let _ = set.add(b"2");
        let _ = store.set(
            CompactString::new("set"),
            SenkoValue::Set(Box::new(set)),
            Default::default(),
        );
        let mut zset = ZSetObject::default();
        let _ = zset.add(1.0, CompactString::new("m"), ZAddOptions::default());
        let _ = store.set(
            CompactString::new("z"),
            SenkoValue::ZSet(Box::new(zset)),
            Default::default(),
        );

        roundtrip(&mut store, b"s", b"s2");
        roundtrip(&mut store, b"h", b"h2");
        roundtrip(&mut store, b"l", b"l2");
        roundtrip(&mut store, b"set", b"set2");
        roundtrip(&mut store, b"z", b"z2");
        assert!(store.type_name(b"s2").is_some());
        assert!(store.type_name(b"h2").is_some());
        assert!(store.type_name(b"l2").is_some());
        assert!(store.type_name(b"set2").is_some());
        assert!(store.type_name(b"z2").is_some());
    }

    #[test]
    fn dump_restore_errors_and_options() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("k"),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
        let Response::Value(Some(SenkoValue::Raw(payload))) =
            dump(&mut store, &[bs(b"k")]).unwrap()
        else {
            panic!()
        };
        let mut bad = payload.to_vec();
        bad[0] ^= 0xFF;
        assert!(restore(&mut store, &[bs(b"bad"), bs(b"0"), Frame::BulkString(&bad)]).is_err());

        assert_eq!(
            dump(&mut store, &[bs(b"missing")]).unwrap(),
            Response::Value(None)
        );
        assert!(
            restore(
                &mut store,
                &[bs(b"k"), bs(b"0"), Frame::BulkString(payload.as_ref())]
            )
            .is_err()
        );
        assert_eq!(
            restore(
                &mut store,
                &[
                    bs(b"k"),
                    bs(b"0"),
                    Frame::BulkString(payload.as_ref()),
                    bs(b"REPLACE")
                ]
            )
            .unwrap(),
            Response::Simple(b"OK")
        );
        assert_eq!(
            restore(
                &mut store,
                &[bs(b"ttl0"), bs(b"0"), Frame::BulkString(payload.as_ref())]
            )
            .unwrap(),
            Response::Simple(b"OK")
        );
        assert_eq!(store.ttl_ms(b"ttl0"), Some(-1));
        let future = (crate::store::current_unix_ms() + 5_000).to_string();
        assert_eq!(
            restore(
                &mut store,
                &[
                    bs(b"abs"),
                    Frame::BulkString(future.as_bytes()),
                    Frame::BulkString(payload.as_ref()),
                    bs(b"ABSTTL")
                ]
            )
            .unwrap(),
            Response::Simple(b"OK")
        );
        assert!(store.ttl_ms(b"abs").unwrap() > 0);
    }

    #[test]
    fn move_wait_and_migrate_behave() {
        let mut store = Store::default();
        assert_eq!(
            r#move(&mut store, &[bs(b"k"), bs(b"0")]).unwrap(),
            Response::Integer(0)
        );
        assert!(r#move(&mut store, &[bs(b"k"), bs(b"1")]).is_err());
        assert_eq!(
            wait(&mut store, &[bs(b"1"), bs(b"100")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            waitaof(&mut store, &[bs(b"1"), bs(b"1"), bs(b"100")]).unwrap(),
            Response::Array(Box::new(smallvec::smallvec![
                Response::Integer(0),
                Response::Integer(0)
            ]))
        );
        assert!(
            migrate(
                &mut store,
                &[
                    bs(b"host"),
                    bs(b"6379"),
                    bs(b"k"),
                    bs(b"0"),
                    bs(b"1000"),
                    bs(b"COPY")
                ]
            )
            .is_err()
        );
    }
}
