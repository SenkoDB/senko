use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult};
use senko_proto::Frame;

use crate::{
    Response,
    store::{Store, current_unix_ms},
};

const OK: &[u8] = b"OK";
const NONE: &[u8] = b"none";
const ERR_NO_KEY: &str = "ERR no such key";
const ERR_INVALID_DB_INDEX: &str = "ERR invalid DB index";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOutcome {
    pub count: u64,
    pub deleted_blocking_keys: Vec<CompactString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameOutcome {
    pub source_blocking_key: Option<CompactString>,
    pub overwritten_blocking_key: Option<CompactString>,
    pub destination_blocking_key: Option<CompactString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOutcome {
    pub overwritten_blocking_key: Option<CompactString>,
    pub destination_blocking_key: Option<CompactString>,
}

#[inline]
pub fn del(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'del' command",
        ));
    }
    let keys = collect_arg_bytes(args)?;
    Ok(Response::Integer(delete_keys(store, &keys).count as i64))
}

#[inline]
pub fn unlink(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'unlink' command",
        ));
    }
    let keys = collect_arg_bytes(args)?;
    // FUTURE: background deallocation via compio::spawn.
    Ok(Response::Integer(delete_keys(store, &keys).count as i64))
}

#[inline]
pub fn exists(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'exists' command",
        ));
    }
    let mut count = 0i64;
    for arg in args {
        if store.type_name(arg_bytes(arg)?).is_some() {
            count += 1;
        }
    }
    Ok(Response::Integer(count))
}

#[inline]
pub fn type_cmd(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'type' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    Ok(Response::Simple(store.type_name(key).unwrap_or(NONE)))
}

#[inline]
pub fn rename(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'rename' command",
        ));
    }
    let source = arg_bytes(&args[0])?;
    let destination = parse_key(arg_bytes(&args[1])?)?;
    rename_key(store, source, destination, true)?;
    Ok(Response::Simple(OK))
}

#[inline]
pub fn renamenx(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'renamenx' command",
        ));
    }
    let source = arg_bytes(&args[0])?;
    let destination = parse_key(arg_bytes(&args[1])?)?;
    Ok(Response::Integer(
        rename_nx_key(store, source, destination)? as i64,
    ))
}

#[inline]
pub fn copy(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'copy' command",
        ));
    }
    let source = arg_bytes(&args[0])?;
    let destination = parse_key(arg_bytes(&args[1])?)?;
    let mut replace = false;
    let mut index = 2usize;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if is_opt(token, b"REPLACE") {
            replace = true;
            index += 1;
            continue;
        }
        if is_opt(token, b"DB") {
            index += 1;
            if index >= args.len() {
                return Err(syntax_error());
            }
            let db = parse_db_index(arg_bytes(&args[index])?)?;
            if db != 0 {
                return Err(SenkoError::Protocol(ERR_INVALID_DB_INDEX));
            }
            index += 1;
            continue;
        }
        return Err(syntax_error());
    }
    let db = 0u64;
    Ok(Response::Integer(
        copy_key(store, source, destination, replace, db)?.is_some() as i64,
    ))
}

pub fn delete_keys(store: &mut Store, keys: &[&[u8]]) -> DeleteOutcome {
    let mut count = 0u64;
    let mut deleted_blocking_keys = Vec::new();
    for key in keys {
        let Some(value_type) = store.delete_with_type(key) else {
            continue;
        };
        count += 1;
        if is_blocking_type(value_type)
            && let Ok(key_owned) = CompactString::from_utf8(key)
        {
            deleted_blocking_keys.push(key_owned);
        }
    }
    DeleteOutcome {
        count,
        deleted_blocking_keys,
    }
}

pub fn rename_key(
    store: &mut Store,
    source: &[u8],
    destination: CompactString,
    overwrite_forced: bool,
) -> SenkoResult<RenameOutcome> {
    let source_type = store.type_name(source);
    let Some(source_type) = source_type else {
        return Err(SenkoError::Protocol(ERR_NO_KEY));
    };

    if source == destination.as_bytes() {
        return Ok(RenameOutcome {
            source_blocking_key: None,
            overwritten_blocking_key: None,
            destination_blocking_key: None,
        });
    }

    let overwritten_type = if overwrite_forced {
        store.type_name(destination.as_bytes())
    } else {
        None
    };
    let renamed = store.rename(source, destination.clone());
    if renamed.is_none() {
        return Err(SenkoError::Protocol(ERR_NO_KEY));
    }
    Ok(RenameOutcome {
        source_blocking_key: blocking_key(source, source_type),
        overwritten_blocking_key: overwritten_type
            .and_then(|ty| blocking_key(destination.as_bytes(), ty)),
        destination_blocking_key: blocking_key(destination.as_bytes(), source_type),
    })
}

pub fn rename_nx_key(
    store: &mut Store,
    source: &[u8],
    destination: CompactString,
) -> SenkoResult<bool> {
    let Some(_) = store.type_name(source) else {
        return Err(SenkoError::Protocol(ERR_NO_KEY));
    };
    if source == destination.as_bytes() {
        return Ok(false);
    }
    if store.type_name(destination.as_bytes()).is_some() {
        return Ok(false);
    }
    let renamed = store.rename(source, destination);
    Ok(renamed.is_some())
}

pub fn copy_key(
    store: &mut Store,
    source: &[u8],
    destination: CompactString,
    replace: bool,
    destination_db: u64,
) -> SenkoResult<Option<CopyOutcome>> {
    if destination_db != 0 {
        return Err(SenkoError::Protocol(ERR_INVALID_DB_INDEX));
    }
    if source == destination.as_bytes() {
        return Ok(None);
    }
    let Some(source_entry) = store.clone_entry(source) else {
        return Ok(None);
    };
    if !replace && store.type_name(destination.as_bytes()).is_some() {
        return Ok(None);
    }
    let now_ms = current_unix_ms();
    let new_expires_at = source_entry
        .expires_at
        .and_then(|deadline| deadline.checked_sub(now_ms))
        .map(|remaining| now_ms.saturating_add(remaining));
    let source_type = store
        .type_name(source)
        .expect("source still exists after clone");
    let overwritten = store.copy_from_entry(destination.clone(), &source_entry, new_expires_at);
    Ok(Some(CopyOutcome {
        overwritten_blocking_key: overwritten
            .and_then(|ty| blocking_key(destination.as_bytes(), ty)),
        destination_blocking_key: blocking_key(destination.as_bytes(), source_type),
    }))
}

fn collect_arg_bytes<'a>(args: &'a [Frame<'_>]) -> SenkoResult<Vec<&'a [u8]>> {
    args.iter().map(arg_bytes).collect()
}

fn blocking_key(key: &[u8], value_type: &'static [u8]) -> Option<CompactString> {
    if !is_blocking_type(value_type) {
        return None;
    }
    CompactString::from_utf8(key).ok()
}

fn is_blocking_type(value_type: &[u8]) -> bool {
    value_type == b"list" || value_type == b"zset" || value_type == b"stream"
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

fn parse_db_index(raw: &[u8]) -> SenkoResult<u64> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or(SenkoError::Protocol(ERR_INVALID_DB_INDEX))
}

fn is_opt(input: &[u8], expected_upper: &[u8]) -> bool {
    input.eq_ignore_ascii_case(expected_upper)
}

fn syntax_error() -> SenkoError {
    SenkoError::Protocol("syntax error")
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
    use std::{thread, time::Duration};

    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_core::{
        HashObject, QuickList, SenkoError, SenkoValue, SetObject, StreamObject, ZAddOptions,
        ZSetObject,
    };
    use senko_proto::Frame;

    use super::{copy, del, exists, rename, renamenx, type_cmd, unlink};
    use crate::{Response, Store};

    fn bs<'a>(value: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(value)
    }

    #[test]
    fn del_counts_existing_keys_only() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("a"),
            SenkoValue::from(Bytes::from_static(b"1")),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("b"),
            SenkoValue::from(Bytes::from_static(b"2")),
            Default::default(),
        );

        assert_eq!(
            del(&mut store, &[bs(b"a"), bs(b"missing"), bs(b"b")]).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(store.type_name(b"a"), None);
        assert_eq!(store.type_name(b"b"), None);
    }

    #[test]
    fn del_removes_all_collection_types() {
        let mut store = Store::default();
        let mut hash = HashObject::default();
        let _ = hash.set(
            CompactString::new("f"),
            SenkoValue::from(Bytes::from_static(b"v")),
            None,
        );
        let mut list = QuickList::default();
        list.push_back(b"v");
        let mut set = SetObject::default();
        let _ = set.add(b"v");
        let mut zset = ZSetObject::default();
        let _ = zset.add(1.0, CompactString::new("m"), ZAddOptions::default());
        let stream = StreamObject::new();
        let _ = store.set(
            CompactString::new("h"),
            SenkoValue::Hash(Box::new(hash)),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("l"),
            SenkoValue::List(Box::new(list)),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("s"),
            SenkoValue::Set(Box::new(set)),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("z"),
            SenkoValue::ZSet(Box::new(zset)),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("x"),
            SenkoValue::Stream(Box::new(stream)),
            Default::default(),
        );

        assert_eq!(
            del(
                &mut store,
                &[bs(b"h"), bs(b"l"), bs(b"s"), bs(b"z"), bs(b"x")]
            )
            .unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            exists(
                &mut store,
                &[bs(b"h"), bs(b"l"), bs(b"s"), bs(b"z"), bs(b"x")]
            )
            .unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn del_removes_timerwheel_entries() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("exp"),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
        store.set_expiry(b"exp", crate::store::current_unix_ms() + 200);
        assert_eq!(
            del(&mut store, &[bs(b"exp")]).unwrap(),
            Response::Integer(1)
        );
        thread::sleep(Duration::from_millis(250));
        let _ = store.advance_expiry_wheel(crate::store::current_unix_ms());
        assert_eq!(
            type_cmd(&mut store, &[bs(b"exp")]).unwrap(),
            Response::Simple(b"none")
        );
    }

    #[test]
    fn exists_counts_duplicates_and_expiry() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("foo"),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
        assert_eq!(
            exists(&mut store, &[bs(b"foo"), bs(b"foo")]).unwrap(),
            Response::Integer(2)
        );

        let _ = store.set(
            CompactString::new("gone"),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
        store.set_expiry(b"gone", crate::store::current_unix_ms() + 25);
        thread::sleep(Duration::from_millis(40));
        assert_eq!(
            exists(&mut store, &[bs(b"gone")]).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn type_returns_expected_simple_strings() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("str"),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
        let mut list = QuickList::default();
        list.push_back(b"v");
        let _ = store.set(
            CompactString::new("list"),
            SenkoValue::List(Box::new(list)),
            Default::default(),
        );
        let mut set = SetObject::default();
        let _ = set.add(b"v");
        let _ = store.set(
            CompactString::new("set"),
            SenkoValue::Set(Box::new(set)),
            Default::default(),
        );
        let mut zset = ZSetObject::default();
        let _ = zset.add(1.0, CompactString::new("m"), ZAddOptions::default());
        let _ = store.set(
            CompactString::new("zset"),
            SenkoValue::ZSet(Box::new(zset)),
            Default::default(),
        );
        let mut hash = HashObject::default();
        let _ = hash.set(
            CompactString::new("f"),
            SenkoValue::from(Bytes::from_static(b"v")),
            None,
        );
        let _ = store.set(
            CompactString::new("hash"),
            SenkoValue::Hash(Box::new(hash)),
            Default::default(),
        );
        let stream = StreamObject::new();
        let _ = store.set(
            CompactString::new("stream"),
            SenkoValue::Stream(Box::new(stream)),
            Default::default(),
        );

        assert_eq!(
            type_cmd(&mut store, &[bs(b"str")]).unwrap(),
            Response::Simple(b"string")
        );
        assert_eq!(
            type_cmd(&mut store, &[bs(b"list")]).unwrap(),
            Response::Simple(b"list")
        );
        assert_eq!(
            type_cmd(&mut store, &[bs(b"set")]).unwrap(),
            Response::Simple(b"set")
        );
        assert_eq!(
            type_cmd(&mut store, &[bs(b"zset")]).unwrap(),
            Response::Simple(b"zset")
        );
        assert_eq!(
            type_cmd(&mut store, &[bs(b"hash")]).unwrap(),
            Response::Simple(b"hash")
        );
        assert_eq!(
            type_cmd(&mut store, &[bs(b"stream")]).unwrap(),
            Response::Simple(b"stream")
        );
        assert_eq!(
            type_cmd(&mut store, &[bs(b"missing")]).unwrap(),
            Response::Simple(b"none")
        );
    }

    #[test]
    fn rename_moves_value_and_ttl() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("src"),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
        store.set_expiry(b"src", crate::store::current_unix_ms() + 5_000);

        assert_eq!(
            rename(&mut store, &[bs(b"src"), bs(b"dst")]).unwrap(),
            Response::Simple(b"OK")
        );
        assert_eq!(
            type_cmd(&mut store, &[bs(b"src")]).unwrap(),
            Response::Simple(b"none")
        );
        assert_eq!(store.ttl_ms(b"dst").unwrap() > 0, true);
    }

    #[test]
    fn rename_same_key_is_noop() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("same"),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
        assert_eq!(
            rename(&mut store, &[bs(b"same"), bs(b"same")]).unwrap(),
            Response::Simple(b"OK")
        );
        assert_eq!(
            type_cmd(&mut store, &[bs(b"same")]).unwrap(),
            Response::Simple(b"string")
        );
    }

    #[test]
    fn rename_missing_key_errors() {
        let mut store = Store::default();
        let err = rename(&mut store, &[bs(b"missing"), bs(b"dst")]).unwrap_err();
        assert!(matches!(err, SenkoError::Protocol(message) if message == super::ERR_NO_KEY));
    }

    #[test]
    fn renamenx_existing_destination_returns_zero() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("src"),
            SenkoValue::from(Bytes::from_static(b"v1")),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("dst"),
            SenkoValue::from(Bytes::from_static(b"v2")),
            Default::default(),
        );

        assert_eq!(
            renamenx(&mut store, &[bs(b"src"), bs(b"dst")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            type_cmd(&mut store, &[bs(b"src")]).unwrap(),
            Response::Simple(b"string")
        );
    }

    #[test]
    fn copy_creates_independent_value_and_preserves_relative_ttl() {
        let mut store = Store::default();
        let mut list = QuickList::default();
        list.push_back(b"a");
        let _ = store.set(
            CompactString::new("src"),
            SenkoValue::List(Box::new(list)),
            Default::default(),
        );
        store.set_expiry(b"src", crate::store::current_unix_ms() + 5_000);

        assert_eq!(
            copy(&mut store, &[bs(b"src"), bs(b"dst")]).unwrap(),
            Response::Integer(1)
        );
        store.get_list_mut(b"src").unwrap().push_back(b"b");
        let src_len = store.get_list(b"src").unwrap().len();
        let dst_len = store.get_list(b"dst").unwrap().len();
        assert_ne!(src_len, dst_len);
        let src_ttl = store.ttl_ms(b"src").unwrap();
        let dst_ttl = store.ttl_ms(b"dst").unwrap();
        assert!((src_ttl - dst_ttl).abs() < 100);
    }

    #[test]
    fn copy_replace_and_db_behaviors() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("src"),
            SenkoValue::from(Bytes::from_static(b"v1")),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("dst"),
            SenkoValue::from(Bytes::from_static(b"v2")),
            Default::default(),
        );

        assert_eq!(
            copy(&mut store, &[bs(b"src"), bs(b"dst")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            copy(&mut store, &[bs(b"src"), bs(b"dst"), bs(b"REPLACE")]).unwrap(),
            Response::Integer(1)
        );
        let err = copy(&mut store, &[bs(b"src"), bs(b"other"), bs(b"DB"), bs(b"1")]).unwrap_err();
        assert!(
            matches!(err, SenkoError::Protocol(message) if message == super::ERR_INVALID_DB_INDEX)
        );
    }

    #[test]
    fn unlink_is_alias_of_del() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::new("k"),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
        assert_eq!(
            unlink(&mut store, &[bs(b"k")]).unwrap(),
            Response::Integer(1)
        );
    }
}
