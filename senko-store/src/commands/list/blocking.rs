use std::time::Duration;

use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::{SmallVec, smallvec};

use crate::{commands::Response, store::Store};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockingOp {
    Pop {
        direction: Direction,
    },
    Move {
        dest: CompactString,
        src_dir: Direction,
        dst_dir: Direction,
    },
    MoveDeprecated {
        dest: CompactString,
    },
    MPop {
        direction: Direction,
        count: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockingResponseKind {
    NullArray,
    NullBulk,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockSpec {
    pub keys: SmallVec<[CompactString; 4]>,
    pub timeout: Option<Duration>,
    pub op: BlockingOp,
    pub timeout_response: BlockingResponseKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BlockingCommandResult {
    Immediate(Response),
    Block(BlockSpec),
}

pub fn blpop(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<BlockingCommandResult> {
    bpop(store, args, Direction::Left, "blpop")
}

pub fn brpop(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<BlockingCommandResult> {
    bpop(store, args, Direction::Right, "brpop")
}

pub fn blmove(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<BlockingCommandResult> {
    if args.len() != 5 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'blmove' command",
        ));
    }
    let source = arg_bytes(&args[0])?;
    let destination = parse_key(arg_bytes(&args[1])?)?;
    let src_dir = parse_direction(arg_bytes(&args[2])?)?;
    let dst_dir = parse_direction(arg_bytes(&args[3])?)?;
    let timeout = parse_timeout(arg_bytes(&args[4])?)?;

    match store.get(source) {
        None => {}
        Some(SenkoValue::List(list)) if !list.is_empty() => {
            let response = move_now(store, source, destination.as_bytes(), src_dir, dst_dir)?;
            return Ok(BlockingCommandResult::Immediate(response));
        }
        Some(SenkoValue::List(_)) => {}
        Some(other) => return Err(wrong_type(other)),
    }
    if let Some(value) = store.get(destination.as_bytes())
        && !matches!(value, SenkoValue::List(_))
    {
        return Err(wrong_type(value));
    }

    Ok(BlockingCommandResult::Block(BlockSpec {
        keys: smallvec![parse_key(source)?],
        timeout,
        op: BlockingOp::Move {
            dest: destination,
            src_dir,
            dst_dir,
        },
        timeout_response: BlockingResponseKind::NullBulk,
    }))
}

pub fn brpoplpush(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<BlockingCommandResult> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'brpoplpush' command",
        ));
    }
    let source = arg_bytes(&args[0])?;
    let destination = parse_key(arg_bytes(&args[1])?)?;
    let timeout = parse_timeout(arg_bytes(&args[2])?)?;

    match store.get(source) {
        None => {}
        Some(SenkoValue::List(list)) if !list.is_empty() => {
            let response = move_now(
                store,
                source,
                destination.as_bytes(),
                Direction::Right,
                Direction::Left,
            )?;
            return Ok(BlockingCommandResult::Immediate(response));
        }
        Some(SenkoValue::List(_)) => {}
        Some(other) => return Err(wrong_type(other)),
    }
    if let Some(value) = store.get(destination.as_bytes())
        && !matches!(value, SenkoValue::List(_))
    {
        return Err(wrong_type(value));
    }

    Ok(BlockingCommandResult::Block(BlockSpec {
        keys: smallvec![parse_key(source)?],
        timeout,
        op: BlockingOp::MoveDeprecated { dest: destination },
        timeout_response: BlockingResponseKind::NullBulk,
    }))
}

pub fn blmpop(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<BlockingCommandResult> {
    if args.len() < 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'blmpop' command",
        ));
    }

    let timeout = parse_timeout(arg_bytes(&args[0])?)?;
    let numkeys = parse_usize(arg_bytes(&args[1])?)?;
    if numkeys == 0 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR numkeys should be greater than 0",
        )));
    }
    if args.len() < 2 + numkeys + 1 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR numkeys does not match number of keys",
        )));
    }
    let side_index = 2 + numkeys;
    let direction = parse_direction(arg_bytes(&args[side_index])?)?;
    let mut count = 1usize;
    let mut option_index = side_index + 1;
    if option_index < args.len() {
        let token = arg_bytes(&args[option_index])?;
        option_index += 1;
        if !token.eq_ignore_ascii_case(b"COUNT") || option_index >= args.len() {
            return Err(SenkoError::Protocol("syntax error"));
        }
        count = parse_usize(arg_bytes(&args[option_index])?)?;
        option_index += 1;
    }
    if option_index != args.len() {
        return Err(SenkoError::Protocol("syntax error"));
    }

    let mut keys = SmallVec::<[CompactString; 4]>::with_capacity(numkeys);
    for frame in &args[2..2 + numkeys] {
        keys.push(parse_key(arg_bytes(frame)?)?);
    }

    for key in &keys {
        match store.get(key.as_bytes()) {
            None => continue,
            Some(SenkoValue::List(list)) if !list.is_empty() => {
                let response = mpop_now(store, key.as_bytes(), direction, count)?;
                return Ok(BlockingCommandResult::Immediate(response));
            }
            Some(SenkoValue::List(_)) => {}
            Some(other) => return Err(wrong_type(other)),
        }
    }

    Ok(BlockingCommandResult::Block(BlockSpec {
        keys,
        timeout,
        op: BlockingOp::MPop { direction, count },
        timeout_response: BlockingResponseKind::NullArray,
    }))
}

fn bpop(
    store: &mut Store,
    args: &[Frame<'_>],
    direction: Direction,
    command: &'static str,
) -> SenkoResult<BlockingCommandResult> {
    if args.len() < 2 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(format!(
            "wrong number of arguments for '{command}' command"
        ))));
    }
    let timeout = parse_timeout(arg_bytes(args.last().expect("timeout exists"))?)?;
    let key_frames = &args[..args.len() - 1];
    let mut keys = SmallVec::<[CompactString; 4]>::with_capacity(key_frames.len());
    for frame in key_frames {
        keys.push(parse_key(arg_bytes(frame)?)?);
    }

    for key in &keys {
        match store.get(key.as_bytes()) {
            None => continue,
            Some(SenkoValue::List(list)) if !list.is_empty() => {
                let response = pop_now(store, key.as_bytes(), direction)?;
                return Ok(BlockingCommandResult::Immediate(response));
            }
            Some(SenkoValue::List(_)) => {}
            Some(other) => return Err(wrong_type(other)),
        }
    }

    Ok(BlockingCommandResult::Block(BlockSpec {
        keys,
        timeout,
        op: BlockingOp::Pop { direction },
        timeout_response: BlockingResponseKind::NullArray,
    }))
}

pub fn pop_now(store: &mut Store, key: &[u8], direction: Direction) -> SenkoResult<Response> {
    let popped = {
        let list = store
            .get_list_mut(key)
            .ok_or_else(|| SenkoError::Storage("missing list for blocked pop"))?;
        match direction {
            Direction::Left => list.pop_front(),
            Direction::Right => list.pop_back(),
        }
    };
    store.remove_list_if_empty(key);
    let Some(value) = popped else {
        return Ok(Response::NullArray);
    };
    Ok(Response::Array(Box::new(smallvec![
        Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(key)))),
        Response::Value(Some(SenkoValue::Raw(Bytes::from(value)))),
    ])))
}

pub fn mpop_now(
    store: &mut Store,
    key: &[u8],
    direction: Direction,
    count: usize,
) -> SenkoResult<Response> {
    let mut values = SmallVec::<[Response; 16]>::new();
    if count > 0 {
        let list = store
            .get_list_mut(key)
            .ok_or_else(|| SenkoError::Storage("missing list for blocked mpop"))?;
        for _ in 0..count {
            let popped = match direction {
                Direction::Left => list.pop_front(),
                Direction::Right => list.pop_back(),
            };
            let Some(value) = popped else {
                break;
            };
            values.push(Response::Value(Some(SenkoValue::Raw(Bytes::from(value)))));
        }
    }
    store.remove_list_if_empty(key);
    Ok(Response::Array(Box::new(smallvec![
        Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(key)))),
        Response::Array(Box::new(values)),
    ])))
}

pub fn move_now(
    store: &mut Store,
    source: &[u8],
    destination: &[u8],
    src_dir: Direction,
    dst_dir: Direction,
) -> SenkoResult<Response> {
    // FUTURE: cross-shard LMOVE is not implemented in phase 1.
    if source == destination {
        let list = store
            .get_list_mut(source)
            .ok_or_else(|| SenkoError::Storage("missing list for blocked move"))?;
        let moved = match src_dir {
            Direction::Left => list.pop_front(),
            Direction::Right => list.pop_back(),
        };
        if let Some(ref value) = moved {
            match dst_dir {
                Direction::Left => list.push_front(value),
                Direction::Right => list.push_back(value),
            }
        }
        return Ok(Response::Value(
            moved.map(|value| SenkoValue::Raw(Bytes::from(value))),
        ));
    }

    let moved = {
        let list = store
            .get_list_mut(source)
            .ok_or_else(|| SenkoError::Storage("missing list for blocked move"))?;
        match src_dir {
            Direction::Left => list.pop_front(),
            Direction::Right => list.pop_back(),
        }
    };
    let Some(value) = moved else {
        store.remove_list_if_empty(source);
        return Ok(Response::Value(None));
    };
    let destination_key = parse_key(destination)?;
    let list = store.get_or_create_list(destination_key);
    match dst_dir {
        Direction::Left => list.push_front(&value),
        Direction::Right => list.push_back(&value),
    }
    store.remove_list_if_empty(source);
    Ok(Response::Value(Some(SenkoValue::Raw(Bytes::from(value)))))
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

fn parse_direction(raw: &[u8]) -> SenkoResult<Direction> {
    if raw.eq_ignore_ascii_case(b"LEFT") {
        Ok(Direction::Left)
    } else if raw.eq_ignore_ascii_case(b"RIGHT") {
        Ok(Direction::Right)
    } else {
        Err(SenkoError::Protocol("syntax error"))
    }
}

fn parse_timeout(raw: &[u8]) -> SenkoResult<Option<Duration>> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| *value >= 0.0)
        .ok_or(SenkoError::Protocol(
            "timeout is not a float or out of range",
        ))?;
    if value == 0.0 {
        return Ok(None);
    }
    let ms = (value * 1000.0).ceil() as u64;
    Ok(Some(Duration::from_millis(ms.max(1))))
}

fn parse_usize(raw: &[u8]) -> SenkoResult<usize> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or(SenkoError::Protocol(
            "value is not an integer or out of range",
        ))
}

fn wrong_type(value: &SenkoValue) -> SenkoError {
    let actual = match value {
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
    };
    SenkoError::WrongType {
        expected: "list",
        actual,
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
