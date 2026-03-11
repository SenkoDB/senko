use std::time::Duration;

use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    commands::list::blocking::BlockingResponseKind,
    commands::zset::basic::{arg_bytes, parse_compact, wrong_type},
    commands::zset::pop::{
        ZPopDir, parse_direction, parse_usize, pop_entries, zmpop_response, zpop_block_response,
    },
    store::Store,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockingOp {
    ZPop { direction: ZPopDir },
    ZMPop { direction: ZPopDir, count: usize },
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

#[inline]
pub fn bzpopmin(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<BlockingCommandResult> {
    bzpop(store, args, ZPopDir::Min, "bzpopmin")
}

#[inline]
pub fn bzpopmax(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<BlockingCommandResult> {
    bzpop(store, args, ZPopDir::Max, "bzpopmax")
}

#[inline]
pub fn bzmpop(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<BlockingCommandResult> {
    if args.len() < 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'bzmpop' command",
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
        keys.push(parse_compact(arg_bytes(frame)?));
    }

    for key in &keys {
        match store.get(key.as_bytes()) {
            None => continue,
            Some(SenkoValue::ZSet(_)) => {
                return Ok(BlockingCommandResult::Immediate(zmpop_now(
                    store,
                    key.as_bytes(),
                    direction,
                    count,
                )?));
            }
            Some(other) => return Err(wrong_type(other)),
        }
    }

    Ok(BlockingCommandResult::Block(BlockSpec {
        keys,
        timeout,
        op: BlockingOp::ZMPop { direction, count },
        timeout_response: BlockingResponseKind::NullBulk,
    }))
}

pub fn zpop_now(store: &mut Store, key: &[u8], direction: ZPopDir) -> SenkoResult<Response> {
    let entries = pop_entries(store, key, direction, 1)?;
    Ok(zpop_block_response(key, entries))
}

pub fn zmpop_now(
    store: &mut Store,
    key: &[u8],
    direction: ZPopDir,
    count: usize,
) -> SenkoResult<Response> {
    let entries = pop_entries(store, key, direction, count)?;
    if entries.is_empty() && store.get(key).is_none() {
        return Ok(Response::Value(None));
    }
    Ok(zmpop_response(key, entries))
}

fn bzpop(
    store: &mut Store,
    args: &[Frame<'_>],
    direction: ZPopDir,
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
        keys.push(parse_compact(arg_bytes(frame)?));
    }

    for key in &keys {
        match store.get(key.as_bytes()) {
            None => continue,
            Some(SenkoValue::ZSet(_)) => {
                return Ok(BlockingCommandResult::Immediate(zpop_now(
                    store,
                    key.as_bytes(),
                    direction,
                )?));
            }
            Some(other) => return Err(wrong_type(other)),
        }
    }

    Ok(BlockingCommandResult::Block(BlockSpec {
        keys,
        timeout,
        op: BlockingOp::ZPop { direction },
        timeout_response: BlockingResponseKind::NullArray,
    }))
}

fn parse_timeout(raw: &[u8]) -> SenkoResult<Option<Duration>> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .ok_or(SenkoError::Protocol(
            "timeout is not a float or out of range",
        ))?;
    if value.is_nan() {
        return Err(SenkoError::Protocol(
            "timeout is not a float or out of range",
        ));
    }
    if value < 0.0 {
        return Err(SenkoError::Protocol("ERR timeout is negative"));
    }
    if value == 0.0 {
        return Ok(None);
    }
    let ms = (value * 1000.0).ceil() as u64;
    Ok(Some(Duration::from_millis(ms.max(1))))
}

#[cfg(test)]
mod tests {
    use senko_proto::Frame;

    use super::*;
    use crate::commands::zset::basic::zadd;

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    #[test]
    fn bzpopmin_fast_path_returns_key_member_score() {
        let mut store = Store::default();
        let _ = zadd(&mut store, &[bs(b"zs"), bs(b"1"), bs(b"a")]).unwrap();
        let result = bzpopmin(&mut store, &[bs(b"zs"), bs(b"1")]).unwrap();
        assert!(matches!(result, BlockingCommandResult::Immediate(_)));
    }

    #[test]
    fn bzmpop_blocks_when_all_keys_empty() {
        let mut store = Store::default();
        let result = bzmpop(
            &mut store,
            &[bs(b"1"), bs(b"2"), bs(b"a"), bs(b"b"), bs(b"MIN")],
        )
        .unwrap();
        assert!(matches!(result, BlockingCommandResult::Block(_)));
    }

    #[test]
    fn bzpopmin_negative_timeout_is_rejected() {
        let mut store = Store::default();
        let err = bzpopmin(&mut store, &[bs(b"zs"), bs(b"-1")]).unwrap_err();
        assert!(matches!(
            err,
            SenkoError::Protocol("ERR timeout is negative")
        ));
    }
}
