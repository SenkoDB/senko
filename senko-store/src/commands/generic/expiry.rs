use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult};
use senko_proto::Frame;

use crate::{
    Entry, Response,
    store::{Store, current_unix_ms},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiryCondition {
    Always,
    NX,
    XX,
    GT,
    LT,
}

pub fn apply_key_expiry(
    entry: &mut Entry,
    new_expires_at: u64,
    condition: ExpiryCondition,
    _now_ms: u64,
) -> bool {
    let allowed = match condition {
        ExpiryCondition::Always => true,
        ExpiryCondition::NX => entry.expires_at.is_none(),
        ExpiryCondition::XX => entry.expires_at.is_some(),
        ExpiryCondition::GT => entry
            .expires_at
            .is_some_and(|current| new_expires_at > current),
        ExpiryCondition::LT => entry
            .expires_at
            .is_some_and(|current| new_expires_at < current),
    };
    if !allowed {
        return false;
    }
    entry.expires_at = Some(new_expires_at);
    true
}

#[inline]
pub fn expire(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    let (key, condition, ttl_raw) = parse_expiry_args(args, "expire")?;
    let ttl = parse_non_negative_i64(ttl_raw, "expire")? as u64;
    if ttl == 0 {
        return expire_immediately(store, key, condition);
    }
    let now_ms = current_unix_ms();
    let new_expires_at = now_ms
        .checked_add(ttl.saturating_mul(1_000))
        .ok_or_else(|| invalid_expire_error("expire"))?;
    Ok(Response::Integer(
        set_key_expiry(store, key, new_expires_at, condition) as i64,
    ))
}

#[inline]
pub fn pexpire(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    let (key, condition, ttl_raw) = parse_expiry_args(args, "pexpire")?;
    let ttl = parse_non_negative_i64(ttl_raw, "pexpire")? as u64;
    if ttl == 0 {
        return expire_immediately(store, key, condition);
    }
    let now_ms = current_unix_ms();
    let new_expires_at = now_ms
        .checked_add(ttl)
        .ok_or_else(|| invalid_expire_error("pexpire"))?;
    Ok(Response::Integer(
        set_key_expiry(store, key, new_expires_at, condition) as i64,
    ))
}

#[inline]
pub fn expireat(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    let (key, condition, at_raw) = parse_expiry_args(args, "expireat")?;
    let new_expires_at = (parse_non_negative_i64(at_raw, "expireat")? as u64).saturating_mul(1_000);
    Ok(Response::Integer(
        set_key_expiry(store, key, new_expires_at, condition) as i64,
    ))
}

#[inline]
pub fn pexpireat(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    let (key, condition, at_raw) = parse_expiry_args(args, "pexpireat")?;
    let new_expires_at = parse_non_negative_i64(at_raw, "pexpireat")? as u64;
    Ok(Response::Integer(
        set_key_expiry(store, key, new_expires_at, condition) as i64,
    ))
}

#[inline]
pub fn ttl(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    ttl_like(store, args, true)
}

#[inline]
pub fn pttl(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    ttl_like(store, args, false)
}

#[inline]
pub fn expiretime(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    expiretime_like(store, args, true)
}

#[inline]
pub fn pexpiretime(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    expiretime_like(store, args, false)
}

#[inline]
pub fn persist(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(wrong_arity("persist"));
    }
    let key = arg_bytes(&args[0])?;
    let Some(entry) = store.clone_entry(key) else {
        return Ok(Response::Integer(0));
    };
    if entry.expires_at.is_none() {
        return Ok(Response::Integer(0));
    }
    store.remove_expiry(key);
    Ok(Response::Integer(1))
}

fn expire_immediately(
    store: &mut Store,
    key: &[u8],
    condition: ExpiryCondition,
) -> SenkoResult<Response> {
    let Some(entry) = store.clone_entry(key) else {
        return Ok(Response::Integer(0));
    };
    let mut candidate = entry;
    if !apply_key_expiry(
        &mut candidate,
        current_unix_ms(),
        condition,
        current_unix_ms(),
    ) {
        return Ok(Response::Integer(0));
    }
    let _ = store.delete(key);
    Ok(Response::Integer(1))
}

fn ttl_like(store: &mut Store, args: &[Frame<'_>], seconds: bool) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(wrong_arity(if seconds { "ttl" } else { "pttl" }));
    }
    let key = arg_bytes(&args[0])?;
    if store.type_name(key).is_none() {
        return Ok(Response::Integer(-2));
    }
    let ttl_ms = store.ttl_ms(key).unwrap_or(-2);
    Ok(Response::Integer(if ttl_ms == -1 || ttl_ms == -2 {
        ttl_ms
    } else if seconds {
        ttl_ms / 1_000
    } else {
        ttl_ms
    }))
}

fn expiretime_like(store: &mut Store, args: &[Frame<'_>], seconds: bool) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(wrong_arity(if seconds {
            "expiretime"
        } else {
            "pexpiretime"
        }));
    }
    let key = arg_bytes(&args[0])?;
    let Some(expires_at_ms) = store.expiretime_ms(key) else {
        return Ok(Response::Integer(-2));
    };
    Ok(Response::Integer(if expires_at_ms == -1 {
        -1
    } else if seconds {
        expires_at_ms / 1_000
    } else {
        expires_at_ms
    }))
}

fn set_key_expiry(
    store: &mut Store,
    key: &[u8],
    new_expires_at: u64,
    condition: ExpiryCondition,
) -> bool {
    let now_ms = current_unix_ms();
    let mut should_set = false;
    if let Some(entry) = store.get_mut(key) {
        should_set = apply_key_expiry(entry, new_expires_at, condition, now_ms);
    }
    if !should_set {
        return false;
    }
    store.set_expiry(key, new_expires_at);
    true
}

fn parse_expiry_args<'a>(
    args: &'a [Frame<'_>],
    command: &'static str,
) -> SenkoResult<(&'a [u8], ExpiryCondition, &'a [u8])> {
    if args.len() < 2 || args.len() > 3 {
        return Err(wrong_arity(command));
    }
    let key = arg_bytes(&args[0])?;
    let ttl_raw = arg_bytes(&args[1])?;
    let condition = if args.len() == 3 {
        parse_condition(arg_bytes(&args[2])?)?
    } else {
        ExpiryCondition::Always
    };
    Ok((key, condition, ttl_raw))
}

fn parse_condition(raw: &[u8]) -> SenkoResult<ExpiryCondition> {
    if raw.eq_ignore_ascii_case(b"NX") {
        return Ok(ExpiryCondition::NX);
    }
    if raw.eq_ignore_ascii_case(b"XX") {
        return Ok(ExpiryCondition::XX);
    }
    if raw.eq_ignore_ascii_case(b"GT") {
        return Ok(ExpiryCondition::GT);
    }
    if raw.eq_ignore_ascii_case(b"LT") {
        return Ok(ExpiryCondition::LT);
    }
    Err(SenkoError::Protocol("syntax error"))
}

fn parse_non_negative_i64(raw: &[u8], command: &'static str) -> SenkoResult<i64> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or_else(|| invalid_expire_error(command))?;
    if value < 0 {
        return Err(invalid_expire_error(command));
    }
    Ok(value)
}

fn invalid_expire_error(command: &'static str) -> SenkoError {
    SenkoError::ProtocolMessage(CompactString::new(format!(
        "ERR invalid expire time in '{command}' command"
    )))
}

fn wrong_arity(command: &'static str) -> SenkoError {
    SenkoError::ProtocolMessage(CompactString::new(format!(
        "wrong number of arguments for '{command}' command"
    )))
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
    use std::{thread, time::Duration};

    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_core::{SenkoError, SenkoValue};
    use senko_proto::Frame;

    use super::{
        ExpiryCondition, apply_key_expiry, expire, expireat, expiretime, persist, pexpire,
        pexpireat, pexpiretime, pttl, ttl,
    };
    use crate::{Entry, Response, Store, store::current_unix_ms};

    fn bs<'a>(value: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(value)
    }

    fn make_key(store: &mut Store, key: &str) {
        let _ = store.set(
            CompactString::new(key),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
    }

    #[test]
    fn apply_key_expiry_respects_conditions() {
        let mut entry = Entry {
            value: SenkoValue::from(Bytes::from_static(b"v")),
            expires_at: None,
            lru_clock: std::cell::Cell::new(0),
        };
        assert!(apply_key_expiry(
            &mut entry,
            100,
            ExpiryCondition::Always,
            0
        ));
        assert!(!apply_key_expiry(&mut entry, 50, ExpiryCondition::NX, 0));
        assert!(apply_key_expiry(&mut entry, 150, ExpiryCondition::XX, 0));
        assert!(apply_key_expiry(&mut entry, 200, ExpiryCondition::GT, 0));
        assert!(apply_key_expiry(&mut entry, 100, ExpiryCondition::LT, 0));
    }

    #[test]
    fn expire_zero_deletes_on_next_access() {
        let mut store = Store::default();
        make_key(&mut store, "k");
        assert_eq!(
            expire(&mut store, &[bs(b"k"), bs(b"0")]).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(ttl(&mut store, &[bs(b"k")]).unwrap(), Response::Integer(-2));
    }

    #[test]
    fn expire_negative_errors() {
        let mut store = Store::default();
        make_key(&mut store, "k");
        let err = expire(&mut store, &[bs(b"k"), bs(b"-1")]).unwrap_err();
        assert!(
            matches!(err, SenkoError::ProtocolMessage(message) if message.as_str() == "ERR invalid expire time in 'expire' command")
        );
    }

    #[test]
    fn expire_nx_xx_gt_lt_behave_correctly() {
        let mut store = Store::default();
        make_key(&mut store, "k");
        assert_eq!(
            expire(&mut store, &[bs(b"k"), bs(b"10"), bs(b"NX")]).unwrap(),
            Response::Integer(1)
        );
        make_key(&mut store, "fresh");
        assert_eq!(
            expire(&mut store, &[bs(b"fresh"), bs(b"10"), bs(b"XX")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            expire(&mut store, &[bs(b"k"), bs(b"20"), bs(b"NX")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            expire(&mut store, &[bs(b"k"), bs(b"30"), bs(b"XX")]).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            expire(&mut store, &[bs(b"k"), bs(b"1"), bs(b"GT")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            expire(&mut store, &[bs(b"k"), bs(b"60"), bs(b"GT")]).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            expire(&mut store, &[bs(b"k"), bs(b"120"), bs(b"LT")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            expire(&mut store, &[bs(b"k"), bs(b"1"), bs(b"LT")]).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn pexpire_precision_and_pttl() {
        let mut store = Store::default();
        make_key(&mut store, "k");
        assert_eq!(
            pexpire(&mut store, &[bs(b"k"), bs(b"500")]).unwrap(),
            Response::Integer(1)
        );
        let Response::Integer(pttl_value) = pttl(&mut store, &[bs(b"k")]).unwrap() else {
            panic!()
        };
        assert!(pttl_value > 0 && pttl_value <= 500);
    }

    #[test]
    fn expireat_past_timestamp_expires_immediately_on_access() {
        let mut store = Store::default();
        make_key(&mut store, "k");
        let past = (current_unix_ms() / 1_000).saturating_sub(1).to_string();
        assert_eq!(
            expireat(&mut store, &[bs(b"k"), bs(past.as_bytes())]).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(ttl(&mut store, &[bs(b"k")]).unwrap(), Response::Integer(-2));
    }

    #[test]
    fn ttl_and_pttl_missing_and_persistent() {
        let mut store = Store::default();
        make_key(&mut store, "keep");
        assert_eq!(
            ttl(&mut store, &[bs(b"keep")]).unwrap(),
            Response::Integer(-1)
        );
        assert_eq!(
            ttl(&mut store, &[bs(b"missing")]).unwrap(),
            Response::Integer(-2)
        );
    }

    #[test]
    fn expiretime_and_pexpiretime_round_trip() {
        let mut store = Store::default();
        make_key(&mut store, "persisted");
        assert_eq!(
            pexpiretime(&mut store, &[bs(b"missing")]).unwrap(),
            Response::Integer(-2)
        );
        assert_eq!(
            expiretime(&mut store, &[bs(b"persisted")]).unwrap(),
            Response::Integer(-1)
        );
        make_key(&mut store, "k");
        let at_ms = current_unix_ms() + 5_000;
        let at_ms_raw = at_ms.to_string();
        assert_eq!(
            pexpireat(&mut store, &[bs(b"k"), bs(at_ms_raw.as_bytes())]).unwrap(),
            Response::Integer(1)
        );
        let Response::Integer(returned_ms) = pexpiretime(&mut store, &[bs(b"k")]).unwrap() else {
            panic!()
        };
        assert_eq!(returned_ms, at_ms as i64);
        let at_s = (current_unix_ms() / 1_000) + 5;
        let at_s_raw = at_s.to_string();
        assert_eq!(
            expireat(&mut store, &[bs(b"k"), bs(at_s_raw.as_bytes())]).unwrap(),
            Response::Integer(1)
        );
        let Response::Integer(returned_s) = expiretime(&mut store, &[bs(b"k")]).unwrap() else {
            panic!()
        };
        assert_eq!(returned_s, at_s as i64);
    }

    #[test]
    fn persist_removes_ttl() {
        let mut store = Store::default();
        make_key(&mut store, "k");
        let _ = pexpire(&mut store, &[bs(b"k"), bs(b"500")]).unwrap();
        assert_eq!(
            persist(&mut store, &[bs(b"k")]).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(ttl(&mut store, &[bs(b"k")]).unwrap(), Response::Integer(-1));
        assert_eq!(
            persist(&mut store, &[bs(b"k")]).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn timer_wheel_expiry_and_persist_behave() {
        let mut store = Store::default();
        make_key(&mut store, "k1");
        let _ = pexpire(&mut store, &[bs(b"k1"), bs(b"200")]).unwrap();
        thread::sleep(Duration::from_millis(300));
        let _ = store.advance_expiry_wheel(current_unix_ms());
        assert_eq!(
            ttl(&mut store, &[bs(b"k1")]).unwrap(),
            Response::Integer(-2)
        );

        make_key(&mut store, "k2");
        let _ = pexpire(&mut store, &[bs(b"k2"), bs(b"200")]).unwrap();
        let _ = persist(&mut store, &[bs(b"k2")]).unwrap();
        thread::sleep(Duration::from_millis(300));
        let _ = store.advance_expiry_wheel(current_unix_ms());
        assert_eq!(
            ttl(&mut store, &[bs(b"k2")]).unwrap(),
            Response::Integer(-1)
        );
    }

    #[test]
    fn timer_wheel_overflow_tracks_long_ttl() {
        let mut store = Store::default();
        make_key(&mut store, "k");
        assert_eq!(
            pexpire(&mut store, &[bs(b"k"), bs(b"60000")]).unwrap(),
            Response::Integer(1)
        );
        let deadline = store.expiretime_ms(b"k").unwrap() as u64;
        assert!(store.expiry_overflow_contains_deadline(deadline));
    }
}
