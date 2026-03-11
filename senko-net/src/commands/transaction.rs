use std::{cell::RefCell, rc::Rc};

use bytes::{Bytes, BytesMut};
use compact_str::CompactString;
use senko_core::SenkoError;
use senko_proto::{Frame, RespSerializer};
use senko_store::Store;

use crate::{
    commands::connection::basic as connection_basic,
    dispatch,
    transaction::{QueuedCommand, TxState, WatchRegistry, WatchState},
};

pub enum TransactionCommandResult {
    NotHandled,
    Respond(Vec<u8>),
    Exec { queue: Vec<QueuedCommand> },
}

pub fn handle_transaction_command(
    conn_id: u64,
    command: &[u8],
    frames: &[Frame<'_>],
    store: &Rc<RefCell<Store>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    tx_state: &mut TxState,
    watch_state: &Rc<RefCell<WatchState>>,
) -> Result<TransactionCommandResult, Vec<u8>> {
    let args = &frames[1..];

    if eq_ascii(command, b"MULTI") {
        if !args.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'multi' command",
            ));
        }
        if matches!(tx_state, TxState::Multi { .. }) {
            return Err(error_message("ERR MULTI calls can not be nested"));
        }
        *tx_state = TxState::Multi {
            queue: Vec::new(),
            error: false,
        };
        return Ok(TransactionCommandResult::Respond(simple_string(b"OK")));
    }

    if eq_ascii(command, b"DISCARD") {
        if !args.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'discard' command",
            ));
        }
        if !matches!(tx_state, TxState::Multi { .. }) {
            return Err(error_message("ERR DISCARD without MULTI"));
        }
        *tx_state = TxState::None;
        clear_watch_state(conn_id, watch_registry, watch_state);
        return Ok(TransactionCommandResult::Respond(simple_string(b"OK")));
    }

    if eq_ascii(command, b"UNWATCH") {
        if !args.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'unwatch' command",
            ));
        }
        if matches!(tx_state, TxState::Multi { .. }) {
            queue_local_response(
                tx_state,
                CompactString::const_new("UNWATCH"),
                owned_command_frames(frames)?,
                None,
            );
            return Ok(TransactionCommandResult::Respond(simple_string(b"QUEUED")));
        }
        clear_watch_state(conn_id, watch_registry, watch_state);
        return Ok(TransactionCommandResult::Respond(simple_string(b"OK")));
    }

    if eq_ascii(command, b"WATCH") {
        if args.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'watch' command",
            ));
        }
        if matches!(tx_state, TxState::Multi { .. }) {
            queue_local_response(
                tx_state,
                CompactString::const_new("WATCH"),
                Vec::new(),
                Some(error_message("ERR WATCH inside MULTI is not allowed")),
            );
            return Ok(TransactionCommandResult::Respond(simple_string(b"QUEUED")));
        }
        let mut watch_state_ref = watch_state.borrow_mut();
        for arg in args {
            let key = parse_compact_key(arg)?;
            let version = store.borrow().key_version(key.as_bytes());
            if let Some(existing) = watch_state_ref
                .watched_keys
                .iter_mut()
                .find(|(watched_key, _)| *watched_key == key)
            {
                existing.1 = version;
            } else {
                watch_state_ref.watched_keys.push((key.clone(), version));
            }
            watch_registry.borrow_mut().watch(conn_id, key, version);
        }
        return Ok(TransactionCommandResult::Respond(simple_string(b"OK")));
    }

    if eq_ascii(command, b"EXEC") {
        if !args.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'exec' command",
            ));
        }
        let queued = match std::mem::replace(tx_state, TxState::None) {
            TxState::None => return Err(error_message("ERR EXEC without MULTI")),
            TxState::Multi { queue, error } => {
                if watch_state.borrow().dirty {
                    clear_watch_state(conn_id, watch_registry, watch_state);
                    return Ok(TransactionCommandResult::Respond(null_array()));
                }
                if error {
                    clear_watch_state(conn_id, watch_registry, watch_state);
                    return Err(error_message(
                        "EXECABORT Transaction discarded because of previous errors.",
                    ));
                }
                queue
            }
        };
        return Ok(TransactionCommandResult::Exec { queue: queued });
    }

    Ok(TransactionCommandResult::NotHandled)
}

pub fn queue_transaction_command(
    command: &[u8],
    frames: &[Frame<'_>],
    tx_state: &mut TxState,
) -> Result<Vec<u8>, Vec<u8>> {
    let owned = owned_command_frames(frames)?;
    let validation = validate_queued_command(command, frames);
    let name = CompactString::from_utf8(command).unwrap_or_else(|_| CompactString::const_new(""));
    let queued = QueuedCommand {
        name,
        frames: owned,
        response_override: validation.clone().err(),
    };
    let TxState::Multi { queue, error } = tx_state else {
        return Err(error_message("ERR EXEC without MULTI"));
    };
    queue.push(queued);
    if let Err(response) = validation {
        *error = true;
        return Err(response);
    }
    Ok(simple_string(b"QUEUED"))
}

pub fn should_execute_immediately_in_multi(command: &[u8]) -> bool {
    eq_ascii(command, b"EXEC")
        || eq_ascii(command, b"DISCARD")
        || eq_ascii(command, b"MULTI")
        || eq_ascii(command, b"QUIT")
        || eq_ascii(command, b"RESET")
        || eq_ascii(command, b"HELLO")
        || eq_ascii(command, b"AUTH")
        || eq_ascii(command, b"CLIENT")
        || eq_ascii(command, b"ACL")
        || eq_ascii(command, b"CLUSTER")
        || eq_ascii(command, b"WATCH")
        || eq_ascii(command, b"UNWATCH")
}

pub fn clear_watch_state(
    conn_id: u64,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    watch_state: &Rc<RefCell<WatchState>>,
) {
    watch_registry.borrow_mut().unwatch(conn_id);
    let mut state = watch_state.borrow_mut();
    state.watched_keys.clear();
    state.dirty = false;
}

pub fn queued_command_response(
    conn_id: u64,
    queued_command: &QueuedCommand,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    watch_state: &Rc<RefCell<WatchState>>,
) -> Option<Vec<u8>> {
    if let Some(response) = &queued_command.response_override {
        return Some(response.clone());
    }
    if queued_command.name.eq_ignore_ascii_case("UNWATCH") {
        clear_watch_state(conn_id, watch_registry, watch_state);
        return Some(simple_string(b"OK"));
    }
    None
}

pub fn queued_frames_as_refs(frames: &[Bytes]) -> Vec<Frame<'_>> {
    frames
        .iter()
        .map(|frame| Frame::BulkString(frame.as_ref()))
        .collect()
}

pub fn serialize_exec_array(frames: &[Vec<u8>]) -> Vec<u8> {
    let mut out = BytesMut::new();
    RespSerializer::write_array_header(&mut out, frames.len());
    for frame in frames {
        out.extend_from_slice(frame);
    }
    out.to_vec()
}

fn queue_local_response(
    tx_state: &mut TxState,
    name: CompactString,
    frames: Vec<Bytes>,
    response_override: Option<Vec<u8>>,
) {
    let TxState::Multi { queue, .. } = tx_state else {
        return;
    };
    queue.push(QueuedCommand {
        name,
        frames,
        response_override,
    });
}

fn owned_command_frames(frames: &[Frame<'_>]) -> Result<Vec<Bytes>, Vec<u8>> {
    frames
        .iter()
        .map(|frame| {
            frame_bytes(frame)
                .map(Bytes::copy_from_slice)
                .map_err(|error| error_bytes(&error))
        })
        .collect()
}

fn validate_queued_command(command: &[u8], frames: &[Frame<'_>]) -> Result<(), Vec<u8>> {
    if let Some(result) = connection_basic::validate_queued(command, frames) {
        return result;
    }
    if is_blocking_command(command) {
        return Ok(());
    }
    let mut scratch = Store::default();
    match dispatch::dispatch(&mut scratch, command, &frames[1..]) {
        Ok(_) => Ok(()),
        Err(error) => {
            let message = error_message_text(&error);
            if message.contains("wrong number of arguments")
                || message.contains("syntax error")
                || message.contains("unknown command")
            {
                Err(error_message(&message))
            } else {
                Ok(())
            }
        }
    }
}

fn parse_compact_key(frame: &Frame<'_>) -> Result<CompactString, Vec<u8>> {
    let bytes = frame_bytes(frame).map_err(|error| error_bytes(&error))?;
    CompactString::from_utf8(bytes)
        .map_err(|_| error_bytes(&SenkoError::Protocol("invalid UTF-8 key")))
}

fn frame_bytes<'a>(frame: &'a Frame<'_>) -> Result<&'a [u8], SenkoError> {
    match frame {
        Frame::BulkString(bytes) | Frame::SimpleString(bytes) | Frame::BlobError(bytes) => {
            Ok(bytes)
        }
        Frame::VerbatimString { data, .. } => Ok(data),
        _ => Err(SenkoError::Protocol("command arguments must be strings")),
    }
}

fn error_bytes(error: &SenkoError) -> Vec<u8> {
    error_message(&error_message_text(error))
}

fn error_message_text(error: &SenkoError) -> String {
    let text = match error {
        SenkoError::Protocol(message) => (*message).to_owned(),
        SenkoError::ProtocolMessage(message) => message.to_string(),
        SenkoError::WrongType { .. } => {
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()
        }
        _ => error.to_string(),
    };
    if text.starts_with("ERR ")
        || text.starts_with("WRONGTYPE")
        || text.starts_with("NOPROTO")
        || text.starts_with("NOAUTH")
        || text.starts_with("WRONGPASS")
    {
        text
    } else {
        format!("ERR {text}")
    }
}

fn simple_string(value: &[u8]) -> Vec<u8> {
    let mut out = BytesMut::new();
    RespSerializer::write_simple_string(&mut out, value);
    out.to_vec()
}

fn error_message(message: &str) -> Vec<u8> {
    let mut out = BytesMut::new();
    RespSerializer::write_error(&mut out, message.as_bytes());
    out.to_vec()
}

fn null_array() -> Vec<u8> {
    b"*-1\r\n".to_vec()
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn is_blocking_command(command: &[u8]) -> bool {
    eq_ascii(command, b"BLPOP")
        || eq_ascii(command, b"BRPOP")
        || eq_ascii(command, b"BLMOVE")
        || eq_ascii(command, b"BRPOPLPUSH")
        || eq_ascii(command, b"BLMPOP")
        || eq_ascii(command, b"BZPOPMIN")
        || eq_ascii(command, b"BZPOPMAX")
        || eq_ascii(command, b"BZMPOP")
        || eq_ascii(command, b"XREAD")
        || eq_ascii(command, b"XREADGROUP")
}

#[cfg(test)]
mod tests {
    use super::{
        TransactionCommandResult, clear_watch_state, handle_transaction_command,
        queue_transaction_command, queued_command_response, should_execute_immediately_in_multi,
    };
    use crate::transaction::{ConnectionMap, TxState, WatchRegistry, WatchState};
    use compact_str::CompactString;
    use senko_proto::Frame;
    use senko_store::Store;
    use std::{cell::RefCell, rc::Rc};

    fn bs(bytes: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(bytes)
    }

    #[allow(clippy::type_complexity)]
    fn setup() -> (
        Rc<RefCell<Store>>,
        Rc<RefCell<WatchRegistry>>,
        Rc<RefCell<WatchState>>,
    ) {
        (
            Rc::new(RefCell::new(Store::default())),
            Rc::new(RefCell::new(WatchRegistry::default())),
            Rc::new(RefCell::new(WatchState::default())),
        )
    }

    #[test]
    fn multi_enters_transaction_mode() {
        let (store, registry, watch_state) = setup();
        let mut tx_state = TxState::None;

        let result = handle_transaction_command(
            1,
            b"MULTI",
            &[bs(b"MULTI")],
            &store,
            &registry,
            &mut tx_state,
            &watch_state,
        )
        .unwrap();

        assert!(matches!(result, TransactionCommandResult::Respond(_)));
        assert!(matches!(tx_state, TxState::Multi { .. }));
    }

    #[test]
    fn watch_outside_multi_registers_keys() {
        let (store, registry, watch_state) = setup();
        let mut tx_state = TxState::None;

        let result = handle_transaction_command(
            1,
            b"WATCH",
            &[bs(b"WATCH"), bs(b"foo"), bs(b"bar")],
            &store,
            &registry,
            &mut tx_state,
            &watch_state,
        )
        .unwrap();

        assert!(matches!(result, TransactionCommandResult::Respond(_)));
        assert_eq!(watch_state.borrow().watched_keys.len(), 2);
    }

    #[test]
    fn watch_inside_multi_queues_error_slot() {
        let (store, registry, watch_state) = setup();
        let mut tx_state = TxState::Multi {
            queue: Vec::new(),
            error: false,
        };

        let result = handle_transaction_command(
            1,
            b"WATCH",
            &[bs(b"WATCH"), bs(b"foo")],
            &store,
            &registry,
            &mut tx_state,
            &watch_state,
        )
        .unwrap();

        assert!(matches!(result, TransactionCommandResult::Respond(_)));
        let TxState::Multi { queue, error } = tx_state else {
            panic!("expected multi");
        };
        assert!(!error);
        assert_eq!(queue.len(), 1);
        assert!(queue[0].response_override.is_some());
    }

    #[test]
    fn unwatch_inside_multi_is_queued_and_clears_on_execution() {
        let (_, registry, watch_state) = setup();
        watch_state
            .borrow_mut()
            .watched_keys
            .push((CompactString::from("foo"), 0));
        registry
            .borrow_mut()
            .watch(1, CompactString::from("foo"), 0);
        let mut tx_state = TxState::Multi {
            queue: Vec::new(),
            error: false,
        };
        let store = Rc::new(RefCell::new(Store::default()));

        handle_transaction_command(
            1,
            b"UNWATCH",
            &[bs(b"UNWATCH")],
            &store,
            &registry,
            &mut tx_state,
            &watch_state,
        )
        .unwrap();

        let TxState::Multi { queue, .. } = &tx_state else {
            panic!("expected multi");
        };
        assert_eq!(
            queued_command_response(1, &queue[0], &registry, &watch_state),
            Some(b"+OK\r\n".to_vec())
        );
        assert!(watch_state.borrow().watched_keys.is_empty());
    }

    #[test]
    fn syntax_error_during_queue_sets_abort_flag() {
        let mut tx_state = TxState::Multi {
            queue: Vec::new(),
            error: false,
        };
        let result = queue_transaction_command(b"SET", &[bs(b"SET")], &mut tx_state);
        assert!(result.is_err());
        let TxState::Multi { queue, error } = tx_state else {
            panic!("expected multi");
        };
        assert!(error);
        assert_eq!(queue.len(), 1);
        assert!(queue[0].response_override.is_some());
    }

    #[test]
    fn clear_watch_state_clears_dirty_and_keys() {
        let (_, registry, watch_state) = setup();
        watch_state.borrow_mut().dirty = true;
        watch_state
            .borrow_mut()
            .watched_keys
            .push((CompactString::from("foo"), 1));
        clear_watch_state(1, &registry, &watch_state);
        assert!(!watch_state.borrow().dirty);
        assert!(watch_state.borrow().watched_keys.is_empty());
    }

    #[test]
    fn bypass_commands_are_not_queued() {
        assert!(should_execute_immediately_in_multi(b"QUIT"));
        assert!(should_execute_immediately_in_multi(b"AUTH"));
        assert!(!should_execute_immediately_in_multi(b"SET"));
    }

    #[test]
    fn compile_time_types_cover_connection_map_use() {
        let _ = ConnectionMap::default();
    }
}
