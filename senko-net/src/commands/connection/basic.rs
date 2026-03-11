#![allow(clippy::too_many_arguments)]

use std::{cell::RefCell, rc::Rc};

use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{ModuleResponse, SenkoConfig, SenkoValue};
use senko_proto::Frame;
use senko_store::Response;
use smallvec::{SmallVec, smallvec};

use crate::{
    acl,
    blocked::BlockedKeyRegistry,
    commands::transaction::clear_watch_state,
    connection::{
        ConnectionFlags, ConnectionMeta, ConnectionState, ReplyMode, bulk_string, error_bytes,
        error_message, frame_bytes, serialize_response, simple_string,
    },
    transaction::{TxState, WatchRegistry, WatchState},
};

pub(crate) struct BasicCommandOutcome {
    pub response: Vec<u8>,
    pub close_after_write: bool,
    pub suppress_response: bool,
    pub force_send_response: bool,
}

pub(crate) fn execute(
    command: &[u8],
    args: &[Frame<'_>],
    config: &SenkoConfig,
    meta: &mut ConnectionMeta,
    state: &mut ConnectionState,
    tx_state: &mut TxState,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    watch_state: &Rc<RefCell<WatchState>>,
) -> Option<Result<BasicCommandOutcome, Vec<u8>>> {
    if eq_ascii(command, b"PING") {
        return Some(handle_ping(args, meta));
    }
    if eq_ascii(command, b"ECHO") {
        return Some(handle_echo(args));
    }
    if eq_ascii(command, b"QUIT") {
        return Some(Ok(handle_quit(
            meta,
            state,
            tx_state,
            blocked,
            watch_registry,
            watch_state,
        )));
    }
    if eq_ascii(command, b"RESET") {
        return Some(Ok(handle_reset(
            meta,
            state,
            tx_state,
            blocked,
            watch_registry,
            watch_state,
        )));
    }
    if eq_ascii(command, b"SELECT") {
        return Some(handle_select(args, meta));
    }
    if eq_ascii(command, b"AUTH") {
        return Some(
            handle_auth(config, args, meta).map(|response| BasicCommandOutcome {
                response,
                close_after_write: false,
                suppress_response: false,
                force_send_response: false,
            }),
        );
    }
    if eq_ascii(command, b"HELLO") {
        return Some(
            handle_hello(config, args, meta).map(|response| BasicCommandOutcome {
                response,
                close_after_write: false,
                suppress_response: false,
                force_send_response: false,
            }),
        );
    }
    None
}

pub(crate) fn allows_unauthenticated(command: &[u8]) -> bool {
    eq_ascii(command, b"AUTH")
        || eq_ascii(command, b"HELLO")
        || eq_ascii(command, b"QUIT")
        || eq_ascii(command, b"RESET")
}

pub(crate) fn validate_queued(command: &[u8], frames: &[Frame<'_>]) -> Option<Result<(), Vec<u8>>> {
    let args = &frames[1..];
    if eq_ascii(command, b"PING") {
        return Some(validate_ping(args));
    }
    if eq_ascii(command, b"ECHO") {
        return Some(validate_echo(args));
    }
    if eq_ascii(command, b"SELECT") {
        return Some(validate_select(args));
    }
    None
}

fn handle_ping(args: &[Frame<'_>], meta: &ConnectionMeta) -> Result<BasicCommandOutcome, Vec<u8>> {
    let response = match args {
        [] => {
            if meta.flags.contains(ConnectionFlags::PUBSUB) {
                if meta.resp_version == 3 {
                    let items = Response::Array(Box::new(smallvec![
                        bulk_value(b"pong"),
                        bulk_value(b""),
                        bulk_value(b""),
                    ]));
                    serialize_push(&items)
                } else {
                    serialize_response(
                        &Response::Array(Box::new(smallvec![
                            bulk_value(b"pong"),
                            bulk_value(b""),
                            bulk_value(b""),
                        ])),
                        false,
                    )
                }
            } else {
                simple_string(b"PONG")
            }
        }
        [value] => {
            let bytes = frame_bytes(value).map_err(|error| error_bytes(&error))?;
            if meta.flags.contains(ConnectionFlags::PUBSUB) {
                if meta.resp_version == 3 {
                    let items = Response::Array(Box::new(smallvec![
                        bulk_value(b"pong"),
                        bulk_value(b""),
                        bulk_value(bytes),
                    ]));
                    serialize_push(&items)
                } else {
                    serialize_response(
                        &Response::Array(Box::new(smallvec![
                            bulk_value(b"pong"),
                            bulk_value(b""),
                            bulk_value(bytes),
                        ])),
                        false,
                    )
                }
            } else {
                bulk_string(bytes)
            }
        }
        _ => {
            return Err(error_message(
                "ERR wrong number of arguments for 'ping' command",
            ));
        }
    };
    Ok(BasicCommandOutcome {
        response,
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    })
}

fn handle_echo(args: &[Frame<'_>]) -> Result<BasicCommandOutcome, Vec<u8>> {
    let [value] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'echo' command",
        ));
    };
    let bytes = frame_bytes(value).map_err(|error| error_bytes(&error))?;
    Ok(BasicCommandOutcome {
        response: bulk_string(bytes),
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    })
}

fn handle_quit(
    meta: &mut ConnectionMeta,
    state: &mut ConnectionState,
    tx_state: &mut TxState,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    watch_state: &Rc<RefCell<WatchState>>,
) -> BasicCommandOutcome {
    unblock_if_needed(meta, state, blocked);
    if matches!(tx_state, TxState::Multi { .. }) {
        *tx_state = TxState::None;
        meta.flags.remove(ConnectionFlags::MULTI);
        clear_watch_state(meta.id, watch_registry, watch_state);
    }
    BasicCommandOutcome {
        response: simple_string(b"OK"),
        close_after_write: true,
        suppress_response: false,
        force_send_response: false,
    }
}

fn handle_reset(
    meta: &mut ConnectionMeta,
    state: &mut ConnectionState,
    tx_state: &mut TxState,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    watch_state: &Rc<RefCell<WatchState>>,
) -> BasicCommandOutcome {
    unblock_if_needed(meta, state, blocked);
    *tx_state = TxState::None;
    meta.flags.remove(ConnectionFlags::MULTI);
    meta.flags.remove(ConnectionFlags::TRACKING);
    meta.flags.remove(ConnectionFlags::PUBSUB);
    clear_watch_state(meta.id, watch_registry, watch_state);
    meta.name = None;
    meta.db = 0;
    meta.resp_version = 2;
    meta.no_evict = false;
    meta.no_touch = false;
    meta.reply_mode = ReplyMode::Normal;
    acl::reset_connection_auth(meta);
    BasicCommandOutcome {
        response: simple_string(b"RESET"),
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

fn handle_select(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
) -> Result<BasicCommandOutcome, Vec<u8>> {
    let [index] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'select' command",
        ));
    };
    let index = parse_u8(frame_bytes(index).map_err(|error| error_bytes(&error))?)
        .ok_or_else(|| error_message("ERR DB index is out of range"))?;
    if index != 0 {
        return Err(error_message("ERR DB index is out of range"));
    }
    meta.db = 0;
    Ok(BasicCommandOutcome {
        response: simple_string(b"OK"),
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    })
}

fn handle_auth(
    _config: &SenkoConfig,
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
) -> Result<Vec<u8>, Vec<u8>> {
    let (username, password) = match args {
        [password] => (
            b"default".as_slice(),
            frame_bytes(password).map_err(|error| error_bytes(&error))?,
        ),
        [username, password] => (
            frame_bytes(username).map_err(|error| error_bytes(&error))?,
            frame_bytes(password).map_err(|error| error_bytes(&error))?,
        ),
        _ => {
            return Err(error_message(
                "ERR wrong number of arguments for 'auth' command",
            ));
        }
    };
    acl::authenticate(meta, username, password)?;
    Ok(simple_string(b"OK"))
}

fn handle_hello(
    _config: &SenkoConfig,
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
) -> Result<Vec<u8>, Vec<u8>> {
    let mut index = 0usize;
    let mut selected_proto = meta.resp_version;
    let mut selected_name = meta.name.clone();
    let mut auth_succeeded = meta.flags.contains(ConnectionFlags::AUTHENTICATED);

    if let Some(first) = args.first() {
        let first = frame_bytes(first).map_err(|error| error_bytes(&error))?;
        if first.iter().all(u8::is_ascii_digit) {
            selected_proto = match first {
                b"2" => 2,
                b"3" => 3,
                _ => return Err(error_message("NOPROTO unsupported protocol version")),
            };
            index = 1;
        }
    }

    while index < args.len() {
        let option = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
        index += 1;

        if eq_ascii(option, b"AUTH") {
            if index + 1 >= args.len() {
                return Err(error_message("ERR Syntax error in HELLO option 'AUTH'"));
            }
            let auth_args = &args[index..index + 2];
            let username = frame_bytes(&auth_args[0]).map_err(|error| error_bytes(&error))?;
            let password = frame_bytes(&auth_args[1]).map_err(|error| error_bytes(&error))?;
            acl::authenticate(meta, username, password)?;
            auth_succeeded = true;
            index += 2;
            continue;
        }

        if eq_ascii(option, b"SETNAME") {
            if index >= args.len() {
                return Err(error_message("ERR Syntax error in HELLO option 'SETNAME'"));
            }
            let name_bytes = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
            validate_client_name(name_bytes)?;
            selected_name = Some(CompactString::from_utf8(name_bytes).map_err(|_| {
                error_message(
                    "ERR Client names cannot contain spaces, newlines or special characters.",
                )
            })?);
            index += 1;
            continue;
        }

        return Err(error_message(&format!(
            "ERR Syntax error in HELLO option '{}'",
            String::from_utf8_lossy(option)
        )));
    }

    if !auth_succeeded {
        return Err(error_message(
            "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time",
        ));
    }

    meta.resp_version = selected_proto;
    meta.name = selected_name;

    Ok(serialize_response(
        &hello_response(meta),
        selected_proto == 3,
    ))
}

fn validate_ping(args: &[Frame<'_>]) -> Result<(), Vec<u8>> {
    match args {
        [] => Ok(()),
        [value] => frame_bytes(value)
            .map(|_| ())
            .map_err(|error| error_bytes(&error)),
        _ => Err(error_message(
            "ERR wrong number of arguments for 'ping' command",
        )),
    }
}

fn validate_echo(args: &[Frame<'_>]) -> Result<(), Vec<u8>> {
    let [value] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'echo' command",
        ));
    };
    frame_bytes(value)
        .map(|_| ())
        .map_err(|error| error_bytes(&error))
}

fn validate_select(args: &[Frame<'_>]) -> Result<(), Vec<u8>> {
    let [index] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'select' command",
        ));
    };
    let index = frame_bytes(index).map_err(|error| error_bytes(&error))?;
    if parse_u8(index).is_some() {
        Ok(())
    } else {
        Err(error_message("ERR DB index is out of range"))
    }
}

fn validate_client_name(name: &[u8]) -> Result<(), Vec<u8>> {
    if name.is_empty() || name.iter().any(|byte| !matches!(*byte, 33..=126)) {
        return Err(error_message(
            "ERR Client names cannot contain spaces, newlines or special characters.",
        ));
    }
    Ok(())
}

fn unblock_if_needed(
    meta: &mut ConnectionMeta,
    state: &mut ConnectionState,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
) {
    if let ConnectionState::Blocked { .. } = state {
        blocked.borrow_mut().remove_client(meta.id);
        meta.flags.remove(ConnectionFlags::BLOCKED);
        *state = ConnectionState::Reading;
    }
}

fn hello_response(meta: &ConnectionMeta) -> Response {
    let modules = match crate::modules::list_response() {
        ModuleResponse::Array(items) => Response::Array(Box::new(
            items.into_iter().map(module_to_response).collect(),
        )),
        _ => Response::Array(Box::new(SmallVec::new())),
    };
    Response::Map(Box::new(smallvec![
        bulk_value(b"server"),
        bulk_value(b"senko"),
        bulk_value(b"version"),
        bulk_value(b"8.0.0"),
        bulk_value(b"proto"),
        Response::Integer(i64::from(meta.resp_version)),
        bulk_value(b"id"),
        Response::Integer(meta.id as i64),
        bulk_value(b"mode"),
        bulk_value(b"standalone"),
        bulk_value(b"role"),
        bulk_value(b"master"),
        bulk_value(b"modules"),
        modules,
    ]))
}

fn module_to_response(response: ModuleResponse) -> Response {
    match response {
        ModuleResponse::Simple(value) => Response::Simple(value),
        ModuleResponse::Bulk(value) => Response::Value(value.map(SenkoValue::Raw)),
        ModuleResponse::Integer(value) => Response::Integer(value),
        ModuleResponse::Array(values) => Response::Array(Box::new(
            values.into_iter().map(module_to_response).collect(),
        )),
        ModuleResponse::Map(values) => Response::Map(Box::new(
            values.into_iter().map(module_to_response).collect(),
        )),
    }
}

fn bulk_value(value: &[u8]) -> Response {
    Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(value))))
}

fn serialize_push(items: &Response) -> Vec<u8> {
    let Response::Array(items) = items else {
        return Vec::new();
    };
    let mut out = bytes::BytesMut::new();
    out.extend_from_slice(format!(">{}\r\n", items.len()).as_bytes());
    for item in items.iter() {
        crate::connection::write_response(&mut out, item, true);
    }
    out.to_vec()
}

fn parse_u8(bytes: &[u8]) -> Option<u8> {
    std::str::from_utf8(bytes).ok()?.parse::<u8>().ok()
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

#[cfg(test)]
mod tests {
    use super::{allows_unauthenticated, execute, validate_queued};
    use crate::{
        blocked::BlockedKeyRegistry,
        connection::{ConnectionFlags, ConnectionMeta, ConnectionState, ReplyMode},
        transaction::{TxState, WatchRegistry, WatchState},
    };
    use compact_str::CompactString;
    use senko_core::SenkoConfig;
    use senko_proto::Frame;
    use std::{
        cell::RefCell,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        rc::Rc,
    };

    fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(input)
    }

    fn meta() -> ConnectionMeta {
        ConnectionMeta {
            id: 7,
            username: CompactString::const_new("default"),
            name: None,
            db: 0,
            flags: ConnectionFlags::empty(),
            created_at: 0,
            last_cmd: None,
            last_cmd_at: 0,
            lib_name: None,
            lib_ver: None,
            peer_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234),
            local_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            resp_version: 2,
            no_evict: false,
            no_touch: false,
            reply_mode: ReplyMode::Normal,
            watch_count: 0,
            multi_queue_len: -1,
            tracking_redirect: -1,
            tracking_optin: false,
            tracking_optout: false,
            tracking_bcast: false,
            tracking_noloop: false,
            tracking_prefixes: smallvec::SmallVec::new(),
            tracking_caching: None,
            replica_listening_port: None,
            replica_ip_address: None,
            replica_psync2: false,
            replica_eof: false,
            replica_ack_offset: 0,
        }
    }

    #[test]
    fn unauthenticated_allowlist_matches_spec() {
        assert!(allows_unauthenticated(b"AUTH"));
        assert!(allows_unauthenticated(b"HELLO"));
        assert!(allows_unauthenticated(b"QUIT"));
        assert!(allows_unauthenticated(b"RESET"));
        assert!(!allows_unauthenticated(b"PING"));
    }

    #[test]
    fn queued_connection_commands_validate() {
        assert!(validate_queued(b"PING", &[bs(b"PING")]).unwrap().is_ok());
        assert!(
            validate_queued(b"ECHO", &[bs(b"ECHO"), bs(b"x")])
                .unwrap()
                .is_ok()
        );
        assert!(
            validate_queued(b"SELECT", &[bs(b"SELECT"), bs(b"0")])
                .unwrap()
                .is_ok()
        );
    }

    #[test]
    fn reset_clears_connection_state() {
        let config = SenkoConfig::default();
        let blocked = Rc::new(RefCell::new(BlockedKeyRegistry::default()));
        let watch_registry = Rc::new(RefCell::new(WatchRegistry::default()));
        let watch_state = Rc::new(RefCell::new(WatchState::default()));
        let mut meta = meta();
        meta.flags.insert(ConnectionFlags::AUTHENTICATED);
        meta.flags.insert(ConnectionFlags::MULTI);
        meta.flags.insert(ConnectionFlags::TRACKING);
        meta.flags.insert(ConnectionFlags::PUBSUB);
        meta.name = Some(CompactString::from("name"));
        meta.db = 1;
        meta.resp_version = 3;
        meta.no_evict = true;
        meta.no_touch = true;
        meta.reply_mode = ReplyMode::Skip;
        let mut state = ConnectionState::Reading;
        let mut tx_state = TxState::Multi {
            queue: Vec::new(),
            error: false,
        };

        let result = execute(
            b"RESET",
            &[],
            &config,
            &mut meta,
            &mut state,
            &mut tx_state,
            &blocked,
            &watch_registry,
            &watch_state,
        )
        .unwrap()
        .unwrap();

        assert_eq!(result.response, b"+RESET\r\n");
        assert!(!result.close_after_write);
        assert_eq!(meta.name, None);
        assert_eq!(meta.db, 0);
        assert_eq!(meta.resp_version, 2);
        assert!(!meta.no_evict);
        assert!(!meta.no_touch);
        assert_eq!(meta.reply_mode, ReplyMode::Normal);
        assert_eq!(meta.username, "default");
        assert!(meta.flags.contains(ConnectionFlags::AUTHENTICATED));
        assert!(!meta.flags.contains(ConnectionFlags::MULTI));
        assert!(!meta.flags.contains(ConnectionFlags::TRACKING));
        assert!(!meta.flags.contains(ConnectionFlags::PUBSUB));
        assert!(matches!(tx_state, TxState::None));
    }
}
