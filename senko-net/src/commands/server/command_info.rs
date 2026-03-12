use std::{
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use ahash::RandomState;
use bytes::Bytes;
use flume::{Receiver, Sender};
use hashbrown::HashMap;
use senko_core::{SenkoConfig, SenkoValue};
use senko_proto::Frame;
use senko_store::{Response, pattern::glob_match};
use smallvec::{SmallVec, smallvec};

use crate::connection::{
    ConnectionFlags, ConnectionMeta, error_bytes, error_message, frame_bytes, serialize_response,
    simple_string,
};

#[derive(Debug)]
pub struct CommandCommandOutcome {
    pub response: Vec<u8>,
    pub close_after_write: bool,
    pub suppress_response: bool,
    pub force_send_response: bool,
}

#[derive(Debug)]
pub struct MonitorSubscription {
    shard_id: usize,
    subscriber_id: u64,
    receiver: Receiver<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct CommandMeta {
    pub name: &'static str,
    pub arity: i64,
    pub flags: &'static [&'static str],
    pub first_key: i64,
    pub last_key: i64,
    pub step: i64,
    pub acl_categories: &'static [&'static str],
    pub tips: &'static [&'static str],
    pub key_specs: &'static [KeySpec],
    pub subcommands: &'static [&'static str],
    pub summary: &'static str,
    pub since: &'static str,
    pub group: &'static str,
    pub complexity: &'static str,
    pub arguments: &'static [DocArgument],
}

#[derive(Debug, Clone)]
pub struct KeySpec {
    extractor: KeyExtractor,
    flags: &'static [&'static str],
}

#[derive(Debug, Clone, Copy)]
enum KeyExtractor {
    Range {
        first: usize,
        last: isize,
        step: usize,
    },
    EveryOther {
        start: usize,
        step: usize,
    },
    NumKeys {
        count_index: usize,
        start: usize,
    },
}

#[derive(Debug, Clone)]
pub struct DocArgument {
    name: &'static str,
    arg_type: &'static str,
    token: &'static str,
    optional: bool,
    multiple: bool,
}

#[derive(Debug)]
struct CommandRegistry {
    metas: Box<[CommandMeta]>,
    by_name: HashMap<&'static str, usize, RandomState>,
}

#[derive(Debug)]
struct MonitorRegistry {
    shards: Box<[MonitorShard]>,
    next_id: AtomicU64,
}

#[derive(Debug)]
struct MonitorShard {
    count: AtomicU32,
    subscribers: Mutex<Vec<(u64, Sender<Vec<u8>>)>>,
}

static COMMAND_REGISTRY: OnceLock<CommandRegistry> = OnceLock::new();
static MONITOR_REGISTRY: OnceLock<Arc<MonitorRegistry>> = OnceLock::new();

pub fn init(config: &SenkoConfig) {
    let _ = registry();
    let candidate = Arc::new(MonitorRegistry::new(config.num_shards));
    if let Some(existing) = MONITOR_REGISTRY.get() {
        existing.reset();
    } else {
        let _ = MONITOR_REGISTRY.set(candidate);
    }
}

pub fn execute(
    command: &[u8],
    args: &[Frame<'_>],
    resp3: bool,
    meta: &mut ConnectionMeta,
    shard_id: usize,
    monitor: &mut Option<MonitorSubscription>,
) -> Option<Result<CommandCommandOutcome, Vec<u8>>> {
    if eq_ascii(command, b"COMMAND") {
        return Some(dispatch_command(args, resp3));
    }
    if eq_ascii(command, b"MONITOR") {
        return Some(command_monitor(args, meta, shard_id, monitor));
    }
    None
}

pub fn publish_monitor(shard_id: usize, meta: &ConnectionMeta, command: &[u8], args: &[Frame<'_>]) {
    let Some(registry) = MONITOR_REGISTRY.get() else {
        return;
    };
    if registry.count(shard_id) == 0 {
        return;
    }
    let line = format_monitor_line(meta, command, args);
    registry.publish(shard_id, line.into_bytes());
}

pub fn monitor_allows_command(command: &[u8]) -> bool {
    eq_ascii(command, b"RESET") || eq_ascii(command, b"QUIT")
}

pub fn drain_monitor_messages(subscription: &MonitorSubscription) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while let Ok(message) = subscription.receiver.try_recv() {
        out.push(message);
    }
    out
}

pub fn unsubscribe_monitor(subscription: &MonitorSubscription) {
    if let Some(registry) = MONITOR_REGISTRY.get() {
        registry.unsubscribe(subscription.shard_id, subscription.subscriber_id);
    }
}

pub(crate) fn is_write_command(command: &[u8]) -> bool {
    let Ok(name) = std::str::from_utf8(command).map(str::to_ascii_lowercase) else {
        return false;
    };
    registry()
        .lookup(&name)
        .is_some_and(|meta| meta.flags.contains(&"write"))
}

fn command_monitor(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    shard_id: usize,
    monitor: &mut Option<MonitorSubscription>,
) -> Result<CommandCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'monitor' command",
        ));
    }
    if let Some(existing) = monitor.take() {
        unsubscribe_monitor(&existing);
    }
    let subscription = MONITOR_REGISTRY
        .get()
        .expect("monitor registry not initialized")
        .subscribe(shard_id);
    meta.flags.insert(ConnectionFlags::MONITOR);
    *monitor = Some(subscription);
    Ok(outcome(simple_string(b"OK")))
}

fn dispatch_command(args: &[Frame<'_>], resp3: bool) -> Result<CommandCommandOutcome, Vec<u8>> {
    if args.is_empty() {
        return Ok(outcome(serialize_response(
            &Response::Array(Box::new(
                registry()
                    .metas
                    .iter()
                    .map(command_meta_response)
                    .collect::<SmallVec<[Response; 16]>>(),
            )),
            resp3,
        )));
    }
    let subcommand = frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?;
    let rest = &args[1..];
    if eq_ascii(subcommand, b"COUNT") {
        return command_count(rest, resp3);
    }
    if eq_ascii(subcommand, b"INFO") {
        return command_info(rest, resp3);
    }
    if eq_ascii(subcommand, b"DOCS") {
        return command_docs(rest, resp3);
    }
    if eq_ascii(subcommand, b"LIST") {
        return command_list(rest, resp3);
    }
    if eq_ascii(subcommand, b"GETKEYS") {
        return command_getkeys(rest, resp3, false);
    }
    if eq_ascii(subcommand, b"GETKEYSANDFLAGS") {
        return command_getkeys(rest, resp3, true);
    }
    Err(error_message(
        "ERR unknown subcommand or wrong number of arguments for 'COMMAND'",
    ))
}

fn command_count(args: &[Frame<'_>], resp3: bool) -> Result<CommandCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'command|count' command",
        ));
    }
    Ok(outcome(serialize_response(
        &Response::Integer(registry().metas.len() as i64),
        resp3,
    )))
}

fn command_info(args: &[Frame<'_>], resp3: bool) -> Result<CommandCommandOutcome, Vec<u8>> {
    if args.is_empty() {
        return command_count(&[], resp3).map(|_| {
            outcome(serialize_response(
                &Response::Array(Box::new(
                    registry()
                        .metas
                        .iter()
                        .map(command_meta_response)
                        .collect::<SmallVec<[Response; 16]>>(),
                )),
                resp3,
            ))
        });
    }
    let response = Response::Array(Box::new(
        args.iter()
            .map(|arg| {
                let name = parse_lower(frame_bytes(arg).map_err(|error| error_bytes(&error))?);
                Ok(name
                    .ok()
                    .and_then(|name| registry().lookup(&name))
                    .map(command_meta_response)
                    .unwrap_or(Response::Value(None)))
            })
            .collect::<Result<SmallVec<[Response; 16]>, Vec<u8>>>()?,
    ));
    Ok(outcome(serialize_response(&response, resp3)))
}

fn command_docs(args: &[Frame<'_>], resp3: bool) -> Result<CommandCommandOutcome, Vec<u8>> {
    let names = if args.is_empty() {
        registry()
            .metas
            .iter()
            .map(|meta| meta.name)
            .collect::<Vec<_>>()
    } else {
        args.iter()
            .map(|arg| parse_lower(frame_bytes(arg).map_err(|error| error_bytes(&error))?))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|name| Box::leak(name.into_boxed_str()) as &'static str)
            .collect()
    };
    let mut entries = SmallVec::<[Response; 32]>::new();
    for name in names {
        if let Some(meta) = registry().lookup(name) {
            entries.push(bulk(meta.name));
            entries.push(command_docs_map(meta));
        }
    }
    Ok(outcome(serialize_response(
        &Response::Map(Box::new(entries)),
        resp3,
    )))
}

fn command_list(args: &[Frame<'_>], resp3: bool) -> Result<CommandCommandOutcome, Vec<u8>> {
    let metas = if args.is_empty() {
        registry().metas.iter().collect::<Vec<_>>()
    } else {
        parse_command_list_filter(args)?
    };
    let response = Response::Array(Box::new(
        metas
            .into_iter()
            .map(|meta| bulk(meta.name))
            .collect::<SmallVec<[Response; 16]>>(),
    ));
    Ok(outcome(serialize_response(&response, resp3)))
}

fn command_getkeys(
    args: &[Frame<'_>],
    resp3: bool,
    with_flags: bool,
) -> Result<CommandCommandOutcome, Vec<u8>> {
    if args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'command|getkeys' command",
        ));
    }
    let command = parse_lower(frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?)?;
    let Some(meta) = registry().lookup(&command) else {
        return Err(error_message("ERR Invalid command specified"));
    };
    let keys = extract_keys(meta, args)?;
    let response = if with_flags {
        Response::Array(Box::new(
            keys.into_iter()
                .map(|(key, flags)| {
                    Response::Array(Box::new(smallvec![
                        bulk_bytes(&key),
                        Response::Array(Box::new(
                            flags
                                .iter()
                                .map(|flag| bulk(flag))
                                .collect::<SmallVec<[Response; 16]>>(),
                        )),
                    ]))
                })
                .collect(),
        ))
    } else {
        Response::Array(Box::new(
            keys.into_iter()
                .map(|(key, _)| bulk_bytes(&key))
                .collect::<SmallVec<[Response; 16]>>(),
        ))
    };
    Ok(outcome(serialize_response(&response, resp3)))
}

fn parse_command_list_filter(args: &[Frame<'_>]) -> Result<Vec<&'static CommandMeta>, Vec<u8>> {
    if args.len() != 3 {
        return Err(error_message(
            "ERR wrong number of arguments for 'command|list' command",
        ));
    }
    let filterby = frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?;
    if !eq_ascii(filterby, b"FILTERBY") {
        return Err(error_message("ERR syntax error"));
    }
    let filter = parse_lower(frame_bytes(&args[1]).map_err(|error| error_bytes(&error))?)?;
    let value = frame_bytes(&args[2]).map_err(|error| error_bytes(&error))?;
    match filter.as_str() {
        "module" => Ok(Vec::new()),
        "aclcat" => {
            let wanted = parse_lower(value)?;
            Ok(registry()
                .metas
                .iter()
                .filter(|meta| {
                    meta.acl_categories.iter().any(|category| {
                        category
                            .trim_start_matches('@')
                            .eq_ignore_ascii_case(wanted.as_str())
                    })
                })
                .collect())
        }
        "pattern" => Ok(registry()
            .metas
            .iter()
            .filter(|meta| glob_match(value, meta.name.as_bytes()))
            .collect()),
        _ => Err(error_message("ERR syntax error")),
    }
}

fn command_meta_response(meta: &CommandMeta) -> Response {
    Response::Array(Box::new(smallvec![
        bulk(meta.name),
        Response::Integer(meta.arity),
        Response::Array(Box::new(
            meta.flags
                .iter()
                .map(|flag| bulk(flag))
                .collect::<SmallVec<[Response; 16]>>(),
        )),
        Response::Integer(meta.first_key),
        Response::Integer(meta.last_key),
        Response::Integer(meta.step),
        Response::Array(Box::new(
            meta.acl_categories
                .iter()
                .map(|category| bulk(category))
                .collect::<SmallVec<[Response; 16]>>(),
        )),
        Response::Array(Box::new(
            meta.tips
                .iter()
                .map(|tip| bulk(tip))
                .collect::<SmallVec<[Response; 16]>>(),
        )),
        Response::Array(Box::new(
            meta.key_specs
                .iter()
                .map(command_keyspec_response)
                .collect::<SmallVec<[Response; 16]>>(),
        )),
        Response::Array(Box::new(
            meta.subcommands
                .iter()
                .map(|sub| bulk(sub))
                .collect::<SmallVec<[Response; 16]>>(),
        )),
    ]))
}

fn command_keyspec_response(spec: &KeySpec) -> Response {
    let flags = spec
        .flags
        .iter()
        .map(|flag| bulk(flag))
        .collect::<SmallVec<[Response; 16]>>();
    Response::Map(Box::new(smallvec![
        bulk("flags"),
        Response::Array(Box::new(flags)),
    ]))
}

fn command_docs_map(meta: &CommandMeta) -> Response {
    Response::Map(Box::new(smallvec![
        bulk("summary"),
        bulk(meta.summary),
        bulk("since"),
        bulk(meta.since),
        bulk("group"),
        bulk(meta.group),
        bulk("complexity"),
        bulk(meta.complexity),
        bulk("arguments"),
        Response::Array(Box::new(
            meta.arguments
                .iter()
                .map(doc_argument_response)
                .collect::<SmallVec<[Response; 16]>>(),
        )),
    ]))
}

fn doc_argument_response(argument: &DocArgument) -> Response {
    Response::Map(Box::new(smallvec![
        bulk("name"),
        bulk(argument.name),
        bulk("type"),
        bulk(argument.arg_type),
        bulk("token"),
        bulk(argument.token),
        bulk("optional"),
        bulk(if argument.optional { "yes" } else { "no" }),
        bulk("multiple"),
        bulk(if argument.multiple { "yes" } else { "no" }),
    ]))
}

#[allow(clippy::type_complexity)]
fn extract_keys(
    meta: &CommandMeta,
    args: &[Frame<'_>],
) -> Result<Vec<(Vec<u8>, &'static [&'static str])>, Vec<u8>> {
    let mut out = Vec::new();
    for spec in meta.key_specs {
        match spec.extractor {
            KeyExtractor::Range { first, last, step } => {
                let end = if last < 0 {
                    args.len().saturating_sub(last.unsigned_abs())
                } else {
                    usize::min(last as usize + 1, args.len())
                };
                for index in (first..end).step_by(step) {
                    out.push((
                        frame_bytes(&args[index])
                            .map_err(|error| error_bytes(&error))?
                            .to_vec(),
                        spec.flags,
                    ));
                }
            }
            KeyExtractor::EveryOther { start, step } => {
                for index in (start..args.len()).step_by(step) {
                    out.push((
                        frame_bytes(&args[index])
                            .map_err(|error| error_bytes(&error))?
                            .to_vec(),
                        spec.flags,
                    ));
                }
            }
            KeyExtractor::NumKeys { count_index, start } => {
                let count = std::str::from_utf8(
                    frame_bytes(&args[count_index]).map_err(|error| error_bytes(&error))?,
                )
                .ok()
                .and_then(|text| text.parse::<usize>().ok())
                .ok_or_else(|| error_message("ERR Invalid numkeys specification"))?;
                for index in start..start + count {
                    if let Some(arg) = args.get(index) {
                        out.push((
                            frame_bytes(arg)
                                .map_err(|error| error_bytes(&error))?
                                .to_vec(),
                            spec.flags,
                        ));
                    }
                }
            }
        }
    }
    Ok(out)
}

fn registry() -> &'static CommandRegistry {
    COMMAND_REGISTRY.get_or_init(build_registry)
}

fn build_registry() -> CommandRegistry {
    let mut metas = Vec::new();
    let mut by_name = HashMap::with_hasher(RandomState::new());
    for meta in command_metas() {
        let index = metas.len();
        by_name.insert(meta.name, index);
        metas.push(meta);
    }
    CommandRegistry {
        metas: metas.into_boxed_slice(),
        by_name,
    }
}

fn command_metas() -> Vec<CommandMeta> {
    let mut metas = Vec::new();
    let get_args: &'static [DocArgument] =
        Box::leak(vec![arg("key", "key", "key", false, false)].into_boxed_slice());
    let set_args: &'static [DocArgument] = Box::leak(
        vec![
            arg("key", "key", "key", false, false),
            arg("value", "string", "value", false, false),
        ]
        .into_boxed_slice(),
    );
    let hset_args: &'static [DocArgument] = Box::leak(
        vec![
            arg("key", "key", "key", false, false),
            arg("field", "string", "field", false, true),
            arg("value", "string", "value", false, true),
        ]
        .into_boxed_slice(),
    );

    macro_rules! push {
        ($name:expr, $arity:expr, $flags:expr, $fk:expr, $lk:expr, $step:expr, $acl:expr, $group:expr, $summary:expr, $complexity:expr) => {
            metas.push(CommandMeta {
                name: $name,
                arity: $arity,
                flags: $flags,
                first_key: $fk,
                last_key: $lk,
                step: $step,
                acl_categories: $acl,
                tips: &[],
                key_specs: &[],
                subcommands: &[],
                summary: $summary,
                since: "1.0.0",
                group: $group,
                complexity: $complexity,
                arguments: &[],
            });
        };
    }

    push!(
        "get",
        -2,
        &["readonly", "fast"],
        1,
        1,
        1,
        &["@read", "@string", "@fast"],
        "string",
        "Get the value of a key",
        "O(1)"
    );
    metas.last_mut().unwrap().key_specs =
        Box::leak(vec![keys_range(1, 1, 1, &["read", "RO"])].into_boxed_slice());
    metas.last_mut().unwrap().arguments = get_args;
    push!(
        "set",
        -3,
        &["write", "denyoom"],
        1,
        1,
        1,
        &["@write", "@string", "@slow"],
        "string",
        "Set the string value of a key",
        "O(1)"
    );
    metas.last_mut().unwrap().key_specs =
        Box::leak(vec![keys_range(1, 1, 1, &["write", "OW"])].into_boxed_slice());
    metas.last_mut().unwrap().arguments = set_args;
    push!(
        "hset",
        -4,
        &["write", "fast"],
        1,
        1,
        1,
        &["@write", "@hash", "@fast"],
        "hash",
        "Set one or more hash fields",
        "O(N)"
    );
    metas.last_mut().unwrap().key_specs =
        Box::leak(vec![keys_range(1, 1, 1, &["write", "OW"])].into_boxed_slice());
    metas.last_mut().unwrap().arguments = hset_args;
    push!(
        "mset",
        -3,
        &["write"],
        1,
        -1,
        2,
        &["@write", "@string", "@slow"],
        "string",
        "Set multiple keys",
        "O(N)"
    );
    metas.last_mut().unwrap().key_specs =
        Box::leak(vec![keys_every_other(1, 2, &["write", "OW"])].into_boxed_slice());
    push!(
        "mget",
        -2,
        &["readonly"],
        1,
        -1,
        1,
        &["@read", "@string"],
        "string",
        "Get values for multiple keys",
        "O(N)"
    );
    metas.last_mut().unwrap().key_specs =
        Box::leak(vec![keys_range(1, -1, 1, &["read", "RO"])].into_boxed_slice());
    push!(
        "eval",
        -3,
        &["noscript"],
        0,
        0,
        0,
        &["@scripting", "@slow"],
        "scripting",
        "Execute a server-side script",
        "Depends on script"
    );
    metas.last_mut().unwrap().key_specs =
        Box::leak(vec![keys_numkeys(2, 3, &["read", "write"])].into_boxed_slice());

    for name in [
        "append",
        "auth",
        "blmove",
        "blmpop",
        "blpop",
        "brpop",
        "brpoplpush",
        "command",
        "config",
        "copy",
        "dbsize",
        "decr",
        "decrby",
        "del",
        "echo",
        "exists",
        "expire",
        "expireat",
        "expiretime",
        "getdel",
        "getex",
        "getrange",
        "getset",
        "hdel",
        "hexists",
        "hexpire",
        "hexpireat",
        "hexpiretime",
        "hget",
        "hgetall",
        "hgetdel",
        "hgetex",
        "hincrby",
        "hincrbyfloat",
        "hkeys",
        "hlen",
        "hmget",
        "hmset",
        "hpersist",
        "hpexpire",
        "hpexpireat",
        "hpexpiretime",
        "hpttl",
        "hrandfield",
        "hscan",
        "hsetex",
        "hsetnx",
        "hstrlen",
        "httl",
        "hvals",
        "hello",
        "incr",
        "incrby",
        "incrbyfloat",
        "info",
        "keys",
        "lastsave",
        "lcs",
        "lindex",
        "linsert",
        "llen",
        "lmove",
        "lmpop",
        "lolwut",
        "lpop",
        "lpos",
        "lpush",
        "lpushx",
        "lrange",
        "lrem",
        "lset",
        "ltrim",
        "migrate",
        "monitor",
        "move",
        "msetex",
        "msetnx",
        "multi",
        "object",
        "persist",
        "pexpire",
        "pexpireat",
        "pexpiretime",
        "ping",
        "psetex",
        "pttl",
        "publish",
        "pubsub",
        "quit",
        "randomkey",
        "rename",
        "renamenx",
        "reset",
        "restore",
        "role",
        "rpop",
        "rpoplpush",
        "rpush",
        "rpushx",
        "sadd",
        "scan",
        "scard",
        "sdiff",
        "sdiffstore",
        "select",
        "setex",
        "setnx",
        "setrange",
        "sinter",
        "sintercard",
        "sinterstore",
        "sismember",
        "smembers",
        "smismember",
        "smove",
        "sort",
        "sort_ro",
        "spop",
        "srandmember",
        "srem",
        "sscan",
        "strlen",
        "subscribe",
        "substr",
        "sunion",
        "sunionstore",
        "time",
        "touch",
        "ttl",
        "type",
        "unlink",
        "unsubscribe",
        "wait",
        "waitaof",
        "xadd",
        "xack",
        "xackdel",
        "xautoclaim",
        "xclaim",
        "xdel",
        "xdelex",
        "xgroup",
        "xinfo",
        "xlen",
        "xpending",
        "xrange",
        "xrevrange",
        "xsetid",
        "xtrim",
        "zadd",
        "zcard",
        "zcount",
        "zdiff",
        "zdiffstore",
        "zincrby",
        "zinter",
        "zintercard",
        "zinterstore",
        "zlexcount",
        "zmpop",
        "zmscore",
        "zpopmax",
        "zpopmin",
        "zrandmember",
        "zrange",
        "zrangebylex",
        "zrangebyscore",
        "zrangestore",
        "zrank",
        "zrem",
        "zremrangebylex",
        "zremrangebyrank",
        "zremrangebyscore",
        "zrevrange",
        "zrevrangebylex",
        "zrevrangebyscore",
        "zrevrank",
        "zscan",
        "zscore",
        "zunion",
        "zunionstore",
        "acl",
        "client",
        "cluster",
        "pubsub|channels",
        "pubsub|numpat",
        "pubsub|numsub",
        "acl|getuser",
        "acl|setuser",
        "acl|list",
        "acl|users",
        "acl|whoami",
        "acl|cat",
        "acl|genpass",
        "acl|dryrun",
        "acl|log",
        "acl|load",
        "acl|save",
        "acl|deluser",
        "client|id",
        "client|info",
        "client|list",
        "client|getname",
        "client|setname",
        "client|kill",
        "client|pause",
        "client|unpause",
        "client|unblock",
        "client|tracking",
        "client|trackinginfo",
        "cluster|info",
        "cluster|nodes",
        "cluster|shards",
        "cluster|meet",
        "cluster|myid",
        "cluster|slots",
        "cluster|reset",
    ] {
        let (flags, group, acl, fk, lk, step) = classify(name);
        push!(
            name,
            default_arity(name),
            flags,
            fk,
            lk,
            step,
            acl,
            group,
            "Senko command",
            "O(1)"
        );
    }

    metas
}

fn classify(
    name: &str,
) -> (
    &'static [&'static str],
    &'static str,
    &'static [&'static str],
    i64,
    i64,
    i64,
) {
    match name {
        "publish" | "subscribe" | "unsubscribe" | "pubsub" | "pubsub|channels"
        | "pubsub|numpat" | "pubsub|numsub" => (&["pubsub"], "pubsub", &["@pubsub"], 0, 0, 0),
        "acl"
        | "acl|getuser"
        | "acl|setuser"
        | "acl|list"
        | "acl|users"
        | "acl|whoami"
        | "acl|cat"
        | "acl|genpass"
        | "acl|dryrun"
        | "acl|log"
        | "acl|load"
        | "acl|save"
        | "acl|deluser"
        | "client"
        | "client|id"
        | "client|info"
        | "client|list"
        | "client|getname"
        | "client|setname"
        | "client|kill"
        | "client|pause"
        | "client|unpause"
        | "client|unblock"
        | "client|tracking"
        | "client|trackinginfo"
        | "config"
        | "command"
        | "info"
        | "monitor"
        | "cluster"
        | "cluster|info"
        | "cluster|nodes"
        | "cluster|shards"
        | "cluster|meet"
        | "cluster|myid"
        | "cluster|slots"
        | "cluster|reset"
        | "dbsize"
        | "lastsave"
        | "role"
        | "time"
        | "lolwut" => (&["admin"], "server", &["@admin"], 0, 0, 0),
        "ping" | "echo" | "auth" | "hello" | "quit" | "reset" | "select" | "multi" => {
            (&["fast"], "connection", &["@connection"], 0, 0, 0)
        }
        "get" | "mget" | "strlen" | "getrange" | "substr" | "exists" | "keys" | "scan" | "type"
        | "ttl" | "pttl" | "expiretime" | "pexpiretime" | "randomkey" | "hget" | "hgetall"
        | "hkeys" | "hlen" | "hmget" | "hpttl" | "httl" | "hstrlen" | "hvals" | "hexists"
        | "hrandfield" | "hscan" | "llen" | "lindex" | "lrange" | "lpos" | "scard" | "sdiff"
        | "sinter" | "sintercard" | "sismember" | "smembers" | "smismember" | "srandmember"
        | "sscan" | "sunion" | "sort_ro" | "zcard" | "zcount" | "zdiff" | "zinter"
        | "zintercard" | "zlexcount" | "zmscore" | "zrandmember" | "zrange" | "zrangebylex"
        | "zrangebyscore" | "zrank" | "zrevrange" | "zrevrangebylex" | "zrevrangebyscore"
        | "zrevrank" | "zscan" | "zscore" | "zunion" | "xinfo" | "xlen" | "xpending" | "xrange"
        | "xrevrange" => (&["readonly"], "generic", &["@read"], 1, 1, 1),
        _ => (&["write"], "generic", &["@write"], 1, 1, 1),
    }
}

fn default_arity(name: &str) -> i64 {
    match name {
        "ping" | "monitor" | "dbsize" | "role" | "lastsave" | "time" | "multi" | "reset" => 1,
        "echo" | "auth" | "get" | "strlen" | "ttl" | "pttl" | "exists" | "del" | "touch"
        | "hget" | "hkeys" | "hlen" | "hexists" | "hgetall" | "llen" | "lpop" | "rpop"
        | "scard" | "smembers" | "sismember" | "zcard" | "zscore" | "xlen" => -2,
        "set" | "append" | "decrby" | "incrby" | "setex" | "psetex" | "getrange" | "setrange"
        | "hdel" | "hsetnx" | "hincrby" | "lindex" | "lpush" | "rpush" | "sadd" | "srem"
        | "zadd" | "zincrby" | "zrem" | "xadd" => -3,
        _ => -2,
    }
}

impl CommandRegistry {
    fn lookup(&'static self, name: &str) -> Option<&'static CommandMeta> {
        self.by_name
            .get(name)
            .and_then(|index| self.metas.get(*index))
    }
}

impl MonitorRegistry {
    fn new(num_shards: usize) -> Self {
        Self {
            shards: (0..num_shards)
                .map(|_| MonitorShard {
                    count: AtomicU32::new(0),
                    subscribers: Mutex::new(Vec::new()),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            next_id: AtomicU64::new(1),
        }
    }

    fn subscribe(&self, shard_id: usize) -> MonitorSubscription {
        let (sender, receiver) = flume::bounded(512);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let shard = &self.shards[shard_id];
        shard
            .subscribers
            .lock()
            .expect("monitor subscribers lock poisoned")
            .push((id, sender));
        shard.count.fetch_add(1, Ordering::Relaxed);
        MonitorSubscription {
            shard_id,
            subscriber_id: id,
            receiver,
        }
    }

    fn unsubscribe(&self, shard_id: usize, subscriber_id: u64) {
        let shard = &self.shards[shard_id];
        let mut subscribers = shard
            .subscribers
            .lock()
            .expect("monitor subscribers lock poisoned");
        let before = subscribers.len();
        subscribers.retain(|(id, _)| *id != subscriber_id);
        if subscribers.len() != before {
            let _ = shard
                .count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_sub(1))
                });
        }
    }

    fn publish(&self, shard_id: usize, payload: Vec<u8>) {
        let shard = &self.shards[shard_id];
        let mut subscribers = shard
            .subscribers
            .lock()
            .expect("monitor subscribers lock poisoned");
        subscribers.retain(|(_, sender)| sender.send(payload.clone()).is_ok());
        shard
            .count
            .store(subscribers.len() as u32, Ordering::Relaxed);
    }

    fn count(&self, shard_id: usize) -> u32 {
        self.shards[shard_id].count.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        for shard in &self.shards {
            shard
                .subscribers
                .lock()
                .expect("monitor subscribers lock poisoned")
                .clear();
            shard.count.store(0, Ordering::Relaxed);
        }
    }
}

fn arg(
    name: &'static str,
    arg_type: &'static str,
    token: &'static str,
    optional: bool,
    multiple: bool,
) -> DocArgument {
    DocArgument {
        name,
        arg_type,
        token,
        optional,
        multiple,
    }
}

fn keys_range(first: usize, last: isize, step: usize, flags: &'static [&'static str]) -> KeySpec {
    KeySpec {
        extractor: KeyExtractor::Range { first, last, step },
        flags,
    }
}

fn keys_every_other(start: usize, step: usize, flags: &'static [&'static str]) -> KeySpec {
    KeySpec {
        extractor: KeyExtractor::EveryOther { start, step },
        flags,
    }
}

fn keys_numkeys(count_index: usize, start: usize, flags: &'static [&'static str]) -> KeySpec {
    KeySpec {
        extractor: KeyExtractor::NumKeys { count_index, start },
        flags,
    }
}

fn bulk(text: &str) -> Response {
    Response::Value(Some(SenkoValue::from(Bytes::copy_from_slice(
        text.as_bytes(),
    ))))
}

fn bulk_bytes(text: &[u8]) -> Response {
    Response::Value(Some(SenkoValue::from(Bytes::copy_from_slice(text))))
}

fn parse_lower(bytes: &[u8]) -> Result<String, Vec<u8>> {
    std::str::from_utf8(bytes)
        .map(|text| text.to_ascii_lowercase())
        .map_err(|_| error_message("ERR syntax error"))
}

fn format_monitor_line(meta: &ConnectionMeta, command: &[u8], args: &[Frame<'_>]) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0);
    let mut out = format!(
        "+{ts:.6} [{} {}:{}] {}",
        meta.db,
        meta.peer_addr.ip(),
        meta.peer_addr.port(),
        quote_monitor_arg(command),
    );
    for arg in args {
        let bytes = frame_bytes(arg).unwrap_or_default();
        out.push(' ');
        out.push_str(&quote_monitor_arg(bytes));
    }
    out.push_str("\r\n");
    out
}

fn quote_monitor_arg(bytes: &[u8]) -> String {
    let mut out = String::from("\"");
    for ch in String::from_utf8_lossy(bytes).chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn outcome(response: Vec<u8>) -> CommandCommandOutcome {
    CommandCommandOutcome {
        response,
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(bytes: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(bytes)
    }

    #[test]
    fn command_count_is_large_enough() {
        init(&SenkoConfig::default());
        assert!(registry().metas.len() >= 100);
    }

    #[test]
    fn command_info_reports_get_and_set() {
        init(&SenkoConfig::default());
        let response = command_info(&[bs(b"get"), bs(b"set"), bs(b"nope")], true).unwrap();
        let rendered = String::from_utf8_lossy(&response.response);
        assert!(rendered.contains("readonly"));
        assert!(rendered.contains("write"));
        assert!(rendered.contains("$3\r\nget"));
        assert!(rendered.contains("$3\r\nset"));
        assert!(rendered.contains("_\r\n") || rendered.contains("$-1\r\n"));
    }

    #[test]
    fn command_getkeys_handles_set_mset_and_eval() {
        init(&SenkoConfig::default());
        let set = command_getkeys(&[bs(b"SET"), bs(b"foo"), bs(b"bar")], true, false).unwrap();
        assert!(String::from_utf8_lossy(&set.response).contains("foo"));

        let mset = command_getkeys(
            &[bs(b"MSET"), bs(b"a"), bs(b"1"), bs(b"b"), bs(b"2")],
            true,
            false,
        )
        .unwrap();
        let rendered = String::from_utf8_lossy(&mset.response);
        assert!(rendered.contains("a"));
        assert!(rendered.contains("b"));

        let eval = command_getkeys(
            &[
                bs(b"EVAL"),
                bs(b"return 1"),
                bs(b"2"),
                bs(b"k1"),
                bs(b"k2"),
                bs(b"arg"),
            ],
            true,
            false,
        )
        .unwrap();
        let rendered = String::from_utf8_lossy(&eval.response);
        assert!(rendered.contains("k1"));
        assert!(rendered.contains("k2"));
    }

    #[test]
    fn monitor_formats_set_line() {
        let meta = ConnectionMeta::for_acl_dryrun("default".into());
        let line = format_monitor_line(&meta, b"SET", &[bs(b"foo"), bs(b"bar")]);
        assert!(line.contains("\"SET\" \"foo\" \"bar\""));
        assert!(line.starts_with('+'));
    }
}
