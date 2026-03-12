use bytes::Bytes;
use mlua::{Lua, RegistryKey, Table, Value};
use phf::phf_set;

use crate::{
    error::LuaError,
    propagate::ScriptPropagation,
    script_cache::{hex_sha1, sha1_bytes},
};

pub static WRITE_COMMANDS: phf::Set<&'static str> = phf_set! {
    "SET", "SETNX", "SETEX", "PSETEX", "MSET", "MSETNX",
    "GETSET", "GETDEL", "GETEX",
    "APPEND", "INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY",
    "SETRANGE", "SETBIT",
    "HSET", "HMSET", "HSETNX", "HDEL", "HEXPIRE", "HPEXPIRE", "HPERSIST",
    "LPUSH", "LPUSHX", "RPUSH", "RPUSHX", "LINSERT", "LSET", "LREM",
    "LTRIM", "LPOP", "RPOP", "LMOVE", "LMPOP",
    "SADD", "SREM", "SMOVE", "SPOP", "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
    "ZADD", "ZREM", "ZINCRBY", "ZPOPMIN", "ZPOPMAX", "ZRANGESTORE",
    "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE",
    "XADD", "XDEL", "XTRIM", "XSETID", "XGROUP", "XCLAIM", "XAUTOCLAIM",
    "DEL", "UNLINK", "RENAME", "RENAMENX", "COPY", "MOVE",
    "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST",
    "SORT", "RESTORE", "FLUSHDB", "FLUSHALL",
    "BITOP", "BITFIELD",
    "PFADD", "PFMERGE",
    "GEOADD", "GEODELBYMEMBER",
};

pub static FORBIDDEN_COMMANDS: phf::Set<&'static str> = phf_set! {
    "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE",
    "PUBLISH", "WAIT", "WAITAOF", "MULTI", "EXEC", "DISCARD",
    "WATCH", "UNWATCH", "SCRIPT", "FUNCTION", "EVAL", "EVALSHA",
    "EVAL_RO", "EVALSHA_RO", "FCALL", "FCALL_RO"
};

pub fn register(lua: &Lua) -> Result<RegistryKey, LuaError> {
    let redis = lua.create_table()?;
    redis.set(
        "error_reply",
        lua.create_function(|lua, message: String| {
            let table = lua.create_table()?;
            table.set("err", message)?;
            Ok(table)
        })?,
    )?;
    redis.set(
        "status_reply",
        lua.create_function(|lua, message: String| {
            let table = lua.create_table()?;
            table.set("ok", message)?;
            Ok(table)
        })?,
    )?;
    redis.set(
        "sha1hex",
        lua.create_function(|_, input: mlua::String| {
            Ok(hex_sha1(&sha1_bytes(input.as_bytes().as_ref())))
        })?,
    )?;
    redis.set("replicate_commands", lua.create_function(|_, ()| Ok(true))?)?;
    redis.set("breakpoint", lua.create_function(|_, ()| Ok(()))?)?;
    redis.set("debug", lua.create_function(|_, _: Value| Ok(()))?)?;
    redis.set("LOG_DEBUG", 0)?;
    redis.set("LOG_VERBOSE", 1)?;
    redis.set("LOG_NOTICE", 2)?;
    redis.set("LOG_WARNING", 3)?;
    redis.set("REPL_NONE", ScriptPropagation::REPL_NONE)?;
    redis.set("REPL_AOF", ScriptPropagation::REPL_AOF)?;
    redis.set("REPL_REPLICA", ScriptPropagation::REPL_REPLICA)?;
    redis.set("REPL_SLAVE", ScriptPropagation::REPL_REPLICA)?;
    redis.set("REPL_ALL", ScriptPropagation::REPL_ALL)?;
    Ok(lua.create_registry_value(redis)?)
}

pub fn clone_table(lua: &Lua, key: &RegistryKey) -> Result<Table, LuaError> {
    let base: Table = lua.registry_value(key)?;
    let copy = lua.create_table()?;
    for pair in base.pairs::<Value, Value>() {
        let (name, value) = pair?;
        copy.set(name, value)?;
    }
    Ok(copy)
}

pub fn is_write_command(command: &str) -> bool {
    WRITE_COMMANDS.contains(command)
}

pub fn is_forbidden_command(command: &str) -> bool {
    FORBIDDEN_COMMANDS.contains(command)
}

pub fn normalize_command_args(command: &str, args: &mut [Bytes]) {
    if matches!(command, "XREAD" | "XREADGROUP") {
        let mut index = 0usize;
        while index + 1 < args.len() {
            if args[index].eq_ignore_ascii_case(b"BLOCK") {
                args[index + 1] = Bytes::from_static(b"0");
                break;
            }
            index += 1;
        }
    }
}
