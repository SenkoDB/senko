use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{Arc, Mutex},
    time::Instant,
};

use bytes::Bytes;
use mlua::{Function, HookTriggers, Lua, MultiValue, Table, Value, VmState};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use crate::{
    error::LuaError,
    functions::{
        FunctionDefinition, FunctionFlags, FunctionRegistry, LibraryInfo, RestoreMode,
        parse_flag_names, parse_shebang,
    },
    killer::ScriptKiller,
    propagate::ScriptPropagation,
    redis_api, sandbox,
    script_cache::{ScriptCache, parse_sha1_hex},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptKind {
    Eval,
    Function,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptDebugMode {
    No,
    Yes,
    Sync,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutingScript {
    pub kind: ScriptKind,
    pub name: String,
    pub start_time: Instant,
    pub is_readonly: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptExecution {
    pub kind: ScriptKind,
    pub name: String,
    pub readonly: bool,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    Simple(Bytes),
    Error(Bytes),
    Bulk(Option<Bytes>),
    Integer(i64),
    Array(Vec<RespValue>),
    Map(Vec<(RespValue, RespValue)>),
}

#[derive(Debug, Clone)]
pub struct ScriptingConfig {
    pub max_depth: u32,
    pub time_limit_ms: u64,
}

impl Default for ScriptingConfig {
    fn default() -> Self {
        Self {
            max_depth: 1,
            time_limit_ms: 5_000,
        }
    }
}

pub trait ScriptExecutionHooks {
    fn dispatch(&mut self, command: &[u8], args: &[Bytes]) -> Result<RespValue, LuaError>;

    fn acl_check(
        &mut self,
        _username: &str,
        _command: &[u8],
        _args: &[Bytes],
    ) -> Result<(), LuaError> {
        Ok(())
    }

    fn log(&mut self, _level: i64, _message: &str) {}

    fn script_completed(&mut self, _committed: bool) -> Result<(), LuaError> {
        Ok(())
    }
}

pub struct ScriptContext<'a> {
    pub keys: &'a [Bytes],
    pub args: &'a [Bytes],
    pub readonly: bool,
    pub db_id: u8,
    pub username: &'a str,
    pub hooks: &'a mut dyn ScriptExecutionHooks,
}

#[derive(Debug)]
struct HookState {
    active: bool,
    start_time: Option<Instant>,
    time_limit_ms: u64,
    readonly: bool,
    has_written: bool,
    busy: bool,
}

impl Default for HookState {
    fn default() -> Self {
        Self {
            active: false,
            start_time: None,
            time_limit_ms: 0,
            readonly: false,
            has_written: false,
            busy: false,
        }
    }
}

pub struct LuaEngine {
    lua: Lua,
    redis_key: mlua::RegistryKey,
    pub script_cache: ScriptCache,
    pub functions: FunctionRegistry,
    pub killer: Arc<ScriptKiller>,
    pub call_depth: u32,
    pub max_depth: u32,
    pub time_limit_ms: u64,
    pub executing: Option<ExecutingScript>,
    debug_mode: ScriptDebugMode,
    hook_state: Arc<Mutex<HookState>>,
}

impl LuaEngine {
    pub fn new(config: &ScriptingConfig) -> Result<Self, LuaError> {
        let lua = Lua::new();
        register_stdlib(&lua)?;
        sandbox::apply(&lua)?;
        let redis_key = redis_api::register(&lua)?;
        let hook_state = Arc::new(Mutex::new(HookState::default()));
        let killer = Arc::new(ScriptKiller::default());
        register_hooks(&lua, Arc::clone(&hook_state), Arc::clone(&killer))?;
        let base: Table = lua.registry_value(&redis_key)?;
        lua.globals().set("redis", base.clone())?;
        lua.globals().set("senko", base.clone())?;
        lua.globals().set("senkou", base)?;
        Ok(Self {
            lua,
            redis_key,
            script_cache: ScriptCache::default(),
            functions: FunctionRegistry::default(),
            killer,
            call_depth: 0,
            max_depth: config.max_depth,
            time_limit_ms: config.time_limit_ms,
            executing: None,
            debug_mode: ScriptDebugMode::No,
            hook_state,
        })
    }

    pub fn eval(&mut self, script: &str, ctx: ScriptContext<'_>) -> Result<RespValue, LuaError> {
        let sha = self.script_cache.load(&self.lua, script)?;
        let sha_bytes = parse_sha1_hex(&sha)?;
        let chunk = self
            .script_cache
            .get(&self.lua, &sha_bytes)?
            .ok_or(LuaError::NoScript)?;
        self.execute_script(chunk, ScriptKind::Eval, sha, ctx, false)
    }

    pub fn evalsha(&mut self, sha1: &str, ctx: ScriptContext<'_>) -> Result<RespValue, LuaError> {
        let sha_bytes = parse_sha1_hex(sha1)?;
        let chunk = self
            .script_cache
            .get(&self.lua, &sha_bytes)?
            .ok_or(LuaError::NoScript)?;
        self.execute_script(chunk, ScriptKind::Eval, sha1.to_owned(), ctx, false)
    }

    pub fn fcall(
        &mut self,
        function_name: &str,
        ctx: ScriptContext<'_>,
    ) -> Result<RespValue, LuaError> {
        let (library, function, flags, callback) = self
            .functions
            .get_function(&self.lua, function_name)?
            .ok_or(LuaError::FunctionNotFound)?;
        if ctx.readonly && !flags.contains(FunctionFlags::NO_WRITES) {
            return Err(LuaError::redis_error(
                "Can not execute a script with write flag using fcall_ro.",
            ));
        }
        self.execute_script(
            callback,
            ScriptKind::Function,
            format!("{library}.{function}"),
            ctx,
            true,
        )
    }

    pub fn script_load(&mut self, script: &str) -> Result<String, LuaError> {
        self.script_cache.load(&self.lua, script)
    }

    pub fn script_exists(&self, sha1s: &[&str]) -> Vec<bool> {
        self.script_cache.exists(sha1s)
    }

    pub fn script_flush(&mut self) -> Result<(), LuaError> {
        self.script_cache.flush(&self.lua)
    }

    pub fn function_load(&mut self, source: &str, replace: bool) -> Result<(), LuaError> {
        let (library_name, body) = parse_shebang(source)?;
        let definitions = self.collect_function_definitions(body.as_str())?;
        self.functions
            .load_library(&self.lua, &library_name, source, definitions, replace)
    }

    pub fn function_list(&self, pattern: Option<&[u8]>, with_code: bool) -> Vec<LibraryInfo> {
        self.functions.list(pattern, with_code)
    }

    pub fn function_delete(&mut self, library_name: &str) -> Result<(), LuaError> {
        self.functions.delete(&self.lua, library_name)
    }

    pub fn function_flush(&mut self) -> Result<(), LuaError> {
        self.functions.flush(&self.lua)
    }

    pub fn function_dump(&self) -> Bytes {
        self.functions.dump()
    }

    pub fn function_restore(&mut self, payload: &[u8], mode: RestoreMode) -> Result<(), LuaError> {
        let redis_key = &self.redis_key;
        self.functions.restore(
            &self.lua,
            payload,
            |lua, _name, code, _replace| {
                let body = parse_shebang(code)
                    .map(|(_, body)| body)
                    .unwrap_or_else(|_| code.to_owned());
                collect_function_definitions(lua, redis_key, body.as_str())
            },
            mode,
        )
    }

    pub fn function_stats(&self, command: Option<Vec<Bytes>>) -> RespValue {
        let running = match &self.executing {
            Some(executing) if executing.kind == ScriptKind::Function => RespValue::Map(vec![
                (
                    RespValue::Bulk(Some(Bytes::from_static(b"name"))),
                    RespValue::Bulk(Some(Bytes::copy_from_slice(executing.name.as_bytes()))),
                ),
                (
                    RespValue::Bulk(Some(Bytes::from_static(b"command"))),
                    RespValue::Array(
                        command
                            .unwrap_or_default()
                            .into_iter()
                            .map(|part| RespValue::Bulk(Some(part)))
                            .collect(),
                    ),
                ),
                (
                    RespValue::Bulk(Some(Bytes::from_static(b"duration_ms"))),
                    RespValue::Integer(executing.start_time.elapsed().as_millis() as i64),
                ),
            ]),
            _ => RespValue::Bulk(None),
        };
        RespValue::Map(vec![
            (
                RespValue::Bulk(Some(Bytes::from_static(b"running_script"))),
                running,
            ),
            (
                RespValue::Bulk(Some(Bytes::from_static(b"engines"))),
                RespValue::Map(vec![(
                    RespValue::Bulk(Some(Bytes::from_static(b"LUA"))),
                    RespValue::Map(vec![
                        (
                            RespValue::Bulk(Some(Bytes::from_static(b"libraries_count"))),
                            RespValue::Integer(self.functions.library_count() as i64),
                        ),
                        (
                            RespValue::Bulk(Some(Bytes::from_static(b"functions_count"))),
                            RespValue::Integer(self.functions.function_count() as i64),
                        ),
                    ]),
                )]),
            ),
        ])
    }

    pub fn request_kill(&self) -> Result<(), LuaError> {
        if self.executing.is_none() {
            return Err(LuaError::NotBusy);
        }
        let state = self.hook_state.lock().expect("hook state lock poisoned");
        if state.has_written && !state.readonly {
            return Err(LuaError::KillDenied);
        }
        drop(state);
        self.killer.request_kill();
        Ok(())
    }

    pub fn set_debug_mode(&mut self, mode: ScriptDebugMode) {
        self.debug_mode = mode;
    }

    fn execute_script(
        &mut self,
        chunk: Function,
        kind: ScriptKind,
        name: String,
        mut ctx: ScriptContext<'_>,
        pass_keys_args: bool,
    ) -> Result<RespValue, LuaError> {
        let start_time = Instant::now();
        self.call_depth = 0;
        self.killer.reset();
        self.executing = Some(ExecutingScript {
            kind,
            name: name.clone(),
            start_time,
            is_readonly: ctx.readonly,
        });
        {
            let mut state = self.hook_state.lock().expect("hook state lock poisoned");
            state.active = true;
            state.start_time = Some(start_time);
            state.time_limit_ms = self.time_limit_ms;
            state.readonly = ctx.readonly;
            state.has_written = false;
            state.busy = false;
        }

        let propagation = Rc::new(RefCell::new(ScriptPropagation::default()));
        let command_depth = Rc::new(Cell::new(0u32));
        let result = self.execute_scoped(
            chunk,
            &name,
            &mut ctx,
            pass_keys_args,
            Rc::clone(&propagation),
            Rc::clone(&command_depth),
        );
        let committed = result.is_ok();
        let hooks_result = ctx.hooks.script_completed(committed);

        {
            let mut state = self.hook_state.lock().expect("hook state lock poisoned");
            state.active = false;
            state.start_time = None;
            state.has_written = false;
        }
        self.executing = None;
        self.call_depth = 0;
        match result {
            Ok(value) => {
                hooks_result?;
                let _ = propagation;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    fn execute_scoped(
        &mut self,
        chunk: Function,
        _name: &str,
        ctx: &mut ScriptContext<'_>,
        pass_keys_args: bool,
        propagation: Rc<RefCell<ScriptPropagation>>,
        command_depth: Rc<Cell<u32>>,
    ) -> Result<RespValue, LuaError> {
        let globals = self.lua.globals();
        let keys_table = create_string_table(&self.lua, ctx.keys)?;
        let args_table = create_string_table(&self.lua, ctx.args)?;
        let old_keys: Value = globals.get("KEYS").unwrap_or(Value::Nil);
        let old_argv: Value = globals.get("ARGV").unwrap_or(Value::Nil);
        let old_redis: Value = globals.get("redis").unwrap_or(Value::Nil);
        let old_senko: Value = globals.get("senko").unwrap_or(Value::Nil);
        let old_senkou: Value = globals.get("senkou").unwrap_or(Value::Nil);

        let hooks_ref: &mut dyn ScriptExecutionHooks = &mut *ctx.hooks;
        let hooks = Rc::new(RefCell::new(hooks_ref));
        let username = ctx.username.to_owned();
        let readonly = ctx.readonly;
        let db_id = ctx.db_id;
        let max_depth = self.max_depth;
        let hook_state = Arc::clone(&self.hook_state);
        let lua_ref = &self.lua;

        let scope_result = self
            .lua
            .scope(
                |scope: &mut mlua::Scope<'_, '_>| -> mlua::Result<RespValue> {
                    let result = (|| -> Result<RespValue, LuaError> {
                        let redis = redis_api::clone_table(&self.lua, &self.redis_key)?;
                        let call_username = username.clone();
                        let pcall_username = username.clone();
                        let call_hooks = Rc::clone(&hooks);
                        let pcall_hooks = Rc::clone(&hooks);
                        let log_hooks = Rc::clone(&hooks);
                        let acl_hooks = Rc::clone(&hooks);
                        let call_propagation = Rc::clone(&propagation);
                        let pcall_propagation = Rc::clone(&propagation);
                        let repl_propagation = Rc::clone(&propagation);
                        let call_depth_ref = Rc::clone(&command_depth);
                        let pcall_depth_ref = Rc::clone(&command_depth);
                        let call_hook_state = Arc::clone(&hook_state);
                        let pcall_hook_state = Arc::clone(&hook_state);

                        redis.set(
                            "call",
                            scope.create_function_mut(move |_, values: MultiValue| {
                                dispatch_script_call(
                                    lua_ref,
                                    true,
                                    values,
                                    &call_hooks,
                                    call_username.as_str(),
                                    readonly,
                                    db_id,
                                    max_depth,
                                    &call_depth_ref,
                                    &call_propagation,
                                    &call_hook_state,
                                )
                            })?,
                        )?;
                        redis.set(
                            "pcall",
                            scope.create_function_mut(move |_, values: MultiValue| {
                                dispatch_script_call(
                                    lua_ref,
                                    false,
                                    values,
                                    &pcall_hooks,
                                    pcall_username.as_str(),
                                    readonly,
                                    db_id,
                                    max_depth,
                                    &pcall_depth_ref,
                                    &pcall_propagation,
                                    &pcall_hook_state,
                                )
                            })?,
                        )?;
                        redis.set(
                            "log",
                            scope.create_function_mut(
                                move |_, (level, message): (i64, String)| {
                                    log_hooks.borrow_mut().log(level, message.as_str());
                                    Ok(())
                                },
                            )?,
                        )?;
                        redis.set(
                            "set_repl",
                            scope.create_function_mut(move |_, flags: u8| {
                                repl_propagation.borrow_mut().set_flags(flags);
                                Ok(())
                            })?,
                        )?;
                        redis.set(
                            "acl_check_cmd",
                            scope.create_function_mut(move |lua, values: MultiValue| {
                                let mut parts = values.into_iter();
                                let Some(username_value) = parts.next() else {
                                    return error_table(lua, "ERR wrong number of arguments")
                                        .map(Value::Table)
                                        .map_err(mlua::Error::external);
                                };
                                let Some(command_value) = parts.next() else {
                                    return error_table(lua, "ERR wrong number of arguments")
                                        .map(Value::Table)
                                        .map_err(mlua::Error::external);
                                };
                                let username = lua_value_to_string(username_value)?;
                                let command = lua_value_to_string(command_value)?;
                                let args = parts
                                    .map(lua_value_to_bytes)
                                    .collect::<Result<Vec<_>, _>>()
                                    .map_err(mlua::Error::external)?;
                                match acl_hooks.borrow_mut().acl_check(
                                    username.as_str(),
                                    command.as_bytes(),
                                    &args,
                                ) {
                                    Ok(()) => Ok(Value::Boolean(true)),
                                    Err(error) => error_table(lua, &error.client_message())
                                        .map(Value::Table)
                                        .map_err(mlua::Error::external),
                                }
                            })?,
                        )?;
                        globals.set("redis", redis.clone())?;
                        globals.set("senko", redis.clone())?;
                        globals.set("senkou", redis)?;
                        globals.set("KEYS", keys_table.clone())?;
                        globals.set("ARGV", args_table.clone())?;

                        let lua_value = if pass_keys_args {
                            chunk.call::<Value>((keys_table, args_table))?
                        } else {
                            chunk.call::<Value>(())?
                        };
                        lua_to_resp(lua_value)
                    })();
                    result.map_err(mlua::Error::external)
                },
            )
            .map_err(LuaError::from);

        globals.set("KEYS", old_keys)?;
        globals.set("ARGV", old_argv)?;
        globals.set("redis", old_redis)?;
        globals.set("senko", old_senko)?;
        globals.set("senkou", old_senkou)?;
        scope_result
    }

    fn collect_function_definitions(
        &self,
        body: &str,
    ) -> Result<Vec<FunctionDefinition>, LuaError> {
        collect_function_definitions(&self.lua, &self.redis_key, body)
    }
}

fn register_hooks(
    lua: &Lua,
    hook_state: Arc<Mutex<HookState>>,
    killer: Arc<ScriptKiller>,
) -> Result<(), LuaError> {
    lua.set_hook(
        HookTriggers {
            every_nth_instruction: Some(10_000),
            ..HookTriggers::default()
        },
        move |_lua, _debug| {
            let mut state = hook_state.lock().expect("hook state lock poisoned");
            if state.active {
                if killer.is_kill_requested() {
                    if state.has_written && !state.readonly {
                        killer.reset();
                        return Ok(VmState::Continue);
                    }
                    killer.mark_aborted();
                    return Err(mlua::Error::runtime(
                        LuaError::ScriptKilled.client_message(),
                    ));
                }
                if let Some(start_time) = state.start_time
                    && state.time_limit_ms > 0
                    && start_time.elapsed().as_millis() > u128::from(state.time_limit_ms)
                {
                    state.busy = true;
                }
            }
            Ok(VmState::Continue)
        },
    );
    Ok(())
}

fn register_stdlib(lua: &Lua) -> Result<(), LuaError> {
    let globals = lua.globals();
    globals.set("cjson", build_cjson(lua)?)?;
    globals.set("cmsgpack", build_cmsgpack(lua)?)?;
    globals.set("bit", build_bit(lua)?)?;
    globals.set("struct", build_struct(lua)?)?;
    Ok(())
}

fn build_cjson(lua: &Lua) -> Result<Table, LuaError> {
    let table = lua.create_table()?;
    table.set(
        "encode",
        lua.create_function(|_, value: Value| {
            let json = lua_to_json(value)?;
            serde_json::to_string(&json).map_err(mlua::Error::external)
        })?,
    )?;
    table.set(
        "decode",
        lua.create_function(|lua, input: String| {
            let json: JsonValue =
                serde_json::from_str(input.as_str()).map_err(mlua::Error::external)?;
            json_to_lua(lua, &json)
        })?,
    )?;
    table.set(
        "new",
        lua.create_function(|lua, ()| build_cjson(lua).map_err(mlua::Error::external))?,
    )?;
    Ok(table)
}

fn build_cmsgpack(lua: &Lua) -> Result<Table, LuaError> {
    let table = lua.create_table()?;
    table.set(
        "pack",
        lua.create_function(|lua, value: Value| {
            let json = lua_to_json(value)?;
            let bytes = rmp_serde::to_vec(&json).map_err(mlua::Error::external)?;
            lua.create_string(bytes)
        })?,
    )?;
    table.set(
        "unpack",
        lua.create_function(|lua, input: mlua::String| {
            let json: JsonValue =
                rmp_serde::from_slice(input.as_bytes().as_ref()).map_err(mlua::Error::external)?;
            json_to_lua(lua, &json)
        })?,
    )?;
    Ok(table)
}

fn build_bit(lua: &Lua) -> Result<Table, LuaError> {
    let bit = lua.create_table()?;
    bit.set(
        "band",
        lua.create_function(|_, values: MultiValue| fold_bits(values, !0u32, |a, b| a & b))?,
    )?;
    bit.set(
        "bor",
        lua.create_function(|_, values: MultiValue| fold_bits(values, 0u32, |a, b| a | b))?,
    )?;
    bit.set(
        "bxor",
        lua.create_function(|_, values: MultiValue| fold_bits(values, 0u32, |a, b| a ^ b))?,
    )?;
    bit.set(
        "bnot",
        lua.create_function(|_, value: i64| Ok((!(value as i32)) as i64))?,
    )?;
    bit.set(
        "lshift",
        lua.create_function(|_, (value, shift): (i64, i64)| {
            Ok(((value as u32) << (shift as u32 & 31)) as i32 as i64)
        })?,
    )?;
    bit.set(
        "rshift",
        lua.create_function(|_, (value, shift): (i64, i64)| {
            Ok(((value as u32) >> (shift as u32 & 31)) as i64)
        })?,
    )?;
    bit.set(
        "arshift",
        lua.create_function(|_, (value, shift): (i64, i64)| {
            Ok(((value as i32) >> (shift as u32 & 31)) as i64)
        })?,
    )?;
    bit.set(
        "tobit",
        lua.create_function(|_, value: i64| Ok((value as i32) as i64))?,
    )?;
    bit.set(
        "tohex",
        lua.create_function(|_, (value, digits): (i64, Option<i64>)| {
            let digits = digits.unwrap_or(8).clamp(1, 8) as usize;
            Ok(format!("{:01$x}", value as u32, digits))
        })?,
    )?;
    Ok(bit)
}

fn build_struct(lua: &Lua) -> Result<Table, LuaError> {
    let table = lua.create_table()?;
    table.set(
        "pack",
        lua.create_function(|lua, values: MultiValue| {
            let mut iter = values.into_iter();
            let Some(format) = iter.next() else {
                return Err(mlua::Error::runtime("missing format"));
            };
            let format = lua_value_to_string(format)?;
            let args = iter.collect::<Vec<_>>();
            let packed = struct_pack(format.as_str(), &args)?;
            lua.create_string(packed)
        })?,
    )?;
    table.set(
        "unpack",
        lua.create_function(
            |lua, (format, input, offset): (String, mlua::String, Option<usize>)| {
                let values = struct_unpack(
                    lua,
                    format.as_str(),
                    input.as_bytes().as_ref(),
                    offset.unwrap_or(1),
                )?;
                let mut out = MultiValue::new();
                for value in values {
                    out.push_back(value);
                }
                Ok(out)
            },
        )?,
    )?;
    Ok(table)
}

fn collect_function_definitions(
    lua: &Lua,
    redis_key: &mlua::RegistryKey,
    body: &str,
) -> Result<Vec<FunctionDefinition>, LuaError> {
    let registrations = Rc::new(RefCell::new(Vec::<FunctionDefinition>::new()));
    let collected = lua
        .scope(
            |scope: &mut mlua::Scope<'_, '_>| -> mlua::Result<Vec<FunctionDefinition>> {
                let result = (|| -> Result<Vec<FunctionDefinition>, LuaError> {
                    let globals = lua.globals();
                    let old_redis: Value = globals.get("redis").unwrap_or(Value::Nil);
                    let old_senko: Value = globals.get("senko").unwrap_or(Value::Nil);
                    let old_senkou: Value = globals.get("senkou").unwrap_or(Value::Nil);
                    let redis = redis_api::clone_table(lua, redis_key)?;
                    redis.set(
                        "register_function",
                        scope.create_function_mut({
                            let registrations = Rc::clone(&registrations);
                            move |_, values: MultiValue| {
                                let definition = parse_registration(values)?;
                                registrations.borrow_mut().push(definition);
                                Ok(())
                            }
                        })?,
                    )?;
                    globals.set("redis", redis.clone())?;
                    globals.set("senko", redis.clone())?;
                    globals.set("senkou", redis)?;
                    let result = lua.load(body).exec().map_err(LuaError::from);
                    globals.set("redis", old_redis)?;
                    globals.set("senko", old_senko)?;
                    globals.set("senkou", old_senkou)?;
                    result?;
                    Ok(std::mem::take(&mut *registrations.borrow_mut()))
                })();
                result.map_err(mlua::Error::external)
            },
        )
        .map_err(LuaError::from)?;
    Ok(collected)
}

fn create_string_table(lua: &Lua, values: &[Bytes]) -> Result<Table, LuaError> {
    let table = lua.create_table()?;
    for (index, value) in values.iter().enumerate() {
        table.set(index + 1, lua.create_string(value)?)?;
    }
    Ok(table)
}

fn parse_registration(values: MultiValue) -> Result<FunctionDefinition, mlua::Error> {
    let parts = values.into_vec();
    match parts.as_slice() {
        [Value::String(name), Value::Function(callback)] => Ok(FunctionDefinition {
            name: String::from_utf8_lossy(name.as_bytes().as_ref()).into_owned(),
            description: None,
            flags: FunctionFlags::empty(),
            callback: callback.clone(),
        }),
        [Value::Table(options)] => {
            let name: String = options.get("function_name")?;
            let callback: Function = options.get("callback")?;
            let description: Option<String> = options.get("description").ok();
            let flags = if let Ok(flags_table) = options.get::<Table>("flags") {
                let flags = flags_table
                    .sequence_values::<String>()
                    .collect::<Result<Vec<_>, _>>()?;
                parse_flag_names(&flags).map_err(mlua::Error::external)?
            } else {
                FunctionFlags::empty()
            };
            Ok(FunctionDefinition {
                name,
                description,
                flags,
                callback,
            })
        }
        _ => Err(mlua::Error::runtime("invalid redis.register_function call")),
    }
}

fn lua_to_resp(value: Value) -> Result<RespValue, LuaError> {
    match value {
        Value::Nil => Ok(RespValue::Bulk(None)),
        Value::Boolean(false) => Ok(RespValue::Bulk(None)),
        Value::Boolean(true) => Ok(RespValue::Integer(1)),
        Value::Integer(value) => Ok(RespValue::Integer(value)),
        Value::Number(value) => Ok(RespValue::Integer(value.trunc() as i64)),
        Value::String(value) => Ok(RespValue::Bulk(Some(Bytes::copy_from_slice(
            value.as_bytes().as_ref(),
        )))),
        Value::Table(table) => table_to_resp(table),
        other => Ok(RespValue::Bulk(Some(Bytes::from(safe_display(other))))),
    }
}

fn table_to_resp(table: Table) -> Result<RespValue, LuaError> {
    if let Ok(ok) = table.get::<String>("ok") {
        return Ok(RespValue::Simple(Bytes::from(ok)));
    }
    if let Ok(err) = table.get::<String>("err") {
        return Ok(RespValue::Error(Bytes::from(err)));
    }
    let len = table.raw_len();
    if len > 0 {
        let mut values = Vec::with_capacity(len);
        for index in 1..=len {
            values.push(lua_to_resp(table.raw_get(index)?)?);
        }
        return Ok(RespValue::Array(values));
    }
    let mut map = Vec::new();
    for pair in table.pairs::<Value, Value>() {
        let (key, value) = pair?;
        map.push((lua_to_resp(key)?, lua_to_resp(value)?));
    }
    Ok(RespValue::Map(map))
}

fn resp_to_lua(lua: &Lua, value: RespValue) -> Result<Value, LuaError> {
    match value {
        RespValue::Simple(value) => Ok(Value::Table(status_table(lua, value.as_ref())?)),
        RespValue::Error(value) => Ok(Value::Table(error_table(
            lua,
            &String::from_utf8_lossy(value.as_ref()),
        )?)),
        RespValue::Bulk(Some(value)) => Ok(Value::String(lua.create_string(value)?)),
        RespValue::Bulk(None) => Ok(Value::Boolean(false)),
        RespValue::Integer(value) => Ok(Value::Integer(value)),
        RespValue::Array(values) => {
            let table = lua.create_table()?;
            for (index, value) in values.into_iter().enumerate() {
                table.set(index + 1, resp_to_lua(lua, value)?)?;
            }
            Ok(Value::Table(table))
        }
        RespValue::Map(values) => {
            let table = lua.create_table()?;
            let mut index = 1usize;
            for (key, value) in values {
                table.set(index, resp_to_lua(lua, key)?)?;
                index += 1;
                table.set(index, resp_to_lua(lua, value)?)?;
                index += 1;
            }
            Ok(Value::Table(table))
        }
    }
}

fn dispatch_script_call(
    lua: &Lua,
    raise_errors: bool,
    values: MultiValue,
    hooks: &Rc<RefCell<&mut dyn ScriptExecutionHooks>>,
    username: &str,
    readonly: bool,
    db_id: u8,
    max_depth: u32,
    command_depth: &Rc<Cell<u32>>,
    propagation: &Rc<RefCell<ScriptPropagation>>,
    hook_state: &Arc<Mutex<HookState>>,
) -> Result<Value, mlua::Error> {
    let mut parts = values.into_iter();
    let Some(command) = parts.next() else {
        return Err(mlua::Error::runtime("wrong number of arguments"));
    };
    let command = lua_value_to_string(command)?;
    let command_upper = command.to_ascii_uppercase();
    if redis_api::is_forbidden_command(command_upper.as_str()) {
        return Err(mlua::Error::runtime(
            LuaError::ForbiddenCommand(command_upper).client_message(),
        ));
    }
    if readonly && redis_api::is_write_command(command_upper.as_str()) {
        return Err(mlua::Error::runtime(
            LuaError::ReadonlyViolation(command_upper).client_message(),
        ));
    }
    let depth = command_depth.get().saturating_add(1);
    if depth > max_depth {
        return Err(mlua::Error::runtime("ERR script recursion depth exceeded"));
    }
    command_depth.set(depth);
    let mut args = parts
        .map(lua_value_to_bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(mlua::Error::external)?;
    redis_api::normalize_command_args(command_upper.as_str(), &mut args);
    if let Err(error) = hooks
        .borrow_mut()
        .acl_check(username, command_upper.as_bytes(), &args)
    {
        command_depth.set(depth.saturating_sub(1));
        return if raise_errors {
            Err(mlua::Error::runtime(error.client_message()))
        } else {
            error_table(lua, &error.client_message())
                .map(Value::Table)
                .map_err(mlua::Error::external)
        };
    }
    let response = hooks.borrow_mut().dispatch(command_upper.as_bytes(), &args);
    command_depth.set(depth.saturating_sub(1));
    match response {
        Ok(response) => {
            if redis_api::is_write_command(command_upper.as_str()) {
                propagation.borrow_mut().push(
                    db_id,
                    &std::iter::once(Bytes::copy_from_slice(command_upper.as_bytes()))
                        .chain(args.iter().cloned())
                        .collect::<Vec<_>>(),
                );
                let mut state = hook_state.lock().expect("hook state lock poisoned");
                state.has_written = true;
            }
            resp_to_lua(lua, response).map_err(mlua::Error::external)
        }
        Err(error) => {
            if raise_errors {
                Err(mlua::Error::runtime(error.client_message()))
            } else {
                error_table(lua, &error.client_message())
                    .map(Value::Table)
                    .map_err(mlua::Error::external)
            }
        }
    }
}

fn status_table(lua: &Lua, status: &[u8]) -> Result<Table, LuaError> {
    let table = lua.create_table()?;
    table.set("ok", String::from_utf8_lossy(status).into_owned())?;
    Ok(table)
}

fn error_table(lua: &Lua, error: &str) -> Result<Table, LuaError> {
    let table = lua.create_table()?;
    table.set("err", error)?;
    Ok(table)
}

fn lua_value_to_string(value: Value) -> Result<String, mlua::Error> {
    match value {
        Value::String(value) => Ok(String::from_utf8_lossy(value.as_bytes().as_ref()).into_owned()),
        Value::Integer(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Boolean(value) => Ok(if value { "1" } else { "0" }.to_owned()),
        other => Err(mlua::Error::runtime(format!(
            "ERR Lua redis() command arguments must be strings or integers, found {}",
            safe_display(other)
        ))),
    }
}

fn lua_value_to_bytes(value: Value) -> Result<Bytes, LuaError> {
    match value {
        Value::String(value) => Ok(Bytes::copy_from_slice(value.as_bytes().as_ref())),
        Value::Integer(value) => Ok(Bytes::from(value.to_string())),
        Value::Number(value) => Ok(Bytes::from(value.to_string())),
        Value::Boolean(value) => Ok(Bytes::from(if value { "1" } else { "0" })),
        other => Err(LuaError::redis_error(format!(
            "Lua redis() command arguments must be strings or integers, found {}",
            safe_display(other)
        ))),
    }
}

fn safe_display(value: Value) -> String {
    match value {
        Value::Nil => "nil".to_owned(),
        Value::Boolean(value) => value.to_string(),
        Value::LightUserData(_) => "lightuserdata".to_owned(),
        Value::Integer(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => String::from_utf8_lossy(value.as_bytes().as_ref()).into_owned(),
        Value::Table(_) => "table".to_owned(),
        Value::Function(_) => "function".to_owned(),
        Value::Thread(_) => "thread".to_owned(),
        Value::UserData(_) => "userdata".to_owned(),
        Value::Error(error) => error.to_string(),
        Value::Other(_) => "value".to_owned(),
    }
}

fn lua_to_json(value: Value) -> Result<JsonValue, mlua::Error> {
    match value {
        Value::Nil => Ok(JsonValue::Null),
        Value::Boolean(value) => Ok(JsonValue::Bool(value)),
        Value::Integer(value) => Ok(JsonValue::Number(JsonNumber::from(value))),
        Value::Number(value) => JsonNumber::from_f64(value)
            .map(JsonValue::Number)
            .ok_or_else(|| mlua::Error::runtime("invalid number")),
        Value::String(value) => Ok(JsonValue::String(
            String::from_utf8_lossy(value.as_bytes().as_ref()).into_owned(),
        )),
        Value::Table(table) => table_to_json(table),
        _ => Err(mlua::Error::runtime("unsupported value")),
    }
}

fn table_to_json(table: Table) -> Result<JsonValue, mlua::Error> {
    let len = table.raw_len();
    if len > 0 {
        let mut out = Vec::with_capacity(len);
        for index in 1..=len {
            out.push(lua_to_json(table.raw_get(index)?)?);
        }
        return Ok(JsonValue::Array(out));
    }
    let mut out = JsonMap::new();
    for pair in table.pairs::<Value, Value>() {
        let (key, value) = pair?;
        let key = match key {
            Value::String(value) => String::from_utf8_lossy(value.as_bytes().as_ref()).into_owned(),
            Value::Integer(value) => value.to_string(),
            Value::Number(value) => value.to_string(),
            _ => return Err(mlua::Error::runtime("unsupported object key")),
        };
        out.insert(key, lua_to_json(value)?);
    }
    Ok(JsonValue::Object(out))
}

fn json_to_lua(lua: &Lua, value: &JsonValue) -> Result<Value, mlua::Error> {
    match value {
        JsonValue::Null => Ok(Value::Nil),
        JsonValue::Bool(value) => Ok(Value::Boolean(*value)),
        JsonValue::Number(value) => {
            if let Some(integer) = value.as_i64() {
                Ok(Value::Integer(integer))
            } else {
                Ok(Value::Number(value.as_f64().unwrap_or_default()))
            }
        }
        JsonValue::String(value) => Ok(Value::String(lua.create_string(value)?)),
        JsonValue::Array(values) => {
            let table = lua.create_table()?;
            for (index, value) in values.iter().enumerate() {
                table.set(index + 1, json_to_lua(lua, value)?)?;
            }
            Ok(Value::Table(table))
        }
        JsonValue::Object(values) => {
            let table = lua.create_table()?;
            for (key, value) in values {
                table.set(key.as_str(), json_to_lua(lua, value)?)?;
            }
            Ok(Value::Table(table))
        }
    }
}

fn fold_bits(
    values: MultiValue,
    initial: u32,
    combine: fn(u32, u32) -> u32,
) -> Result<i64, mlua::Error> {
    let mut acc = initial;
    for value in values {
        acc = combine(
            acc,
            lua_value_to_string(value)?
                .parse::<u32>()
                .unwrap_or_default(),
        );
    }
    Ok(acc as i32 as i64)
}

fn struct_pack(format: &str, args: &[Value]) -> Result<Vec<u8>, mlua::Error> {
    let mut parser = StructFormat::new(format);
    let mut out = Vec::new();
    let mut arg_index = 0usize;
    while let Some(token) = parser.next()? {
        match token {
            StructToken::Pad(count) => out.resize(out.len() + count, 0),
            StructToken::Bytes(count) => {
                let value = args
                    .get(arg_index)
                    .ok_or_else(|| mlua::Error::runtime("missing struct argument"))?;
                arg_index += 1;
                let bytes = lua_value_to_bytes(value.clone()).map_err(mlua::Error::external)?;
                if count == 1 {
                    out.push(*bytes.first().unwrap_or(&0));
                } else {
                    let mut value = bytes.to_vec();
                    value.resize(count, 0);
                    out.extend_from_slice(&value[..count]);
                }
            }
            StructToken::Int { signed, size } => {
                let value = args
                    .get(arg_index)
                    .ok_or_else(|| mlua::Error::runtime("missing struct argument"))?;
                arg_index += 1;
                write_int(&mut out, parser.endian, signed, size, value)?;
            }
            StructToken::Float { size } => {
                let value = args
                    .get(arg_index)
                    .ok_or_else(|| mlua::Error::runtime("missing struct argument"))?;
                arg_index += 1;
                write_float(&mut out, parser.endian, size, value)?;
            }
        }
    }
    Ok(out)
}

fn struct_unpack(
    lua: &Lua,
    format: &str,
    input: &[u8],
    offset: usize,
) -> Result<Vec<Value>, mlua::Error> {
    let mut parser = StructFormat::new(format);
    let mut cursor = offset.saturating_sub(1);
    let mut out = Vec::new();
    while let Some(token) = parser.next()? {
        match token {
            StructToken::Pad(count) => cursor = cursor.saturating_add(count),
            StructToken::Bytes(count) => {
                let end = cursor.saturating_add(count);
                if end > input.len() {
                    return Err(mlua::Error::runtime("unpack overflow"));
                }
                let value = &input[cursor..end];
                cursor = end;
                out.push(Value::String(lua.create_string(value)?));
            }
            StructToken::Int { signed, size } => {
                let end = cursor.saturating_add(size);
                if end > input.len() {
                    return Err(mlua::Error::runtime("unpack overflow"));
                }
                let value = read_int(parser.endian, signed, size, &input[cursor..end]);
                cursor = end;
                out.push(Value::Integer(value));
            }
            StructToken::Float { size } => {
                let end = cursor.saturating_add(size);
                if end > input.len() {
                    return Err(mlua::Error::runtime("unpack overflow"));
                }
                let value = read_float(parser.endian, size, &input[cursor..end]);
                cursor = end;
                out.push(Value::Number(value));
            }
        }
    }
    Ok(out)
}

#[derive(Clone, Copy)]
enum Endian {
    Native,
    Little,
    Big,
}

#[derive(Clone, Copy)]
enum StructToken {
    Pad(usize),
    Bytes(usize),
    Int { signed: bool, size: usize },
    Float { size: usize },
}

struct StructFormat<'a> {
    bytes: &'a [u8],
    index: usize,
    endian: Endian,
}

impl<'a> StructFormat<'a> {
    fn new(format: &'a str) -> Self {
        Self {
            bytes: format.as_bytes(),
            index: 0,
            endian: Endian::Native,
        }
    }

    fn next(&mut self) -> Result<Option<StructToken>, mlua::Error> {
        while self.index < self.bytes.len() {
            let byte = self.bytes[self.index];
            self.index += 1;
            if byte.is_ascii_whitespace() {
                continue;
            }
            if byte.is_ascii_digit() {
                let mut count = (byte - b'0') as usize;
                while self.index < self.bytes.len() && self.bytes[self.index].is_ascii_digit() {
                    count = count.saturating_mul(10) + (self.bytes[self.index] - b'0') as usize;
                    self.index += 1;
                }
                if self.index >= self.bytes.len() {
                    return Err(mlua::Error::runtime("bad struct format"));
                }
                let code = self.bytes[self.index];
                self.index += 1;
                return self.token(code, count.max(1)).map(Some);
            }
            match byte {
                b'<' => self.endian = Endian::Little,
                b'>' | b'!' => self.endian = Endian::Big,
                b'=' => self.endian = Endian::Native,
                code => return self.token(code, 1).map(Some),
            }
        }
        Ok(None)
    }

    fn token(&self, code: u8, count: usize) -> Result<StructToken, mlua::Error> {
        match code {
            b'b' => Ok(StructToken::Int {
                signed: true,
                size: 1,
            }),
            b'B' => Ok(StructToken::Int {
                signed: false,
                size: 1,
            }),
            b'h' => Ok(StructToken::Int {
                signed: true,
                size: 2,
            }),
            b'H' => Ok(StructToken::Int {
                signed: false,
                size: 2,
            }),
            b'i' | b'l' => Ok(StructToken::Int {
                signed: true,
                size: 4,
            }),
            b'I' | b'L' => Ok(StructToken::Int {
                signed: false,
                size: 4,
            }),
            b'q' => Ok(StructToken::Int {
                signed: true,
                size: 8,
            }),
            b'Q' => Ok(StructToken::Int {
                signed: false,
                size: 8,
            }),
            b'f' => Ok(StructToken::Float { size: 4 }),
            b'd' => Ok(StructToken::Float { size: 8 }),
            b's' | b'c' => Ok(StructToken::Bytes(count)),
            b'x' => Ok(StructToken::Pad(count)),
            _ => Err(mlua::Error::runtime("unsupported struct format")),
        }
    }
}

fn write_int(
    out: &mut Vec<u8>,
    endian: Endian,
    signed: bool,
    size: usize,
    value: &Value,
) -> Result<(), mlua::Error> {
    let number = lua_value_to_string(value.clone())?
        .parse::<i128>()
        .map_err(|_| mlua::Error::runtime("invalid integer"))?;
    let mut bytes = [0u8; 16];
    if signed {
        bytes.copy_from_slice(&number.to_le_bytes());
    } else {
        bytes.copy_from_slice(&(number as u128).to_le_bytes());
    }
    match endian {
        Endian::Little => out.extend_from_slice(&bytes[..size]),
        Endian::Big => out.extend(bytes[..size].iter().rev()),
        Endian::Native => {
            if cfg!(target_endian = "big") {
                out.extend(bytes[..size].iter().rev());
            } else {
                out.extend_from_slice(&bytes[..size]);
            }
        }
    }
    Ok(())
}

fn read_int(endian: Endian, signed: bool, size: usize, bytes: &[u8]) -> i64 {
    let mut tmp = [0u8; 8];
    match endian {
        Endian::Little => tmp[..size].copy_from_slice(bytes),
        Endian::Big => {
            for (index, byte) in bytes.iter().rev().enumerate() {
                tmp[index] = *byte;
            }
        }
        Endian::Native => {
            if cfg!(target_endian = "big") {
                for (index, byte) in bytes.iter().rev().enumerate() {
                    tmp[index] = *byte;
                }
            } else {
                tmp[..size].copy_from_slice(bytes);
            }
        }
    }
    if signed {
        i64::from_le_bytes(tmp)
    } else {
        u64::from_le_bytes(tmp) as i64
    }
}

fn write_float(
    out: &mut Vec<u8>,
    endian: Endian,
    size: usize,
    value: &Value,
) -> Result<(), mlua::Error> {
    let number = lua_value_to_string(value.clone())?
        .parse::<f64>()
        .map_err(|_| mlua::Error::runtime("invalid float"))?;
    match size {
        4 => {
            let bytes = match endian {
                Endian::Little => (number as f32).to_le_bytes().to_vec(),
                Endian::Big => (number as f32).to_be_bytes().to_vec(),
                Endian::Native => (number as f32).to_ne_bytes().to_vec(),
            };
            out.extend_from_slice(&bytes);
        }
        8 => {
            let bytes = match endian {
                Endian::Little => number.to_le_bytes().to_vec(),
                Endian::Big => number.to_be_bytes().to_vec(),
                Endian::Native => number.to_ne_bytes().to_vec(),
            };
            out.extend_from_slice(&bytes);
        }
        _ => return Err(mlua::Error::runtime("unsupported float size")),
    }
    Ok(())
}

fn read_float(endian: Endian, size: usize, bytes: &[u8]) -> f64 {
    match size {
        4 => {
            let mut tmp = [0u8; 4];
            tmp.copy_from_slice(bytes);
            match endian {
                Endian::Little => f32::from_le_bytes(tmp) as f64,
                Endian::Big => f32::from_be_bytes(tmp) as f64,
                Endian::Native => f32::from_ne_bytes(tmp) as f64,
            }
        }
        _ => {
            let mut tmp = [0u8; 8];
            tmp.copy_from_slice(bytes);
            match endian {
                Endian::Little => f64::from_le_bytes(tmp),
                Endian::Big => f64::from_be_bytes(tmp),
                Endian::Native => f64::from_ne_bytes(tmp),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Mutex};

    use super::*;

    #[derive(Default)]
    struct MockRuntime {
        values: BTreeMap<Vec<u8>, Bytes>,
        completed: Mutex<Vec<bool>>,
    }

    impl ScriptExecutionHooks for MockRuntime {
        fn dispatch(&mut self, command: &[u8], args: &[Bytes]) -> Result<RespValue, LuaError> {
            match command {
                b"GET" => Ok(RespValue::Bulk(self.values.get(args[0].as_ref()).cloned())),
                b"SET" => {
                    self.values.insert(args[0].to_vec(), args[1].clone());
                    Ok(RespValue::Simple(Bytes::from_static(b"OK")))
                }
                b"INCR" => {
                    let current = self
                        .values
                        .get(args[0].as_ref())
                        .and_then(|value| std::str::from_utf8(value).ok())
                        .and_then(|value| value.parse::<i64>().ok())
                        .unwrap_or(0)
                        + 1;
                    self.values
                        .insert(args[0].to_vec(), Bytes::from(current.to_string()));
                    Ok(RespValue::Integer(current))
                }
                b"NONEXISTENT" => Err(LuaError::redis_error("Unknown command")),
                _ => Err(LuaError::redis_error("unsupported mock command")),
            }
        }

        fn script_completed(&mut self, committed: bool) -> Result<(), LuaError> {
            self.completed
                .lock()
                .expect("completed lock")
                .push(committed);
            Ok(())
        }
    }

    #[test]
    fn eval_type_conversions_match_redis_shape() {
        let mut engine = LuaEngine::new(&ScriptingConfig::default()).unwrap();
        let mut runtime = MockRuntime::default();
        assert_eq!(
            engine
                .eval(
                    "return {1, 2, 3}",
                    ScriptContext {
                        keys: &[],
                        args: &[],
                        readonly: false,
                        db_id: 0,
                        username: "default",
                        hooks: &mut runtime,
                    },
                )
                .unwrap(),
            RespValue::Array(vec![
                RespValue::Integer(1),
                RespValue::Integer(2),
                RespValue::Integer(3),
            ])
        );
        assert_eq!(
            engine
                .eval(
                    "return redis.status_reply('OK')",
                    ScriptContext {
                        keys: &[],
                        args: &[],
                        readonly: false,
                        db_id: 0,
                        username: "default",
                        hooks: &mut runtime,
                    },
                )
                .unwrap(),
            RespValue::Simple(Bytes::from_static(b"OK"))
        );
        assert_eq!(
            engine
                .eval(
                    "return 3.7",
                    ScriptContext {
                        keys: &[],
                        args: &[],
                        readonly: false,
                        db_id: 0,
                        username: "default",
                        hooks: &mut runtime,
                    },
                )
                .unwrap(),
            RespValue::Integer(3)
        );
    }

    #[test]
    fn redis_call_mutates_runtime() {
        let mut engine = LuaEngine::new(&ScriptingConfig::default()).unwrap();
        let mut runtime = MockRuntime::default();
        let response = engine
            .eval(
                "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
                ScriptContext {
                    keys: &[Bytes::from_static(b"foo")],
                    args: &[Bytes::from_static(b"bar")],
                    readonly: false,
                    db_id: 0,
                    username: "default",
                    hooks: &mut runtime,
                },
            )
            .unwrap();
        assert_eq!(response, RespValue::Bulk(Some(Bytes::from_static(b"bar"))));
    }

    #[test]
    fn pcall_returns_error_table() {
        let mut engine = LuaEngine::new(&ScriptingConfig::default()).unwrap();
        let mut runtime = MockRuntime::default();
        let response = engine
            .eval(
                "return redis.pcall('NONEXISTENT')",
                ScriptContext {
                    keys: &[],
                    args: &[],
                    readonly: false,
                    db_id: 0,
                    username: "default",
                    hooks: &mut runtime,
                },
            )
            .unwrap();
        assert!(matches!(response, RespValue::Error(_)));
    }

    #[test]
    fn readonly_scripts_reject_writes() {
        let mut engine = LuaEngine::new(&ScriptingConfig::default()).unwrap();
        let mut runtime = MockRuntime::default();
        let error = engine
            .eval(
                "return redis.call('SET', KEYS[1], 1)",
                ScriptContext {
                    keys: &[Bytes::from_static(b"k")],
                    args: &[],
                    readonly: true,
                    db_id: 0,
                    username: "default",
                    hooks: &mut runtime,
                },
            )
            .unwrap_err();
        assert_eq!(error.client_message(), "READONLY Script attempted write");
    }

    #[test]
    fn script_cache_round_trip_works() {
        let mut engine = LuaEngine::new(&ScriptingConfig::default()).unwrap();
        let sha = engine.script_load("return 1").unwrap();
        assert_eq!(sha.len(), 40);
        let mut runtime = MockRuntime::default();
        let response = engine
            .evalsha(
                sha.as_str(),
                ScriptContext {
                    keys: &[],
                    args: &[],
                    readonly: false,
                    db_id: 0,
                    username: "default",
                    hooks: &mut runtime,
                },
            )
            .unwrap();
        assert_eq!(response, RespValue::Integer(1));
        assert_eq!(engine.script_exists(&[sha.as_str()]), vec![true]);
    }

    #[test]
    fn function_load_and_fcall_work() {
        let mut engine = LuaEngine::new(&ScriptingConfig::default()).unwrap();
        engine
            .function_load(
                "#!lua name=mylib\nredis.register_function('f', function(keys, args) return 1 end)",
                false,
            )
            .unwrap();
        let mut runtime = MockRuntime::default();
        let value = engine
            .fcall(
                "f",
                ScriptContext {
                    keys: &[],
                    args: &[],
                    readonly: false,
                    db_id: 0,
                    username: "default",
                    hooks: &mut runtime,
                },
            )
            .unwrap();
        assert_eq!(value, RespValue::Integer(1));
    }

    #[test]
    fn sandbox_blocks_unsafe_loads() {
        let mut engine = LuaEngine::new(&ScriptingConfig::default()).unwrap();
        let mut runtime = MockRuntime::default();
        let error = engine
            .eval(
                "return load('\\27Lua')",
                ScriptContext {
                    keys: &[],
                    args: &[],
                    readonly: false,
                    db_id: 0,
                    username: "default",
                    hooks: &mut runtime,
                },
            )
            .unwrap_err();
        assert!(
            error
                .client_message()
                .contains("Bytecode loading is not allowed")
        );
    }
}
