use mlua::{Function, Lua, Value};

use crate::error::LuaError;

const WHITELIST_MODULES: &[&str] = &["cjson", "cmsgpack", "struct", "bit"];

pub fn apply(lua: &Lua) -> Result<(), LuaError> {
    let globals = lua.globals();

    for name in [
        "io",
        "file",
        "package",
        "loadfile",
        "dofile",
        "collectgarbage",
        "newproxy",
        "debug",
    ] {
        globals.set(name, Value::Nil)?;
    }

    let os = lua.create_table()?;
    globals.set("os", os)?;

    globals.set(
        "print",
        lua.create_function(|_, message: String| {
            tracing::info!(target = "senko.scripting", "{message}");
            Ok(())
        })?,
    )?;
    globals.set(
        "tostring",
        lua.create_function(|_, value: Value| Ok(safe_tostring(value)))?,
    )?;
    globals.set(
        "require",
        lua.create_function(|lua, module: String| {
            if !WHITELIST_MODULES
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(module.as_str()))
            {
                return Err(mlua::Error::runtime(format!("module '{module}' not found")));
            }
            let globals = lua.globals();
            globals.get::<Value>(module)
        })?,
    )?;
    globals.set(
        "load",
        lua.create_function(
            |lua, (source, chunk_name): (mlua::String, Option<String>)| -> mlua::Result<Function> {
                let bytes = source.as_bytes();
                if bytes.as_ref().first() == Some(&0x1b) {
                    return Err(mlua::Error::runtime("Bytecode loading is not allowed."));
                }
                let mut chunk = lua.load(bytes.as_ref());
                if let Some(name) = chunk_name {
                    chunk = chunk.set_name(name);
                }
                chunk.into_function()
            },
        )?,
    )?;
    Ok(())
}

fn safe_tostring(value: Value) -> String {
    match value {
        Value::Nil => "nil".to_owned(),
        Value::Boolean(value) => {
            if value {
                "true".to_owned()
            } else {
                "false".to_owned()
            }
        }
        Value::Integer(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => String::from_utf8_lossy(value.as_bytes().as_ref()).into_owned(),
        Value::Table(_) => "table".to_owned(),
        Value::Function(_) => "function".to_owned(),
        Value::Thread(_) => "thread".to_owned(),
        Value::UserData(_) => "userdata".to_owned(),
        Value::LightUserData(_) => "lightuserdata".to_owned(),
        Value::Error(error) => error.to_string(),
        Value::Other(_) => "value".to_owned(),
    }
}
