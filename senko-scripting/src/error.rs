use std::{fmt, string::FromUtf8Error};

#[derive(Debug)]
pub enum LuaError {
    Mlua(mlua::Error),
    Utf8(std::str::Utf8Error),
    FromUtf8(FromUtf8Error),
    Message(String),
    NoScript,
    FunctionNotFound,
    LibraryExists(String),
    LibraryNotFound,
    ReadonlyViolation(String),
    ForbiddenCommand(String),
    KillDenied,
    NotBusy,
    ScriptKilled,
}

impl LuaError {
    pub fn client_message(&self) -> String {
        match self {
            Self::NoScript => "NOSCRIPT No matching script. Please use EVAL.".to_owned(),
            Self::FunctionNotFound => "ERR Function not found".to_owned(),
            Self::LibraryExists(name) => format!("ERR Library '{name}' already exists"),
            Self::LibraryNotFound => "ERR Library not found".to_owned(),
            Self::ReadonlyViolation(_) => "READONLY Script attempted write".to_owned(),
            Self::ForbiddenCommand(command) => {
                format!("ERR Command '{command}' is not allowed from script")
            }
            Self::KillDenied => "UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script to terminate or kill the server in a hard way using the SHUTDOWN NOSAVE command.".to_owned(),
            Self::NotBusy => "NOTBUSY No scripts in execution right now".to_owned(),
            Self::ScriptKilled => "ERR Script killed by user with SCRIPT KILL... script aborted.".to_owned(),
            Self::Mlua(error) => normalize_mlua_error(error),
            Self::Utf8(error) => format!("ERR Error running script: {error}"),
            Self::FromUtf8(error) => format!("ERR Error running script: {error}"),
            Self::Message(message) => message.clone(),
        }
    }

    pub fn redis_error(message: impl Into<String>) -> Self {
        Self::Message(format!("ERR {}", message.into()))
    }
}

impl fmt::Display for LuaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.client_message())
    }
}

impl std::error::Error for LuaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Mlua(error) => Some(error),
            Self::Utf8(error) => Some(error),
            Self::FromUtf8(error) => Some(error),
            Self::Message(_)
            | Self::NoScript
            | Self::FunctionNotFound
            | Self::LibraryExists(_)
            | Self::LibraryNotFound
            | Self::ReadonlyViolation(_)
            | Self::ForbiddenCommand(_)
            | Self::KillDenied
            | Self::NotBusy
            | Self::ScriptKilled => None,
        }
    }
}

impl From<mlua::Error> for LuaError {
    fn from(value: mlua::Error) -> Self {
        Self::Mlua(value)
    }
}

impl From<std::str::Utf8Error> for LuaError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::Utf8(value)
    }
}

impl From<FromUtf8Error> for LuaError {
    fn from(value: FromUtf8Error) -> Self {
        Self::FromUtf8(value)
    }
}

fn normalize_mlua_error(error: &mlua::Error) -> String {
    let rendered = error.to_string();
    let line = rendered.lines().next().unwrap_or(rendered.as_str()).trim();
    let line = line.strip_prefix("runtime error: ").unwrap_or(line).trim();
    for prefix in [
        "READONLY ",
        "NOSCRIPT ",
        "NOPERM ",
        "WRONGTYPE ",
        "NOTBUSY ",
        "UNKILLABLE ",
        "ERR ",
    ] {
        if line.starts_with(prefix) {
            return line.to_owned();
        }
    }
    format!("ERR Error running script: {rendered}")
}
