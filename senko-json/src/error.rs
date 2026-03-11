use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum JsonError {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("ERR Path '{0}' does not exist")]
    PathNotFound(String),
    #[error("ERR {0}")]
    ParseError(String),
    #[error("ERR Path must be either absolute JSONPath (starts with '$') or legacy path")]
    InvalidPath,
    #[error("ERR Operation on non-array value")]
    NotArray,
    #[error("ERR Operation on non-object value")]
    NotObject,
    #[error("ERR Operation on non-number value")]
    NotNumber,
    #[error("ERR Operation on non-string value")]
    NotString,
    #[error("ERR Operation on non-boolean value")]
    NotBoolean,
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum JsonPathError {
    #[error("ERR Path '{0}' does not exist")]
    Missing(String),
    #[error("ERR Path must be either absolute JSONPath (starts with '$') or legacy path")]
    InvalidPath,
    #[error("ERR unsupported JSONPath expression")]
    Unsupported,
}
