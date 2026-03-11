use std::{
    fmt, io,
    net::AddrParseError,
    num::{ParseFloatError, ParseIntError},
    str::Utf8Error,
};

use compact_str::CompactString;

pub type SenkoResult<T> = Result<T, SenkoError>;

#[derive(Debug)]
pub enum SenkoError {
    Io(io::Error),
    AddrParse(AddrParseError),
    Utf8(Utf8Error),
    ParseInt(ParseIntError),
    ParseFloat(ParseFloatError),
    Protocol(&'static str),
    ProtocolMessage(CompactString),
    Storage(&'static str),
    StorageMessage(CompactString),
    WrongType {
        expected: &'static str,
        actual: &'static str,
    },
    KeyNotFound,
    KeyExpired,
    OutOfMemory,
    InvalidConfig(&'static str),
}

impl fmt::Display for SenkoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::AddrParse(err) => write!(f, "address parse error: {err}"),
            Self::Utf8(err) => write!(f, "utf-8 error: {err}"),
            Self::ParseInt(err) => write!(f, "integer parse error: {err}"),
            Self::ParseFloat(err) => write!(f, "float parse error: {err}"),
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
            Self::ProtocolMessage(message) => write!(f, "protocol error: {message}"),
            Self::Storage(message) => write!(f, "storage error: {message}"),
            Self::StorageMessage(message) => write!(f, "storage error: {message}"),
            Self::WrongType { expected, actual } => {
                write!(f, "wrong type: expected {expected}, found {actual}")
            }
            Self::KeyNotFound => f.write_str("key not found"),
            Self::KeyExpired => f.write_str("key expired"),
            Self::OutOfMemory => f.write_str("max memory exceeded"),
            Self::InvalidConfig(message) => write!(f, "invalid config: {message}"),
        }
    }
}

impl std::error::Error for SenkoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::AddrParse(err) => Some(err),
            Self::Utf8(err) => Some(err),
            Self::ParseInt(err) => Some(err),
            Self::ParseFloat(err) => Some(err),
            Self::Protocol(_)
            | Self::ProtocolMessage(_)
            | Self::Storage(_)
            | Self::StorageMessage(_)
            | Self::WrongType { .. }
            | Self::KeyNotFound
            | Self::KeyExpired
            | Self::OutOfMemory
            | Self::InvalidConfig(_) => None,
        }
    }
}

impl From<io::Error> for SenkoError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<AddrParseError> for SenkoError {
    fn from(value: AddrParseError) -> Self {
        Self::AddrParse(value)
    }
}

impl From<Utf8Error> for SenkoError {
    fn from(value: Utf8Error) -> Self {
        Self::Utf8(value)
    }
}

impl From<ParseIntError> for SenkoError {
    fn from(value: ParseIntError) -> Self {
        Self::ParseInt(value)
    }
}

impl From<ParseFloatError> for SenkoError {
    fn from(value: ParseFloatError) -> Self {
        Self::ParseFloat(value)
    }
}

impl From<&'static str> for SenkoError {
    fn from(value: &'static str) -> Self {
        Self::Protocol(value)
    }
}

impl From<CompactString> for SenkoError {
    fn from(value: CompactString) -> Self {
        Self::ProtocolMessage(value)
    }
}
