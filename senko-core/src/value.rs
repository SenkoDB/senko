use std::{borrow::Cow, fmt, str};

use bytes::Bytes;

#[cfg(feature = "prob")]
use crate::{BloomFilter, CountMinSketch, CuckooFilter, TDigest, TopKSketch};
use crate::{HashObject, QuickList, SetObject, StreamObject, ZSetObject};

#[derive(Debug, Clone)]
pub enum SenkoValue {
    Raw(Bytes),
    Int(i64),
    Float(f64),
    #[cfg(feature = "json")]
    Json(std::sync::Arc<sonic_rs::Value>),
    #[cfg(feature = "vector")]
    VectorSet(std::sync::Arc<parking_lot::RwLock<crate::VectorSet>>),
    #[cfg(feature = "prob")]
    BloomFilter(Box<BloomFilter>),
    #[cfg(feature = "prob")]
    CuckooFilter(Box<CuckooFilter>),
    #[cfg(feature = "prob")]
    CountMinSketch(Box<CountMinSketch>),
    #[cfg(feature = "prob")]
    TopK(Box<TopKSketch>),
    #[cfg(feature = "prob")]
    TDigest(Box<TDigest>),
    Hash(Box<HashObject>),
    List(Box<QuickList>),
    Set(Box<SetObject>),
    Stream(Box<StreamObject>),
    ZSet(Box<ZSetObject>),
}

pub type FeroxValue = SenkoValue;

impl PartialEq for SenkoValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Raw(left), Self::Raw(right)) => left == right,
            (Self::Int(left), Self::Int(right)) => left == right,
            (Self::Float(left), Self::Float(right)) => left == right,
            #[cfg(feature = "json")]
            (Self::Json(left), Self::Json(right)) => left == right,
            #[cfg(feature = "vector")]
            (Self::VectorSet(left), Self::VectorSet(right)) => std::sync::Arc::ptr_eq(left, right),
            #[cfg(feature = "prob")]
            (Self::BloomFilter(left), Self::BloomFilter(right)) => left == right,
            #[cfg(feature = "prob")]
            (Self::CuckooFilter(left), Self::CuckooFilter(right)) => left == right,
            #[cfg(feature = "prob")]
            (Self::CountMinSketch(left), Self::CountMinSketch(right)) => left == right,
            #[cfg(feature = "prob")]
            (Self::TopK(left), Self::TopK(right)) => left == right,
            #[cfg(feature = "prob")]
            (Self::TDigest(left), Self::TDigest(right)) => left == right,
            (Self::Hash(left), Self::Hash(right)) => left == right,
            (Self::List(left), Self::List(right)) => left == right,
            (Self::Set(left), Self::Set(right)) => left == right,
            (Self::Stream(left), Self::Stream(right)) => left == right,
            (Self::ZSet(left), Self::ZSet(right)) => left == right,
            _ => false,
        }
    }
}

impl SenkoValue {
    pub fn encode_attempt(raw: &[u8]) -> SenkoValue {
        if let Ok(text) = str::from_utf8(raw) {
            if let Ok(value) = text.parse::<i64>() {
                return Self::Int(value);
            }
            if let Ok(value) = text.parse::<f64>() {
                return Self::Float(value);
            }
        }
        Self::Raw(Bytes::copy_from_slice(raw))
    }

    pub fn as_bytes(&self) -> Cow<'_, [u8]> {
        match self {
            Self::Raw(raw) => Cow::Borrowed(raw.as_ref()),
            Self::Int(value) => Cow::Owned(value.to_string().into_bytes()),
            Self::Float(value) => Cow::Owned(value.to_string().into_bytes()),
            #[cfg(feature = "json")]
            Self::Json(value) => Cow::Owned(
                sonic_rs::to_string(value.as_ref())
                    .unwrap_or_else(|_| "null".to_string())
                    .into_bytes(),
            ),
            #[cfg(feature = "vector")]
            Self::VectorSet(_) => Cow::Borrowed(b"[vectorset]"),
            #[cfg(feature = "prob")]
            Self::BloomFilter(_) => Cow::Borrowed(b"[bloom]"),
            #[cfg(feature = "prob")]
            Self::CuckooFilter(_) => Cow::Borrowed(b"[cuckoo]"),
            #[cfg(feature = "prob")]
            Self::CountMinSketch(_) => Cow::Borrowed(b"[cms]"),
            #[cfg(feature = "prob")]
            Self::TopK(_) => Cow::Borrowed(b"[topk]"),
            #[cfg(feature = "prob")]
            Self::TDigest(_) => Cow::Borrowed(b"[tdigest]"),
            Self::Hash(_) => Cow::Borrowed(b"[hash]"),
            Self::List(_) => Cow::Borrowed(b"[list]"),
            Self::Set(_) => Cow::Borrowed(b"[set]"),
            Self::Stream(_) => Cow::Borrowed(b"[stream]"),
            Self::ZSet(_) => Cow::Borrowed(b"[zset]"),
        }
    }
}

impl fmt::Display for SenkoValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Raw(raw) => match str::from_utf8(raw) {
                Ok(text) => f.write_str(text),
                Err(_) => {
                    for &byte in raw.iter() {
                        write!(f, "\\x{byte:02x}")?;
                    }
                    Ok(())
                }
            },
            Self::Int(value) => write!(f, "{value}"),
            Self::Float(value) => {
                let rendered = value.to_string();
                f.write_str(&rendered)
            }
            #[cfg(feature = "json")]
            Self::Json(value) => f.write_str(
                &sonic_rs::to_string(value.as_ref()).unwrap_or_else(|_| "null".to_string()),
            ),
            #[cfg(feature = "vector")]
            Self::VectorSet(_) => f.write_str("[vectorset]"),
            #[cfg(feature = "prob")]
            Self::BloomFilter(_) => f.write_str("[bloom]"),
            #[cfg(feature = "prob")]
            Self::CuckooFilter(_) => f.write_str("[cuckoo]"),
            #[cfg(feature = "prob")]
            Self::CountMinSketch(_) => f.write_str("[cms]"),
            #[cfg(feature = "prob")]
            Self::TopK(_) => f.write_str("[topk]"),
            #[cfg(feature = "prob")]
            Self::TDigest(_) => f.write_str("[tdigest]"),
            Self::Hash(_) => f.write_str("[hash]"),
            Self::List(_) => f.write_str("[list]"),
            Self::Set(_) => f.write_str("[set]"),
            Self::Stream(_) => f.write_str("[stream]"),
            Self::ZSet(_) => f.write_str("[zset]"),
        }
    }
}

impl From<Bytes> for SenkoValue {
    fn from(value: Bytes) -> Self {
        Self::Raw(value)
    }
}

impl From<&[u8]> for SenkoValue {
    fn from(value: &[u8]) -> Self {
        Self::encode_attempt(value)
    }
}

impl From<i64> for SenkoValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for SenkoValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}
