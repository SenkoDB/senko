pub mod arithmetic;
pub mod basic;
pub mod bitmap;
pub mod conditional;
pub mod generic;
pub mod hash;
pub mod hll;
pub mod lcs;
pub mod list;
pub mod multi;
pub mod set;
pub mod stream;
pub mod strops;
pub mod zset;

use senko_core::SenkoValue;
use smallvec::SmallVec;

#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    Simple(&'static [u8]),
    Value(Option<SenkoValue>),
    NullArray,
    Integer(i64),
    Array(Box<SmallVec<[Response; 16]>>),
    Map(Box<SmallVec<[Response; 32]>>),
}
