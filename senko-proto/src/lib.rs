#![deny(unsafe_code)]

pub mod frame;
pub mod parser;
pub mod serializer;

pub use frame::{Aggregate, AggregateEncoding, AggregateKind, Frame, FrameIter};
pub use parser::{ParseStatus, RespParser};
pub use serializer::{
    INTEGER_MINUS_ONE, INTEGER_ONE, INTEGER_ZERO, NIL_BULK, OK, PONG, RespSerializer,
};
