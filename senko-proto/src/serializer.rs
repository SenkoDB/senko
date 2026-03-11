use bytes::BytesMut;

use crate::frame::{Aggregate, AggregateEncoding, AggregateKind, Frame};

pub const OK: &[u8; 5] = b"+OK\r\n";
pub const PONG: &[u8; 7] = b"+PONG\r\n";
pub const NIL_BULK: &[u8; 5] = b"$-1\r\n";
pub const INTEGER_MINUS_ONE: &[u8; 5] = b":-1\r\n";
pub const INTEGER_ZERO: &[u8; 4] = b":0\r\n";
pub const INTEGER_ONE: &[u8; 4] = b":1\r\n";

#[derive(Debug, Default, Clone, Copy)]
pub struct RespSerializer;

impl RespSerializer {
    #[inline(always)]
    pub fn write_simple_string(out: &mut BytesMut, value: &[u8]) {
        out.extend_from_slice(b"+");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }

    #[inline(always)]
    pub fn write_error(out: &mut BytesMut, value: &[u8]) {
        out.extend_from_slice(b"-");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }

    #[inline(always)]
    pub fn write_integer(out: &mut BytesMut, value: i64) {
        out.extend_from_slice(b":");
        let mut buffer = itoa::Buffer::new();
        out.extend_from_slice(buffer.format(value).as_bytes());
        out.extend_from_slice(b"\r\n");
    }

    #[inline(always)]
    pub fn write_bulk_string(out: &mut BytesMut, value: &[u8]) {
        out.extend_from_slice(b"$");
        let mut buffer = itoa::Buffer::new();
        out.extend_from_slice(buffer.format(value.len()).as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }

    #[inline(always)]
    pub fn write_null(out: &mut BytesMut) {
        out.extend_from_slice(b"_\r\n");
    }

    #[inline(always)]
    pub fn write_array_header(out: &mut BytesMut, len: usize) {
        out.extend_from_slice(b"*");
        let mut buffer = itoa::Buffer::new();
        out.extend_from_slice(buffer.format(len).as_bytes());
        out.extend_from_slice(b"\r\n");
    }

    #[inline(always)]
    pub fn write_raw_map_header(out: &mut BytesMut, len: usize) {
        out.extend_from_slice(b"%");
        let mut buffer = itoa::Buffer::new();
        out.extend_from_slice(buffer.format(len).as_bytes());
        out.extend_from_slice(b"\r\n");
    }

    #[inline(always)]
    pub fn write_ok(out: &mut BytesMut) {
        out.extend_from_slice(OK);
    }

    #[inline(always)]
    pub fn write_nil_bulk(out: &mut BytesMut) {
        out.extend_from_slice(NIL_BULK);
    }

    pub fn serialize(frame: &Frame<'_>) -> BytesMut {
        let mut out = BytesMut::with_capacity(frame.encoded_len());
        Self::write_frame(&mut out, frame);
        out
    }

    pub fn write_frame(out: &mut BytesMut, frame: &Frame<'_>) {
        match frame {
            Frame::SimpleString(value) => Self::write_simple_string(out, value),
            Frame::SimpleError(value) => Self::write_error(out, value),
            Frame::Integer(value) => Self::write_integer(out, *value),
            Frame::BulkString(value) => Self::write_bulk_string(out, value),
            Frame::Array(aggregate) => Self::write_aggregate(out, *aggregate),
            Frame::Null => Self::write_null(out),
            Frame::Boolean(value) => {
                out.extend_from_slice(if *value { b"#t\r\n" } else { b"#f\r\n" });
            }
            Frame::Double(value) => {
                out.extend_from_slice(b",");
                let mut buffer = ryu::Buffer::new();
                out.extend_from_slice(buffer.format(*value).as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Frame::BigNumber(value) => {
                out.extend_from_slice(b"(");
                out.extend_from_slice(value);
                out.extend_from_slice(b"\r\n");
            }
            Frame::BlobError(value) => {
                out.extend_from_slice(b"!");
                let mut buffer = itoa::Buffer::new();
                out.extend_from_slice(buffer.format(value.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(value);
                out.extend_from_slice(b"\r\n");
            }
            Frame::VerbatimString { encoding, data } => {
                out.extend_from_slice(b"=");
                let len = encoding.len() + 1 + data.len();
                let mut buffer = itoa::Buffer::new();
                out.extend_from_slice(buffer.format(len).as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(encoding);
                out.extend_from_slice(b":");
                out.extend_from_slice(data);
                out.extend_from_slice(b"\r\n");
            }
            Frame::Map(aggregate) | Frame::Set(aggregate) | Frame::Push(aggregate) => {
                Self::write_aggregate(out, *aggregate)
            }
        }
    }

    fn write_aggregate(out: &mut BytesMut, aggregate: Aggregate<'_>) {
        match aggregate.encoding() {
            AggregateEncoding::Resp => {
                let prefix = match aggregate.kind() {
                    AggregateKind::Array => b"*",
                    AggregateKind::Map => b"%",
                    AggregateKind::Set => b"~",
                    AggregateKind::Push => b">",
                };
                out.extend_from_slice(prefix);
                let mut buffer = itoa::Buffer::new();
                out.extend_from_slice(buffer.format(aggregate.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(aggregate.data());
            }
            AggregateEncoding::Inline => {
                out.extend_from_slice(aggregate.data());
                out.extend_from_slice(b"\r\n");
            }
        }
    }
}

trait EncodedLen {
    fn encoded_len(&self) -> usize;
}

impl EncodedLen for Frame<'_> {
    fn encoded_len(&self) -> usize {
        match self {
            Frame::SimpleString(value) | Frame::SimpleError(value) | Frame::BigNumber(value) => {
                1 + value.len() + 2
            }
            Frame::Integer(value) => integer_wire_len(*value),
            Frame::BulkString(value) | Frame::BlobError(value) => {
                1 + decimal_len(value.len()) + 2 + value.len() + 2
            }
            Frame::Array(aggregate)
            | Frame::Map(aggregate)
            | Frame::Set(aggregate)
            | Frame::Push(aggregate) => aggregate_wire_len(*aggregate),
            Frame::Null => 3,
            Frame::Boolean(_) => 4,
            Frame::Double(value) => 1 + double_len(*value) + 2,
            Frame::VerbatimString { encoding, data } => {
                let payload_len = encoding.len() + 1 + data.len();
                1 + decimal_len(payload_len) + 2 + payload_len + 2
            }
        }
    }
}

fn aggregate_wire_len(aggregate: Aggregate<'_>) -> usize {
    match aggregate.encoding() {
        AggregateEncoding::Resp => 1 + decimal_len(aggregate.len()) + 2 + aggregate.data().len(),
        AggregateEncoding::Inline => aggregate.data().len() + 2,
    }
}

fn integer_wire_len(value: i64) -> usize {
    1 + if value == -1 {
        2
    } else if value < 0 {
        1 + decimal_len(value.unsigned_abs() as usize)
    } else {
        decimal_len(value as usize)
    } + 2
}

fn decimal_len(mut value: usize) -> usize {
    let mut digits = 1usize;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn double_len(value: f64) -> usize {
    let mut buffer = ryu::Buffer::new();
    buffer.format(value).len()
}
