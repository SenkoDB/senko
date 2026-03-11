use senko_core::SenkoResult;

use crate::parser::{ParseStatus, RespParser};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateKind {
    Array,
    Map,
    Set,
    Push,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateEncoding {
    Resp,
    Inline,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Aggregate<'a> {
    kind: AggregateKind,
    len: usize,
    data: &'a [u8],
    encoding: AggregateEncoding,
}

impl<'a> Aggregate<'a> {
    pub const fn new(
        kind: AggregateKind,
        len: usize,
        data: &'a [u8],
        encoding: AggregateEncoding,
    ) -> Self {
        Self {
            kind,
            len,
            data,
            encoding,
        }
    }

    pub const fn kind(self) -> AggregateKind {
        self.kind
    }

    pub const fn len(self) -> usize {
        self.len
    }

    pub const fn is_empty(self) -> bool {
        self.len == 0
    }

    pub const fn data(self) -> &'a [u8] {
        self.data
    }

    pub const fn encoding(self) -> AggregateEncoding {
        self.encoding
    }

    pub fn iter(self) -> FrameIter<'a> {
        FrameIter {
            aggregate: self,
            consumed: 0,
            emitted: 0,
            parser: RespParser::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Frame<'a> {
    SimpleString(&'a [u8]),
    SimpleError(&'a [u8]),
    Integer(i64),
    BulkString(&'a [u8]),
    Array(Aggregate<'a>),
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(&'a [u8]),
    BlobError(&'a [u8]),
    VerbatimString { encoding: &'a [u8], data: &'a [u8] },
    Map(Aggregate<'a>),
    Set(Aggregate<'a>),
    Push(Aggregate<'a>),
}

impl<'a> Frame<'a> {
    pub const fn aggregate(self) -> Option<Aggregate<'a>> {
        match self {
            Self::Array(aggregate)
            | Self::Map(aggregate)
            | Self::Set(aggregate)
            | Self::Push(aggregate) => Some(aggregate),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FrameIter<'a> {
    aggregate: Aggregate<'a>,
    consumed: usize,
    emitted: usize,
    parser: RespParser,
}

impl<'a> Iterator for FrameIter<'a> {
    type Item = SenkoResult<Frame<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.emitted == self.aggregate.len {
            return None;
        }

        match self.aggregate.encoding {
            AggregateEncoding::Resp => {
                let slice = &self.aggregate.data[self.consumed..];
                match self.parser.parse(slice) {
                    Ok(ParseStatus::Complete(frame, used)) => {
                        self.consumed += used;
                        self.emitted += 1;
                        Some(Ok(frame))
                    }
                    Ok(ParseStatus::Incomplete(_)) => Some(Err(senko_core::SenkoError::Protocol(
                        "aggregate data ended mid-frame",
                    ))),
                    Err(error) => Some(Err(error)),
                }
            }
            AggregateEncoding::Inline => {
                let remaining = &self.aggregate.data[self.consumed..];
                let next = remaining
                    .iter()
                    .position(|byte| *byte == b' ' || *byte == b'\t')
                    .unwrap_or(remaining.len());
                let token = &remaining[..next];
                self.consumed += next;
                while self.consumed < self.aggregate.data.len()
                    && matches!(self.aggregate.data[self.consumed], b' ' | b'\t')
                {
                    self.consumed += 1;
                }
                self.emitted += 1;
                Some(Ok(Frame::BulkString(token)))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.aggregate.len.saturating_sub(self.emitted);
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for FrameIter<'a> {}
