use memchr::memchr;
use senko_core::{SenkoError, SenkoResult};

use crate::frame::{Aggregate, AggregateEncoding, AggregateKind, Frame};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ParseStatus<'a> {
    Complete(Frame<'a>, usize),
    Incomplete(usize),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RespParser;

impl RespParser {
    pub const fn new() -> Self {
        Self
    }

    pub fn parse<'a>(&self, input: &'a [u8]) -> SenkoResult<ParseStatus<'a>> {
        self.parse_frame(input, true)
    }

    fn parse_frame<'a>(&self, input: &'a [u8], inline: bool) -> SenkoResult<ParseStatus<'a>> {
        let Some((&prefix, _)) = input.split_first() else {
            return Ok(ParseStatus::Incomplete(1));
        };

        match prefix {
            b'+' => self.parse_line_prefixed(input, Frame::SimpleString),
            b'-' => self.parse_line_prefixed(input, Frame::SimpleError),
            b':' => self.parse_integer(input),
            b'$' => self.parse_bulk_string(input),
            b'*' => self.parse_aggregate(input, AggregateKind::Array),
            b'_' => self.parse_null(input),
            b'#' => self.parse_boolean(input),
            b',' => self.parse_double(input),
            b'(' => self.parse_line_prefixed(input, Frame::BigNumber),
            b'!' => self.parse_blob_error(input),
            b'=' => self.parse_verbatim_string(input),
            b'%' => self.parse_aggregate(input, AggregateKind::Map),
            b'~' => self.parse_aggregate(input, AggregateKind::Set),
            b'>' => self.parse_aggregate(input, AggregateKind::Push),
            b'\r' | b'\n' => Err(SenkoError::Protocol("unexpected line break")),
            _ if inline => self.parse_inline(input),
            _ => Err(SenkoError::Protocol("unsupported RESP type prefix")),
        }
    }

    fn parse_line_prefixed<'a, F>(&self, input: &'a [u8], build: F) -> SenkoResult<ParseStatus<'a>>
    where
        F: FnOnce(&'a [u8]) -> Frame<'a>,
    {
        let Some((line, consumed)) = find_crlf(input, 1) else {
            return Ok(ParseStatus::Incomplete(input.len() + 2));
        };
        Ok(ParseStatus::Complete(build(line), consumed))
    }

    fn parse_integer<'a>(&self, input: &'a [u8]) -> SenkoResult<ParseStatus<'a>> {
        let Some((line, consumed)) = find_crlf(input, 1) else {
            return Ok(ParseStatus::Incomplete(input.len() + 2));
        };
        let value = parse_i64(line)?;
        Ok(ParseStatus::Complete(Frame::Integer(value), consumed))
    }

    fn parse_null<'a>(&self, input: &'a [u8]) -> SenkoResult<ParseStatus<'a>> {
        if input.len() < 3 {
            return Ok(ParseStatus::Incomplete(3));
        }
        if &input[..3] != b"_\r\n" {
            return Err(SenkoError::Protocol("invalid null frame"));
        }
        Ok(ParseStatus::Complete(Frame::Null, 3))
    }

    fn parse_boolean<'a>(&self, input: &'a [u8]) -> SenkoResult<ParseStatus<'a>> {
        if input.len() < 4 {
            return Ok(ParseStatus::Incomplete(4));
        }
        let value = match &input[..4] {
            b"#t\r\n" => true,
            b"#f\r\n" => false,
            _ => return Err(SenkoError::Protocol("invalid boolean frame")),
        };
        Ok(ParseStatus::Complete(Frame::Boolean(value), 4))
    }

    fn parse_double<'a>(&self, input: &'a [u8]) -> SenkoResult<ParseStatus<'a>> {
        let Some((line, consumed)) = find_crlf(input, 1) else {
            return Ok(ParseStatus::Incomplete(input.len() + 2));
        };
        let value = std::str::from_utf8(line)?.parse::<f64>()?;
        Ok(ParseStatus::Complete(Frame::Double(value), consumed))
    }

    fn parse_bulk_string<'a>(&self, input: &'a [u8]) -> SenkoResult<ParseStatus<'a>> {
        let Some((line, header_consumed)) = find_crlf(input, 1) else {
            return Ok(ParseStatus::Incomplete(input.len() + 2));
        };
        let len = parse_isize(line)?;
        if len < 0 {
            return Ok(ParseStatus::Complete(Frame::Null, header_consumed));
        }
        let len = len as usize;
        let total = header_consumed + len + 2;
        if input.len() < total {
            return Ok(ParseStatus::Incomplete(total));
        }
        if &input[header_consumed + len..total] != b"\r\n" {
            return Err(SenkoError::Protocol("bulk string missing trailing CRLF"));
        }
        let data = &input[header_consumed..header_consumed + len];
        Ok(ParseStatus::Complete(Frame::BulkString(data), total))
    }

    fn parse_blob_error<'a>(&self, input: &'a [u8]) -> SenkoResult<ParseStatus<'a>> {
        let Some((line, header_consumed)) = find_crlf(input, 1) else {
            return Ok(ParseStatus::Incomplete(input.len() + 2));
        };
        let len = parse_usize(line)?;
        let total = header_consumed + len + 2;
        if input.len() < total {
            return Ok(ParseStatus::Incomplete(total));
        }
        if &input[header_consumed + len..total] != b"\r\n" {
            return Err(SenkoError::Protocol("blob error missing trailing CRLF"));
        }
        let data = &input[header_consumed..header_consumed + len];
        Ok(ParseStatus::Complete(Frame::BlobError(data), total))
    }

    fn parse_verbatim_string<'a>(&self, input: &'a [u8]) -> SenkoResult<ParseStatus<'a>> {
        let Some((line, header_consumed)) = find_crlf(input, 1) else {
            return Ok(ParseStatus::Incomplete(input.len() + 2));
        };
        let len = parse_usize(line)?;
        let total = header_consumed + len + 2;
        if input.len() < total {
            return Ok(ParseStatus::Incomplete(total));
        }
        if len < 4 {
            return Err(SenkoError::Protocol("verbatim string too short"));
        }
        if &input[header_consumed + len..total] != b"\r\n" {
            return Err(SenkoError::Protocol(
                "verbatim string missing trailing CRLF",
            ));
        }
        let payload = &input[header_consumed..header_consumed + len];
        if payload.get(3).copied() != Some(b':') {
            return Err(SenkoError::Protocol(
                "verbatim string missing format separator",
            ));
        }
        Ok(ParseStatus::Complete(
            Frame::VerbatimString {
                encoding: &payload[..3],
                data: &payload[4..],
            },
            total,
        ))
    }

    fn parse_aggregate<'a>(
        &self,
        input: &'a [u8],
        kind: AggregateKind,
    ) -> SenkoResult<ParseStatus<'a>> {
        let Some((line, header_consumed)) = find_crlf(input, 1) else {
            return Ok(ParseStatus::Incomplete(input.len() + 2));
        };
        let len = parse_isize(line)?;
        if len < 0 {
            return Ok(ParseStatus::Complete(Frame::Null, header_consumed));
        }
        let len = len as usize;
        let expected = match kind {
            AggregateKind::Map => len.saturating_mul(2),
            AggregateKind::Array | AggregateKind::Set | AggregateKind::Push => len,
        };

        let mut consumed = header_consumed;
        let mut parsed = 0usize;
        while parsed < expected {
            match self.parse_frame(&input[consumed..], false)? {
                ParseStatus::Complete(_, used) => {
                    consumed += used;
                    parsed += 1;
                }
                ParseStatus::Incomplete(needed) => {
                    return Ok(ParseStatus::Incomplete(consumed + needed));
                }
            }
        }

        let aggregate = Aggregate::new(
            kind,
            len,
            &input[header_consumed..consumed],
            AggregateEncoding::Resp,
        );
        let frame = match kind {
            AggregateKind::Array => Frame::Array(aggregate),
            AggregateKind::Map => Frame::Map(aggregate),
            AggregateKind::Set => Frame::Set(aggregate),
            AggregateKind::Push => Frame::Push(aggregate),
        };
        Ok(ParseStatus::Complete(frame, consumed))
    }

    fn parse_inline<'a>(&self, input: &'a [u8]) -> SenkoResult<ParseStatus<'a>> {
        let Some((line, consumed)) = find_crlf(input, 0) else {
            return Ok(ParseStatus::Incomplete(input.len() + 2));
        };
        let trimmed = trim_ascii_whitespace(line);
        if trimmed.is_empty() {
            return Err(SenkoError::Protocol("empty inline command"));
        }
        let argc = count_inline_args(trimmed);
        let aggregate = Aggregate::new(
            AggregateKind::Array,
            argc,
            trimmed,
            AggregateEncoding::Inline,
        );
        Ok(ParseStatus::Complete(Frame::Array(aggregate), consumed))
    }
}

fn find_crlf(input: &[u8], start: usize) -> Option<(&[u8], usize)> {
    let rel = memchr(b'\r', input.get(start..)?)?;
    let pos = start + rel;
    if input.get(pos + 1).copied() != Some(b'\n') {
        return None;
    }
    Some((&input[start..pos], pos + 2))
}

fn trim_ascii_whitespace(mut input: &[u8]) -> &[u8] {
    while let Some((first, rest)) = input.split_first() {
        if !matches!(first, b' ' | b'\t') {
            break;
        }
        input = rest;
    }
    while let Some((&last, rest)) = input.split_last() {
        if !matches!(last, b' ' | b'\t') {
            break;
        }
        input = rest;
    }
    input
}

fn count_inline_args(input: &[u8]) -> usize {
    let mut count = 0usize;
    let mut in_token = false;
    for &byte in input {
        if matches!(byte, b' ' | b'\t') {
            in_token = false;
        } else if !in_token {
            in_token = true;
            count += 1;
        }
    }
    count
}

fn parse_i64(input: &[u8]) -> SenkoResult<i64> {
    Ok(std::str::from_utf8(input)?.parse::<i64>()?)
}

fn parse_isize(input: &[u8]) -> SenkoResult<isize> {
    Ok(std::str::from_utf8(input)?.parse::<isize>()?)
}

fn parse_usize(input: &[u8]) -> SenkoResult<usize> {
    Ok(std::str::from_utf8(input)?.parse::<usize>()?)
}
