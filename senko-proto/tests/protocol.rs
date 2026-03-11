use bytes::BytesMut;
use proptest::prelude::*;
use senko_proto::{
    Aggregate, AggregateEncoding, AggregateKind, Frame, ParseStatus, RespParser, RespSerializer,
};

fn assert_round_trip(frame: Frame<'_>, expected: &[u8]) {
    let parser = RespParser::new();
    let status = parser.parse(expected).expect("parse succeeds");
    let ParseStatus::Complete(parsed, consumed) = status else {
        panic!("frame should be complete");
    };
    assert_eq!(consumed, expected.len());
    assert_eq!(parsed, frame);

    let serialized = RespSerializer::serialize(&parsed);
    assert_eq!(serialized.as_ref(), expected);
}

#[test]
fn round_trip_all_resp_types() {
    assert_round_trip(Frame::SimpleString(b"OK"), b"+OK\r\n");
    assert_round_trip(Frame::SimpleError(b"ERR boom"), b"-ERR boom\r\n");
    assert_round_trip(Frame::Integer(42), b":42\r\n");
    assert_round_trip(Frame::BulkString(b"hello"), b"$5\r\nhello\r\n");
    assert_round_trip(Frame::Null, b"_\r\n");
    assert_round_trip(Frame::Boolean(true), b"#t\r\n");
    assert_round_trip(Frame::Boolean(false), b"#f\r\n");
    assert_round_trip(Frame::Double(1.5), b",1.5\r\n");
    assert_round_trip(
        Frame::BigNumber(b"3492890328409238509324850943850943825024385"),
        b"(3492890328409238509324850943850943825024385\r\n",
    );
    assert_round_trip(
        Frame::BlobError(b"SYNTAX invalid"),
        b"!14\r\nSYNTAX invalid\r\n",
    );
    assert_round_trip(
        Frame::VerbatimString {
            encoding: b"txt",
            data: b"hello",
        },
        b"=9\r\ntxt:hello\r\n",
    );

    let array = Frame::Array(Aggregate::new(
        AggregateKind::Array,
        2,
        b"$3\r\nGET\r\n$3\r\nkey\r\n",
        AggregateEncoding::Resp,
    ));
    assert_round_trip(array, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");

    let map = Frame::Map(Aggregate::new(
        AggregateKind::Map,
        1,
        b"+first\r\n:1\r\n",
        AggregateEncoding::Resp,
    ));
    assert_round_trip(map, b"%1\r\n+first\r\n:1\r\n");

    let set = Frame::Set(Aggregate::new(
        AggregateKind::Set,
        2,
        b"+a\r\n+b\r\n",
        AggregateEncoding::Resp,
    ));
    assert_round_trip(set, b"~2\r\n+a\r\n+b\r\n");

    let push = Frame::Push(Aggregate::new(
        AggregateKind::Push,
        2,
        b"+pubsub\r\n+message\r\n",
        AggregateEncoding::Resp,
    ));
    assert_round_trip(push, b">2\r\n+pubsub\r\n+message\r\n");
}

#[test]
fn parses_inline_command_without_allocation() {
    let parser = RespParser::new();
    let ParseStatus::Complete(Frame::Array(aggregate), consumed) = parser
        .parse(b"GET foo bar\r\n")
        .expect("inline parse succeeds")
    else {
        panic!("expected inline array");
    };

    assert_eq!(consumed, 13);
    assert_eq!(aggregate.kind(), AggregateKind::Array);
    assert_eq!(aggregate.encoding(), AggregateEncoding::Inline);
    assert_eq!(aggregate.len(), 3);

    let items: Vec<_> = aggregate
        .iter()
        .map(|item| match item.expect("token frame") {
            Frame::BulkString(token) => token,
            frame => panic!("unexpected inline token frame: {frame:?}"),
        })
        .collect();
    assert_eq!(
        items,
        vec![b"GET".as_slice(), b"foo".as_slice(), b"bar".as_slice()]
    );

    let serialized = RespSerializer::serialize(&Frame::Array(aggregate));
    assert_eq!(serialized.as_ref(), b"GET foo bar\r\n");
}

#[test]
fn helper_writers_emit_expected_bytes() {
    let mut out = BytesMut::new();
    RespSerializer::write_ok(&mut out);
    RespSerializer::write_integer(&mut out, 1);
    RespSerializer::write_nil_bulk(&mut out);
    RespSerializer::write_array_header(&mut out, 2);
    RespSerializer::write_bulk_string(&mut out, b"GET");
    RespSerializer::write_bulk_string(&mut out, b"foo");
    assert_eq!(
        out.as_ref(),
        b"+OK\r\n:1\r\n$-1\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
    );
}

fn frame_corpus() -> Vec<&'static [u8]> {
    vec![
        b"+OK\r\n",
        b"-ERR fail\r\n",
        b":123\r\n",
        b"$3\r\nfoo\r\n",
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
        b"_\r\n",
        b"#t\r\n",
        b",4.25\r\n",
        b"(12345678901234567890\r\n",
        b"!5\r\nerror\r\n",
        b"=8\r\ntxt:ok!\r\n",
        b"%1\r\n+first\r\n:1\r\n",
        b"~2\r\n+one\r\n+two\r\n",
        b">2\r\n+pubsub\r\n+msg\r\n",
        b"PING\r\n",
    ]
}

proptest! {
    #[test]
    fn truncated_frames_never_panic(case in 0usize..15, cut in 0usize..128) {
        let corpus = frame_corpus();
        let bytes = corpus[case % corpus.len()];
        let end = cut.min(bytes.len());
        let truncated = &bytes[..end];

        let outcome = std::panic::catch_unwind(|| RespParser::new().parse(truncated));
        let result = outcome.expect("parser must not panic").expect("truncation must not produce protocol error");
        match result {
            ParseStatus::Complete(_, consumed) => prop_assert_eq!(consumed, truncated.len()),
            ParseStatus::Incomplete(hint) => prop_assert!(hint >= truncated.len().saturating_add(1)),
        }
    }
}
