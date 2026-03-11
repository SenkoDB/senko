use std::{
    collections::HashMap,
    sync::{Arc, mpsc::{self, Receiver}},
    time::Duration,
};

use futures_util::{FutureExt, Stream, StreamExt};
use redis::{AsyncConnectionConfig, Msg, PushInfo, PushKind, RedisResult, Value};
use smol::Timer;

fn redis_url() -> String {
    std::env::var("senko_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

fn redis_url_resp3() -> String {
    let url = redis_url();
    if url.contains("protocol=3") {
        return url;
    }
    if url.contains('?') {
        format!("{url}&protocol=3")
    } else {
        format!("{url}?protocol=3")
    }
}

fn must_client(url: &str) -> redis::Client {
    redis::Client::open(url).expect("compat test requires valid redis url")
}

async fn flush_async(conn: &mut redis::aio::MultiplexedConnection) {
    if redis::cmd("FLUSHDB")
        .query_async::<()>(&mut *conn)
        .await
        .is_ok()
    {
        return;
    }
    let _: () = redis::cmd("FLUSHALL")
        .query_async(&mut *conn)
        .await
        .expect("compat test requires FLUSHDB or FLUSHALL support");
}

async fn async_pub_conn() -> redis::aio::MultiplexedConnection {
    must_client(&redis_url())
        .get_multiplexed_async_connection()
        .await
        .expect("compat test requires running senko at senko_REDIS_URL")
}

async fn async_pubsub() -> redis::aio::PubSub {
    must_client(&redis_url())
        .get_async_pubsub()
        .await
        .expect("compat test requires running senko at senko_REDIS_URL")
}

async fn resp3_push_conn() -> (redis::aio::MultiplexedConnection, Receiver<PushInfo>) {
    let client = must_client(&redis_url_resp3());
    let (tx, rx) = mpsc::channel();
    let config = AsyncConnectionConfig::new().set_push_sender(tx);
    let conn = client
        .get_multiplexed_async_connection_with_config(&config)
        .await
        .expect("compat test requires running senko at senko_REDIS_URL");
    (conn, rx)
}

async fn publish_bytes(
    conn: &mut redis::aio::MultiplexedConnection,
    channel: impl redis::ToRedisArgs,
    payload: impl redis::ToRedisArgs,
) -> i64 {
    redis::cmd("PUBLISH")
        .arg(channel)
        .arg(payload)
        .query_async(&mut *conn)
        .await
        .unwrap()
}

async fn spublish_bytes(
    conn: &mut redis::aio::MultiplexedConnection,
    channel: impl redis::ToRedisArgs,
    payload: impl redis::ToRedisArgs,
) -> i64 {
    redis::cmd("SPUBLISH")
        .arg(channel)
        .arg(payload)
        .query_async(&mut *conn)
        .await
        .unwrap()
}

async fn next_msg<S>(stream: &mut S) -> Msg
where
    S: Stream<Item = Msg> + Unpin,
{
    stream.next().await.expect("expected pub/sub message")
}

async fn assert_no_msg<S>(stream: &mut S)
where
    S: Stream<Item = Msg> + Unpin,
{
    Timer::after(Duration::from_millis(100)).await;
    assert!(stream.next().now_or_never().is_none(), "unexpected pub/sub message");
}

fn recv_push(rx: &Receiver<PushInfo>) -> PushInfo {
    rx.recv_timeout(Duration::from_secs(2))
        .expect("expected push frame")
}

fn assert_no_push(rx: &Receiver<PushInfo>) {
    assert!(
        rx.recv_timeout(Duration::from_millis(150)).is_err(),
        "unexpected push frame"
    );
}

fn push_as_msg(push: PushInfo) -> Msg {
    Msg::from_push_info(push).expect("expected pub/sub push message")
}

#[test]
#[ignore = "requires running senko instance"]
fn pubsub_basic_async_compat() {
    smol::block_on(async {
        let mut publisher = async_pub_conn().await;
        flush_async(&mut publisher).await;

        let pubsub = async_pubsub().await;
        let (mut sink, mut stream) = pubsub.split();
        sink.subscribe(&["basic:one", "basic:two"]).await.unwrap();

        let delivered = publish_bytes(&mut publisher, "basic:one", b"payload".to_vec()).await;
        assert_eq!(delivered, 1);
        let message = next_msg(&mut stream).await;
        assert_eq!(message.get_channel_name(), "basic:one");
        assert_eq!(message.get_payload_bytes(), b"payload");

        let delivered = publish_bytes(&mut publisher, "basic:other", b"skip".to_vec()).await;
        assert_eq!(delivered, 0);
        assert_no_msg(&mut stream).await;

        let delivered = publish_bytes(&mut publisher, "basic:two", b"second".to_vec()).await;
        assert_eq!(delivered, 1);
        let message = next_msg(&mut stream).await;
        assert_eq!(message.get_channel_name(), "basic:two");
        assert_eq!(message.get_payload_bytes(), b"second");

        sink.unsubscribe("basic:one").await.unwrap();
        let delivered = publish_bytes(&mut publisher, "basic:one", b"gone".to_vec()).await;
        assert_eq!(delivered, 0);
        assert_no_msg(&mut stream).await;
    });
}

#[test]
#[ignore = "requires running senko instance"]
fn pubsub_pattern_async_compat() {
    smol::block_on(async {
        let mut publisher = async_pub_conn().await;
        flush_async(&mut publisher).await;

        let pubsub = async_pubsub().await;
        let (mut sink, mut stream) = pubsub.split();
        sink.psubscribe("h?llo").await.unwrap();

        let delivered = publish_bytes(&mut publisher, "hello", b"a".to_vec()).await;
        assert_eq!(delivered, 1);
        let msg = next_msg(&mut stream).await;
        assert!(msg.from_pattern());
        assert_eq!(
            msg.get_pattern::<Option<String>>().unwrap(),
            Some("h?llo".to_string())
        );
        assert_eq!(msg.get_channel_name(), "hello");
        assert_eq!(msg.get_payload_bytes(), b"a");

        let delivered = publish_bytes(&mut publisher, "hxllo", b"b".to_vec()).await;
        assert_eq!(delivered, 1);
        let msg = next_msg(&mut stream).await;
        assert_eq!(msg.get_channel_name(), "hxllo");
        assert_eq!(msg.get_payload_bytes(), b"b");

        let delivered = publish_bytes(&mut publisher, "heello", b"c".to_vec()).await;
        assert_eq!(delivered, 0);
        assert_no_msg(&mut stream).await;

        sink.subscribe("news.sports").await.unwrap();
        sink.psubscribe("news.*").await.unwrap();
        let delivered = publish_bytes(&mut publisher, "news.sports", b"goal".to_vec()).await;
        assert_eq!(delivered, 2);

        let first = next_msg(&mut stream).await;
        let second = next_msg(&mut stream).await;
        let mut saw_exact = false;
        let mut saw_pattern = false;
        for msg in [first, second] {
            if msg.from_pattern() {
                saw_pattern = true;
                assert_eq!(
                    msg.get_pattern::<Option<String>>().unwrap(),
                    Some("news.*".to_string())
                );
            } else {
                saw_exact = true;
            }
            assert_eq!(msg.get_channel_name(), "news.sports");
            assert_eq!(msg.get_payload_bytes(), b"goal");
        }
        assert!(saw_exact && saw_pattern);

        sink.punsubscribe("h?llo").await.unwrap();
        sink.punsubscribe("news.*").await.unwrap();
        let delivered = publish_bytes(&mut publisher, "hello", b"after".to_vec()).await;
        assert_eq!(delivered, 0);
        assert_no_msg(&mut stream).await;
    });
}

#[test]
#[ignore = "requires running senko instance"]
fn pubsub_resp3_push_async_compat() {
    smol::block_on(async {
        let mut publisher = async_pub_conn().await;
        flush_async(&mut publisher).await;

        let (mut subscriber, rx) = resp3_push_conn().await;
        let _: Value = redis::cmd("HELLO")
            .arg(3)
            .query_async(&mut subscriber)
            .await
            .unwrap();
        subscriber.subscribe("resp3:chan").await.unwrap();

        let subscribe = recv_push(&rx);
        assert_eq!(subscribe.kind, PushKind::Subscribe);

        let delivered = publish_bytes(&mut publisher, "resp3:chan", b"push".to_vec()).await;
        assert_eq!(delivered, 1);

        let message = push_as_msg(recv_push(&rx));
        assert_eq!(message.get_channel_name(), "resp3:chan");
        assert_eq!(message.get_payload_bytes(), b"push");
    });
}

#[test]
#[ignore = "requires running senko instance"]
fn shard_pubsub_async_compat() {
    smol::block_on(async {
        let mut publisher = async_pub_conn().await;
        flush_async(&mut publisher).await;

        let (mut subscriber, rx) = resp3_push_conn().await;
        let _: Value = redis::cmd("HELLO")
            .arg(3)
            .query_async(&mut subscriber)
            .await
            .unwrap();
        let _: Value = redis::cmd("SSUBSCRIBE")
            .arg("shard:ch1")
            .query_async(&mut subscriber)
            .await
            .unwrap_or(Value::Nil);
        let subscribe = recv_push(&rx);
        assert_eq!(subscribe.kind, PushKind::SSubscribe);

        let delivered = spublish_bytes(&mut publisher, "shard:ch1", b"msg".to_vec()).await;
        assert_eq!(delivered, 1);
        let msg = push_as_msg(recv_push(&rx));
        assert_eq!(msg.get_channel_name(), "shard:ch1");
        assert_eq!(msg.get_payload_bytes(), b"msg");

        let delivered = spublish_bytes(&mut publisher, "shard:missing", b"msg".to_vec()).await;
        assert_eq!(delivered, 0);

        let delivered = publish_bytes(&mut publisher, "shard:ch1", b"global".to_vec()).await;
        assert_eq!(delivered, 0);
        assert_no_push(&rx);
    });
}

#[test]
#[ignore = "requires running senko instance"]
fn pubsub_edge_cases_async_compat() {
    smol::block_on(async {
        let mut publisher = async_pub_conn().await;
        flush_async(&mut publisher).await;

        let pubsub = async_pubsub().await;
        let (mut sink, mut stream) = pubsub.split();
        let long_channel = "c".repeat(512);
        sink.subscribe(&["edge:empty", "edge:binary", long_channel.as_str(), "edge:long"])
            .await
            .unwrap();

        let delivered = publish_bytes(&mut publisher, "edge:empty", Vec::<u8>::new()).await;
        assert_eq!(delivered, 1);
        let msg = next_msg(&mut stream).await;
        assert_eq!(msg.get_channel_name(), "edge:empty");
        assert_eq!(msg.get_payload_bytes(), b"");

        let binary = vec![0x00, 0xff, 0x80];
        let delivered = publish_bytes(&mut publisher, "edge:binary", binary.clone()).await;
        assert_eq!(delivered, 1);
        let msg = next_msg(&mut stream).await;
        assert_eq!(msg.get_channel_name(), "edge:binary");
        assert_eq!(msg.get_payload_bytes(), binary.as_slice());

        let delivered = publish_bytes(&mut publisher, &long_channel, b"wide".to_vec()).await;
        assert_eq!(delivered, 1);
        let msg = next_msg(&mut stream).await;
        assert_eq!(msg.get_channel::<String>().unwrap(), long_channel);
        assert_eq!(msg.get_payload_bytes(), b"wide");

        let huge = vec![b'x'; 1_048_576];
        let delivered = publish_bytes(&mut publisher, "edge:long", huge.clone()).await;
        assert_eq!(delivered, 1);
        let msg = next_msg(&mut stream).await;
        assert_eq!(msg.get_channel_name(), "edge:long");
        assert_eq!(msg.get_payload_bytes(), huge.as_slice());
    });
}

#[test]
#[ignore = "requires running senko instance"]
fn duplicate_subscribe_and_reset_async_compat() {
    smol::block_on(async {
        let mut publisher = async_pub_conn().await;
        flush_async(&mut publisher).await;

        let (mut subscriber, rx) = resp3_push_conn().await;
        let _: Value = redis::cmd("HELLO")
            .arg(3)
            .query_async(&mut subscriber)
            .await
            .unwrap();

        subscriber.subscribe("dup:chan").await.unwrap();
        assert_eq!(recv_push(&rx).kind, PushKind::Subscribe);
        subscriber.subscribe("dup:chan").await.unwrap();
        assert_eq!(recv_push(&rx).kind, PushKind::Subscribe);

        let delivered = publish_bytes(&mut publisher, "dup:chan", b"once".to_vec()).await;
        assert_eq!(delivered, 1);
        let msg = push_as_msg(recv_push(&rx));
        assert_eq!(msg.get_channel_name(), "dup:chan");
        assert_eq!(msg.get_payload_bytes(), b"once");
        assert_no_push(&rx);

        let reset: String = redis::cmd("RESET")
            .query_async(&mut subscriber)
            .await
            .unwrap();
        assert_eq!(reset, "RESET");
        let pong: String = redis::cmd("PING")
            .query_async(&mut subscriber)
            .await
            .unwrap();
        assert_eq!(pong, "PONG");

        let delivered = publish_bytes(&mut publisher, "dup:chan", b"after-reset".to_vec()).await;
        assert_eq!(delivered, 0);
        assert_no_push(&rx);
    });
}

#[test]
#[ignore = "requires running senko instance"]
fn quit_in_pubsub_mode_async_compat() {
    smol::block_on(async {
        let mut publisher = async_pub_conn().await;
        flush_async(&mut publisher).await;

        let (mut subscriber, rx) = resp3_push_conn().await;
        let _: Value = redis::cmd("HELLO")
            .arg(3)
            .query_async(&mut subscriber)
            .await
            .unwrap();
        subscriber.subscribe("quit:chan").await.unwrap();
        assert_eq!(recv_push(&rx).kind, PushKind::Subscribe);

        let quit: RedisResult<String> = redis::cmd("QUIT").query_async(&mut subscriber).await;
        if let Ok(reply) = quit {
            assert_eq!(reply, "OK");
        }
        Timer::after(Duration::from_millis(100)).await;
        let delivered = publish_bytes(&mut publisher, "quit:chan", b"after".to_vec()).await;
        assert_eq!(delivered, 0);
    });
}

#[test]
#[ignore = "requires running senko instance"]
fn pubsub_multi_subscriber_async_compat() {
    smol::block_on(async {
        let mut publisher = async_pub_conn().await;
        flush_async(&mut publisher).await;

        let mut subscribers = Vec::with_capacity(100);
        for _ in 0..100usize {
            let pubsub = async_pubsub().await;
            let (mut sink, stream) = pubsub.split();
            sink.subscribe("fanout:100").await.unwrap();
            subscribers.push((sink, stream));
        }

        let payload = b"broadcast".to_vec();
        let delivered = publish_bytes(&mut publisher, "fanout:100", payload.clone()).await;
        assert_eq!(delivered, 100);

        for (_, stream) in &mut subscribers {
            let msg = stream.next().await.expect("subscriber message");
            assert_eq!(msg.get_channel_name(), "fanout:100");
            assert_eq!(msg.get_payload_bytes(), payload.as_slice());
        }
    });
}

#[test]
#[ignore = "requires running senko instance"]
fn pubsub_concurrent_async_compat() {
    smol::block_on(async {
        const PUBLISHERS: usize = 10;
        const SUBSCRIBERS: usize = 10;
        const CHANNELS: usize = 100;
        const MESSAGES_PER_CHANNEL: usize = 1_000;

        let mut control = async_pub_conn().await;
        flush_async(&mut control).await;
        let channels = Arc::new(
            (0..CHANNELS)
                .map(|idx| format!("concurrent:ch:{idx}"))
                .collect::<Vec<_>>(),
        );

        let mut subscriber_tasks = Vec::with_capacity(SUBSCRIBERS);
        for _ in 0..SUBSCRIBERS {
            let pubsub = async_pubsub().await;
            let (mut sink, mut stream) = pubsub.split();
            sink.subscribe(channels.as_ref().as_slice()).await.unwrap();
            let channels = Arc::clone(&channels);
            subscriber_tasks.push(smol::spawn(async move {
                let total = PUBLISHERS * CHANNELS * MESSAGES_PER_CHANNEL;
                let mut expected = HashMap::<(usize, String), usize>::new();
                for channel in channels.iter() {
                    for publisher in 0..PUBLISHERS {
                        expected.insert((publisher, channel.clone()), 0);
                    }
                }
                for _ in 0..total {
                    let msg = stream.next().await.expect("expected concurrent message");
                    let channel = msg.get_channel::<String>().unwrap();
                    let payload = std::str::from_utf8(msg.get_payload_bytes()).unwrap();
                    let mut parts = payload.split(':');
                    let publisher = parts.next().unwrap().trim_start_matches('p').parse::<usize>().unwrap();
                    let sequence = parts.next().unwrap().parse::<usize>().unwrap();
                    let entry = expected
                        .get_mut(&(publisher, channel.clone()))
                        .expect("publisher/channel tuple present");
                    assert_eq!(*entry, sequence, "per-publisher ordering violated on {channel}");
                    *entry += 1;
                }
                for ((publisher, channel), seen) in expected {
                    assert_eq!(
                        seen,
                        MESSAGES_PER_CHANNEL,
                        "missing messages for publisher {publisher} on {channel}"
                    );
                }
            }));
        }

        let mut publisher_tasks = Vec::with_capacity(PUBLISHERS);
        for publisher in 0..PUBLISHERS {
            let channels = Arc::clone(&channels);
            publisher_tasks.push(smol::spawn(async move {
                let mut conn = async_pub_conn().await;
                for sequence in 0..MESSAGES_PER_CHANNEL {
                    let payload = format!("p{publisher}:{sequence}");
                    for channel in channels.iter() {
                        let delivered = publish_bytes(&mut conn, channel.as_str(), payload.as_bytes()).await;
                        assert_eq!(delivered, SUBSCRIBERS as i64);
                    }
                }
            }));
        }

        for task in publisher_tasks {
            task.await;
        }
        for task in subscriber_tasks {
            task.await;
        }
    });
}

#[test]
#[ignore = "requires running senko instance"]
fn pubsub_introspection_async_compat() {
    smol::block_on(async {
        let mut publisher = async_pub_conn().await;
        flush_async(&mut publisher).await;

        let channels: Vec<String> = redis::cmd("PUBSUB")
            .arg("CHANNELS")
            .query_async(&mut publisher)
            .await
            .unwrap();
        assert!(channels.is_empty());

        let pubsub = async_pubsub().await;
        let (mut sink, _stream) = pubsub.split();
        sink.subscribe(&["introspect:one", "introspect:two"]).await.unwrap();
        sink.psubscribe("introspect:*").await.unwrap();
        Timer::after(Duration::from_millis(100)).await;

        let channels: Vec<String> = redis::cmd("PUBSUB")
            .arg("CHANNELS")
            .query_async(&mut publisher)
            .await
            .unwrap();
        assert!(channels.iter().any(|channel| channel == "introspect:one"));
        assert!(channels.iter().any(|channel| channel == "introspect:two"));

        let filtered: Vec<String> = redis::cmd("PUBSUB")
            .arg("CHANNELS")
            .arg("introspect:o*")
            .query_async(&mut publisher)
            .await
            .unwrap();
        assert_eq!(filtered, vec!["introspect:one".to_string()]);

        let numsub: Vec<Value> = redis::cmd("PUBSUB")
            .arg("NUMSUB")
            .arg("introspect:one")
            .arg("introspect:two")
            .query_async(&mut publisher)
            .await
            .unwrap();
        assert_eq!(numsub.len(), 4);
        let pairs: Vec<(String, i64)> = numsub
            .chunks_exact(2)
            .map(|chunk| {
                let channel = redis::from_redis_value::<String>(chunk[0].clone()).unwrap();
                let count = redis::from_redis_value::<i64>(chunk[1].clone()).unwrap();
                (channel, count)
            })
            .collect();
        assert_eq!(pairs, vec![("introspect:one".to_string(), 1), ("introspect:two".to_string(), 1)]);

        let numpat: i64 = redis::cmd("PUBSUB")
            .arg("NUMPAT")
            .query_async(&mut publisher)
            .await
            .unwrap();
        assert_eq!(numpat, 1);
    });
}
