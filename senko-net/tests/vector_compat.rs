#![allow(clippy::too_many_lines)]

use std::{
    collections::HashSet,
    sync::{Arc, Barrier},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use redis::{Connection, RedisResult, Value};

fn redis_url() -> String {
    std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

fn connect() -> Option<Connection> {
    let client = redis::Client::open(redis_url()).ok()?;
    client.get_connection().ok()
}

fn must_connect() -> Connection {
    match connect() {
        Some(mut conn) => {
            let _: RedisResult<String> = redis::cmd("PING").query(&mut conn);
            conn
        }
        None => panic!("vector compat test requires running Senko at SENKO_REDIS_URL"),
    }
}

fn flush(conn: &mut Connection) {
    if redis::cmd("FLUSHDB").query::<()>(conn).is_ok() {
        return;
    }
    let _: () = redis::cmd("FLUSHALL")
        .query(conn)
        .expect("compat test requires FLUSHDB or FLUSHALL");
}

fn unique(prefix: &str) -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("vector:{prefix}:{stamp}")
}

fn fp32_blob(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

fn as_string(value: Value) -> String {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes).unwrap(),
        Value::SimpleString(text) => text,
        Value::Okay => "OK".to_string(),
        Value::Int(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        other => panic!("expected string-like value, got {other:?}"),
    }
}

fn as_f64(value: Value) -> f64 {
    match value {
        Value::Double(value) => value,
        Value::Int(value) => value as f64,
        Value::BulkString(bytes) => String::from_utf8(bytes).unwrap().parse().unwrap(),
        Value::SimpleString(text) => text.parse().unwrap(),
        other => panic!("expected float-like value, got {other:?}"),
    }
}

fn as_array(value: Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values,
        other => panic!("expected array, got {other:?}"),
    }
}

fn assert_err_contains<T: std::fmt::Debug>(result: RedisResult<T>, needle: &str) {
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(needle),
        "expected error containing {needle:?}, got {err}"
    );
}

#[derive(Debug)]
struct VsimResult {
    element: String,
    score: Option<f64>,
    attrs: Option<String>,
}

fn parse_vsim(value: Value, with_scores: bool, with_attribs: bool) -> Vec<VsimResult> {
    as_array(value)
        .into_iter()
        .map(|entry| match entry {
            Value::Array(parts) => {
                let mut iter = parts.into_iter();
                let element = as_string(iter.next().unwrap());
                let score = with_scores.then(|| as_f64(iter.next().unwrap()));
                let attrs = with_attribs.then(|| as_string(iter.next().unwrap()));
                VsimResult {
                    element,
                    score,
                    attrs,
                }
            }
            other => {
                assert!(!with_scores && !with_attribs);
                VsimResult {
                    element: as_string(other),
                    score: None,
                    attrs: None,
                }
            }
        })
        .collect()
}

fn parse_strings(value: Value) -> Vec<String> {
    as_array(value).into_iter().map(as_string).collect()
}

#[test]
#[ignore = "requires running Senko with vector module enabled"]
fn vector_basic_shape_and_type_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let key = unique("basic");
    let added: i64 = redis::cmd("VADD")
        .arg(&key)
        .arg("VALUES")
        .arg(4)
        .arg(1.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg("alpha")
        .query(&mut conn)
        .unwrap();
    assert_eq!(added, 1);

    let type_name: String = redis::cmd("TYPE").arg(&key).query(&mut conn).unwrap();
    assert_eq!(type_name, "vectorset");

    let card: i64 = redis::cmd("VCARD").arg(&key).query(&mut conn).unwrap();
    let dim: i64 = redis::cmd("VDIM").arg(&key).query(&mut conn).unwrap();
    let is_member: i64 = redis::cmd("VISMEMBER")
        .arg(&key)
        .arg("alpha")
        .query(&mut conn)
        .unwrap();
    assert_eq!(card, 1);
    assert_eq!(dim, 4);
    assert_eq!(is_member, 1);

    let info = parse_strings(redis::cmd("VINFO").arg(&key).query(&mut conn).unwrap());
    assert!(info.iter().any(|item| item == "quant-type"));
    assert!(info.iter().any(|item| item == "vector-dim"));
    assert!(info.iter().any(|item| item == "hnsw-m"));
    assert!(info.iter().any(|item| item == "size"));
    assert!(info.iter().any(|item| item == "vset-uid"));
}

#[test]
#[ignore = "requires running Senko with vector module enabled"]
fn vector_q8_and_raw_embedding_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let key = unique("q8");
    let src = [0.25_f32, -0.75, 1.50, 0.125];
    let added: i64 = redis::cmd("VADD")
        .arg(&key)
        .arg("FP32")
        .arg(fp32_blob(&src))
        .arg("item")
        .arg("Q8")
        .query(&mut conn)
        .unwrap();
    assert_eq!(added, 1);

    let emb = as_array(
        redis::cmd("VEMB")
            .arg(&key)
            .arg("item")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(emb.len(), src.len());
    for (got, expected) in emb.into_iter().map(as_f64).zip(src) {
        let delta = (got - f64::from(expected)).abs();
        assert!(delta <= 0.05, "delta {delta} too large for {expected}");
    }

    let raw: Vec<u8> = redis::cmd("VEMB")
        .arg(&key)
        .arg("item")
        .arg("RAW")
        .query(&mut conn)
        .unwrap();
    assert_eq!(raw.len(), src.len() + 8);
}

#[test]
#[ignore = "requires running Senko with vector module enabled"]
fn vector_bin_quantization_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let key = unique("bin");
    let added: i64 = redis::cmd("VADD")
        .arg(&key)
        .arg("VALUES")
        .arg(10)
        .arg(-1.0)
        .arg(1.0)
        .arg(1.0)
        .arg(-1.0)
        .arg(1.0)
        .arg(-1.0)
        .arg(-1.0)
        .arg(1.0)
        .arg(1.0)
        .arg(1.0)
        .arg("bits")
        .arg("BIN")
        .query(&mut conn)
        .unwrap();
    assert_eq!(added, 1);

    let raw: Vec<u8> = redis::cmd("VEMB")
        .arg(&key)
        .arg("bits")
        .arg("RAW")
        .query(&mut conn)
        .unwrap();
    assert_eq!(raw.len(), 2);
}

#[test]
#[ignore = "requires running Senko with vector module enabled"]
fn vector_vsim_withscores_truth_and_self_match() {
    let mut conn = must_connect();
    flush(&mut conn);

    let key = unique("vsim");
    let _: i64 = redis::cmd("VADD")
        .arg(&key)
        .arg("VALUES")
        .arg(4)
        .arg(1.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg("x")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("VADD")
        .arg(&key)
        .arg("VALUES")
        .arg(4)
        .arg(0.0)
        .arg(1.0)
        .arg(0.0)
        .arg(0.0)
        .arg("y")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("VADD")
        .arg(&key)
        .arg("VALUES")
        .arg(4)
        .arg(0.0)
        .arg(0.0)
        .arg(1.0)
        .arg(0.0)
        .arg("z")
        .query(&mut conn)
        .unwrap();

    let reply = redis::cmd("VSIM")
        .arg(&key)
        .arg("VALUES")
        .arg(4)
        .arg(1.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg("WITHSCORES")
        .arg("COUNT")
        .arg(3)
        .arg("TRUTH")
        .query(&mut conn)
        .unwrap();
    let parsed = parse_vsim(reply, true, false);
    assert_eq!(parsed[0].element, "x");
    assert!(parsed[0].score.unwrap() > 0.99);
}

#[test]
#[ignore = "requires running Senko with vector module enabled"]
fn vector_attrs_and_filter_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let key = unique("attrs");
    let _: i64 = redis::cmd("VADD")
        .arg(&key)
        .arg("VALUES")
        .arg(3)
        .arg(1.0)
        .arg(0.0)
        .arg(0.0)
        .arg("apple")
        .arg("SETATTR")
        .arg(r#"{"category":"food","price":5}"#)
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("VADD")
        .arg(&key)
        .arg("VALUES")
        .arg(3)
        .arg(0.9)
        .arg(0.1)
        .arg(0.0)
        .arg("hammer")
        .arg("SETATTR")
        .arg(r#"{"category":"tool","price":25}"#)
        .query(&mut conn)
        .unwrap();

    let attrs: String = redis::cmd("VGETATTR")
        .arg(&key)
        .arg("apple")
        .query(&mut conn)
        .unwrap();
    assert!(attrs.contains("\"food\""));

    let filtered = parse_vsim(
        redis::cmd("VSIM")
            .arg(&key)
            .arg("ELE")
            .arg("apple")
            .arg("WITHATTRIBS")
            .arg("COUNT")
            .arg(10)
            .arg("FILTER")
            .arg(r#".category == "food""#)
            .query(&mut conn)
            .unwrap(),
        false,
        true,
    );
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].element, "apple");
    assert!(filtered[0].attrs.as_ref().unwrap().contains("\"food\""));

    let _: String = redis::cmd("VSETATTR")
        .arg(&key)
        .arg("apple")
        .arg(r#"{"category":"fruit","price":7}"#)
        .query(&mut conn)
        .unwrap();
    let updated: String = redis::cmd("VGETATTR")
        .arg(&key)
        .arg("apple")
        .query(&mut conn)
        .unwrap();
    assert!(updated.contains("\"fruit\""));
}

#[test]
#[ignore = "requires running Senko with vector module enabled"]
fn vector_remove_randmember_and_range_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let key = unique("setops");
    for name in ["anna", "bravo", "cello", "delta", "echo"] {
        let _: i64 = redis::cmd("VADD")
            .arg(&key)
            .arg("VALUES")
            .arg(2)
            .arg(name.len() as f64)
            .arg(1.0)
            .arg(name)
            .query(&mut conn)
            .unwrap();
    }

    let range = parse_strings(
        redis::cmd("VRANGE")
            .arg(&key)
            .arg("bravo")
            .arg("delta")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(range, vec!["bravo", "cello", "delta"]);

    let sample: String = redis::cmd("VRANDMEMBER")
        .arg(&key)
        .query(&mut conn)
        .unwrap();
    assert!(["anna", "bravo", "cello", "delta", "echo"].contains(&sample.as_str()));

    let distinct = parse_strings(
        redis::cmd("VRANDMEMBER")
            .arg(&key)
            .arg(3)
            .query(&mut conn)
            .unwrap(),
    );
    let uniq: HashSet<_> = distinct.iter().collect();
    assert_eq!(uniq.len(), distinct.len());

    let removed: i64 = redis::cmd("VREM")
        .arg(&key)
        .arg("cello")
        .query(&mut conn)
        .unwrap();
    assert_eq!(removed, 1);
    let is_member: i64 = redis::cmd("VISMEMBER")
        .arg(&key)
        .arg("cello")
        .query(&mut conn)
        .unwrap();
    assert_eq!(is_member, 0);
}

#[test]
#[ignore = "requires running Senko with vector module enabled"]
fn vector_validation_errors_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let key = unique("errors");
    let _: i64 = redis::cmd("VADD")
        .arg(&key)
        .arg("VALUES")
        .arg(4)
        .arg(1.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg("base")
        .arg("Q8")
        .query(&mut conn)
        .unwrap();

    assert_err_contains(
        redis::cmd("VADD")
            .arg(&key)
            .arg("VALUES")
            .arg(3)
            .arg(1.0)
            .arg(0.0)
            .arg(0.0)
            .arg("bad")
            .query::<i64>(&mut conn),
        "dimension",
    );
    assert_err_contains(
        redis::cmd("VADD")
            .arg(&key)
            .arg("FP32")
            .arg(vec![1_u8, 2, 3])
            .arg("badblob")
            .query::<i64>(&mut conn),
        "FP32 blob",
    );
    assert_err_contains(
        redis::cmd("VADD")
            .arg(&key)
            .arg("VALUES")
            .arg(4)
            .arg(1.0)
            .arg(0.0)
            .arg(0.0)
            .arg(0.0)
            .arg("badquant")
            .arg("BIN")
            .query::<i64>(&mut conn),
        "Quantization",
    );
}

#[test]
#[ignore = "requires running Senko with vector module enabled"]
fn vector_reduce_projection_smoke_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let key = unique("reduce");
    let _: i64 = redis::cmd("VADD")
        .arg(&key)
        .arg("REDUCE")
        .arg(4)
        .arg("VALUES")
        .arg(8)
        .arg(1.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg("left")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("VADD")
        .arg(&key)
        .arg("VALUES")
        .arg(8)
        .arg(0.0)
        .arg(1.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg(0.0)
        .arg("right")
        .query(&mut conn)
        .unwrap();

    let dim: i64 = redis::cmd("VDIM").arg(&key).query(&mut conn).unwrap();
    assert_eq!(dim, 4);

    let results = parse_vsim(
        redis::cmd("VSIM")
            .arg(&key)
            .arg("VALUES")
            .arg(8)
            .arg(1.0)
            .arg(0.0)
            .arg(0.0)
            .arg(0.0)
            .arg(0.0)
            .arg(0.0)
            .arg(0.0)
            .arg(0.0)
            .arg("COUNT")
            .arg(1)
            .query(&mut conn)
            .unwrap(),
        false,
        false,
    );
    assert_eq!(results[0].element, "left");
}

#[test]
#[ignore = "requires running Senko with vector module enabled"]
fn vector_cas_concurrent_insert_smoke() {
    let mut conn = must_connect();
    flush(&mut conn);

    let key = unique("cas");
    let barrier = Arc::new(Barrier::new(8));
    let mut handles = Vec::new();
    for idx in 0..8_u32 {
        let url = redis_url();
        let key = key.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let client = redis::Client::open(url).unwrap();
            let mut conn = client.get_connection().unwrap();
            barrier.wait();
            for item in 0..16_u32 {
                let name = format!("e:{idx}:{item}");
                let added: i64 = redis::cmd("VADD")
                    .arg(&key)
                    .arg("VALUES")
                    .arg(4)
                    .arg(idx as f64)
                    .arg(item as f64)
                    .arg(1.0)
                    .arg(0.0)
                    .arg(name)
                    .arg("CAS")
                    .query(&mut conn)
                    .unwrap();
                assert!(added == 0 || added == 1);
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    let card: i64 = redis::cmd("VCARD").arg(&key).query(&mut conn).unwrap();
    assert_eq!(card, 128);
}

#[test]
#[ignore = "requires running Senko with vector module enabled"]
fn vector_vsim_threading_smoke() {
    let mut conn = must_connect();
    flush(&mut conn);

    let key = unique("threads");
    for idx in 0..64_u32 {
        let _: i64 = redis::cmd("VADD")
            .arg(&key)
            .arg("VALUES")
            .arg(4)
            .arg(idx as f64)
            .arg(1.0)
            .arg(0.0)
            .arg(0.0)
            .arg(format!("e{idx:03}"))
            .query(&mut conn)
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(33));
    let mut handles = Vec::new();
    for _ in 0..33 {
        let url = redis_url();
        let key = key.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let client = redis::Client::open(url).unwrap();
            let mut conn = client.get_connection().unwrap();
            barrier.wait();
            let value = redis::cmd("VSIM")
                .arg(&key)
                .arg("VALUES")
                .arg(4)
                .arg(0.0)
                .arg(1.0)
                .arg(0.0)
                .arg(0.0)
                .arg("COUNT")
                .arg(5)
                .query::<Value>(&mut conn)
                .unwrap();
            parse_vsim(value, false, false)
        }));
    }

    let mut completed = 0usize;
    for handle in handles {
        let rows = handle.join().unwrap();
        assert!(!rows.is_empty());
        completed += 1;
    }
    assert_eq!(completed, 33);
}

#[test]
#[ignore = "requires running Senko with vector module enabled"]
fn vector_links_and_search_excludes_removed_members() {
    let mut conn = must_connect();
    flush(&mut conn);

    let key = unique("links");
    for (name, vec) in [
        ("aa", [1.0, 0.0, 0.0, 0.0]),
        ("ab", [0.9, 0.1, 0.0, 0.0]),
        ("ac", [0.8, 0.2, 0.0, 0.0]),
        ("zz", [0.0, 0.0, 1.0, 0.0]),
    ] {
        let _: i64 = redis::cmd("VADD")
            .arg(&key)
            .arg("VALUES")
            .arg(4)
            .arg(vec[0])
            .arg(vec[1])
            .arg(vec[2])
            .arg(vec[3])
            .arg(name)
            .query(&mut conn)
            .unwrap();
    }

    let links = redis::cmd("VLINKS")
        .arg(&key)
        .arg("aa")
        .arg("WITHSCORES")
        .query::<Value>(&mut conn)
        .unwrap();
    assert!(matches!(links, Value::Array(_)));

    let removed: i64 = redis::cmd("VREM")
        .arg(&key)
        .arg("ab")
        .query(&mut conn)
        .unwrap();
    assert_eq!(removed, 1);

    let results = parse_vsim(
        redis::cmd("VSIM")
            .arg(&key)
            .arg("ELE")
            .arg("aa")
            .arg("WITHSCORES")
            .arg("COUNT")
            .arg(10)
            .query(&mut conn)
            .unwrap(),
        true,
        false,
    );
    assert!(!results.iter().any(|row| row.element == "ab"));
    assert!(results.iter().all(|row| row.score.is_some()));
}
