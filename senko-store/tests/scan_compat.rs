mod support;

use std::collections::{BTreeSet, HashMap, HashSet};

use redis::Connection;

use support::{assert_err_contains, encoding, flush, must_connect};

fn populate(conn: &mut Connection, count: usize) {
    for index in 0..count {
        let key = format!("key:{index}");
        let value = index.to_string();
        let _: String = redis::cmd("SET").arg(&key).arg(&value).query(conn).unwrap();
    }
}

fn scan_collect(
    conn: &mut Connection,
    pattern: Option<&str>,
    type_filter: Option<&str>,
    count: Option<usize>,
) -> Vec<String> {
    let mut cursor = 0u64;
    let mut out = Vec::new();
    loop {
        let mut cmd = redis::cmd("SCAN");
        cmd.arg(cursor);
        if let Some(type_filter) = type_filter {
            cmd.arg("TYPE").arg(type_filter);
        }
        if let Some(pattern) = pattern {
            cmd.arg("MATCH").arg(pattern);
        }
        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }
        let (next, page): (u64, Vec<String>) = cmd.query(conn).unwrap();
        out.extend(page);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    out
}

fn sscan_collect(
    conn: &mut Connection,
    key: &str,
    pattern: Option<&str>,
    count: Option<usize>,
) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut out = Vec::new();
    loop {
        let mut cmd = redis::cmd("SSCAN");
        cmd.arg(key).arg(&cursor);
        if let Some(pattern) = pattern {
            cmd.arg("MATCH").arg(pattern);
        }
        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }
        let (next, page): (String, Vec<String>) = cmd.query(conn).unwrap();
        out.extend(page);
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    out
}

fn hscan_collect(
    conn: &mut Connection,
    key: &str,
    pattern: Option<&str>,
    count: Option<usize>,
    novalues: bool,
) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut out = Vec::new();
    loop {
        let mut cmd = redis::cmd("HSCAN");
        cmd.arg(key).arg(&cursor);
        if let Some(pattern) = pattern {
            cmd.arg("MATCH").arg(pattern);
        }
        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }
        if novalues {
            cmd.arg("NOVALUES");
        }
        let (next, page): (String, Vec<String>) = cmd.query(conn).unwrap();
        out.extend(page);
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    out
}

fn zscan_collect(
    conn: &mut Connection,
    key: &str,
    pattern: Option<&str>,
    count: Option<usize>,
) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut out = Vec::new();
    loop {
        let mut cmd = redis::cmd("ZSCAN");
        cmd.arg(key).arg(&cursor);
        if let Some(pattern) = pattern {
            cmd.arg("MATCH").arg(pattern);
        }
        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }
        let (next, page): (String, Vec<String>) = cmd.query(conn).unwrap();
        out.extend(page);
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    out
}

fn set_hash_entries(conn: &mut Connection, key: &str, count: usize) {
    let _: i64 = redis::cmd("DEL").arg(key).query(conn).unwrap();
    let mut cmd = redis::cmd("HSET");
    cmd.arg(key);
    for index in 0..count {
        let field = format!("key:{index}");
        let value = index.to_string();
        cmd.arg(field).arg(value);
    }
    let _: i64 = cmd.query(conn).unwrap();
}

fn zset_encoding_is_large(actual: &str) -> bool {
    actual.contains("skiplist") || actual.contains("bptree")
}

#[test]
#[ignore = "requires running Senko instance"]
fn scan_basic_count_match_and_type_filters() {
    let mut conn = must_connect();
    flush(&mut conn);
    populate(&mut conn, 1_000);

    let basic = scan_collect(&mut conn, None, None, None);
    assert_eq!(basic.into_iter().collect::<BTreeSet<_>>().len(), 1_000);

    let counted = scan_collect(&mut conn, None, None, Some(5));
    assert_eq!(counted.into_iter().collect::<BTreeSet<_>>().len(), 1_000);

    let matched = scan_collect(&mut conn, Some("key:1??"), None, None);
    assert_eq!(matched.into_iter().collect::<BTreeSet<_>>().len(), 100);

    let lists = scan_collect(&mut conn, None, Some("list"), None);
    assert!(lists.is_empty());

    let strings = scan_collect(&mut conn, None, Some("string"), None);
    assert_eq!(strings.len(), 1_000);

    let combined = scan_collect(&mut conn, Some("key:*"), Some("string"), Some(10));
    assert_eq!(combined.len(), 1_000);
}

#[test]
#[ignore = "requires running Senko instance"]
fn sscan_covers_intset_listpack_and_hashtable_encodings() {
    let mut conn = must_connect();
    flush(&mut conn);

    for (expected_encoding, prefix, count) in [
        ("intset", "", 100usize),
        ("listpack", "ele:", 100usize),
        ("hashtable", "ele:", 200usize),
    ] {
        let _: i64 = redis::cmd("DEL").arg("set").query(&mut conn).unwrap();
        let mut cmd = redis::cmd("SADD");
        cmd.arg("set");
        for index in 0..count {
            cmd.arg(format!("{prefix}{index}"));
        }
        let _: i64 = cmd.query(&mut conn).unwrap();

        assert!(
            encoding(&mut conn, "set").contains(expected_encoding),
            "expected {expected_encoding} for set"
        );

        let scanned = sscan_collect(&mut conn, "set", None, None);
        assert_eq!(scanned.into_iter().collect::<BTreeSet<_>>().len(), count);
    }
}

#[test]
#[ignore = "requires running Senko instance"]
fn hscan_covers_listpack_hashtable_and_novalues() {
    let mut conn = must_connect();
    flush(&mut conn);

    for (expected_encoding, count) in [("listpack", 30usize), ("hashtable", 1_000usize)] {
        set_hash_entries(&mut conn, "hash", count);

        assert!(
            encoding(&mut conn, "hash").contains(expected_encoding),
            "expected {expected_encoding} for hash"
        );

        let flat = hscan_collect(&mut conn, "hash", None, None, false);
        let mut found = BTreeSet::new();
        for pair in flat.chunks_exact(2) {
            assert_eq!(pair[0], format!("key:{}", pair[1]));
            found.insert(pair[0].clone());
        }
        assert_eq!(found.len(), count);

        let only_fields = hscan_collect(&mut conn, "hash", None, Some(1_000), true);
        assert_eq!(
            only_fields.into_iter().collect::<BTreeSet<_>>().len(),
            count
        );
    }
}

#[test]
#[ignore = "requires running Senko instance"]
fn hscan_large_value_and_pattern_cases_match_upstream() {
    let mut conn = must_connect();
    flush(&mut conn);

    for count in [60usize, 170usize] {
        let value1 = "1".repeat(count);
        let value2 = "2".repeat(count);
        let _: i64 = redis::cmd("DEL").arg("hash").query(&mut conn).unwrap();
        let _: i64 = redis::cmd("HSET")
            .arg("hash")
            .arg(&value1)
            .arg(&value1)
            .arg(&value2)
            .arg(&value2)
            .query(&mut conn)
            .unwrap();

        let mut full = hscan_collect(&mut conn, "hash", None, None, false);
        full.sort();
        assert_eq!(
            full,
            vec![
                value1.clone(),
                value1.clone(),
                value2.clone(),
                value2.clone()
            ]
        );

        let mut only_fields = hscan_collect(&mut conn, "hash", None, None, true);
        only_fields.sort();
        assert_eq!(only_fields, vec![value1, value2]);
    }

    let _: i64 = redis::cmd("DEL").arg("mykey").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("HSET")
        .arg("mykey")
        .arg("foo")
        .arg("1")
        .arg("fab")
        .arg("2")
        .arg("fiz")
        .arg("3")
        .arg("foobar")
        .arg("10")
        .arg("1")
        .arg("a")
        .arg("2")
        .arg("b")
        .arg("3")
        .arg("c")
        .arg("4")
        .arg("d")
        .query::<i64>(&mut conn)
        .unwrap();

    let mut matched = hscan_collect(&mut conn, "mykey", Some("foo*"), Some(10_000), false);
    matched.sort();
    assert_eq!(matched, vec!["1", "10", "foo", "foobar"]);

    let mut novalues = hscan_collect(&mut conn, "mykey", None, None, true);
    novalues.sort();
    assert_eq!(
        novalues,
        vec!["1", "2", "3", "4", "fab", "fiz", "foo", "foobar"]
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn zscan_covers_listpack_and_large_encodings() {
    let mut conn = must_connect();
    flush(&mut conn);

    for count in [30usize, 1_000usize] {
        let _: i64 = redis::cmd("DEL").arg("zset").query(&mut conn).unwrap();
        let mut cmd = redis::cmd("ZADD");
        cmd.arg("zset");
        for index in 0..count {
            cmd.arg(index).arg(format!("key:{index}"));
        }
        let _: i64 = cmd.query(&mut conn).unwrap();

        let actual_encoding = encoding(&mut conn, "zset");
        if count == 30 {
            assert!(
                actual_encoding.contains("listpack"),
                "expected listpack, got {actual_encoding}"
            );
        } else {
            assert!(
                zset_encoding_is_large(&actual_encoding),
                "expected large zset encoding, got {actual_encoding}"
            );
        }

        let flat = zscan_collect(&mut conn, "zset", None, None);
        let mut found = BTreeSet::new();
        for pair in flat.chunks_exact(2) {
            assert_eq!(pair[0], format!("key:{}", pair[1]));
            found.insert(pair[0].clone());
        }
        assert_eq!(found.len(), count);
    }
}

#[test]
#[ignore = "requires running Senko instance"]
fn scan_keeps_reporting_initial_keys_under_write_load() {
    let mut conn = must_connect();
    flush(&mut conn);
    populate(&mut conn, 100);

    let mut cursor = 0u64;
    let mut seen = Vec::new();
    loop {
        let (next, page): (u64, Vec<String>) =
            redis::cmd("SCAN").arg(cursor).query(&mut conn).unwrap();
        seen.extend(page);
        cursor = next;
        if cursor == 0 {
            break;
        }
        for suffix in 0..10 {
            let key = format!("addedkey:{cursor}:{suffix}");
            let _: String = redis::cmd("SET")
                .arg(&key)
                .arg("foo")
                .query(&mut conn)
                .unwrap();
        }
    }

    let initial = seen
        .into_iter()
        .filter(|key| key.starts_with("key:"))
        .collect::<BTreeSet<_>>();
    assert_eq!(initial.len(), 100);
}

#[test]
#[ignore = "requires running Senko instance"]
fn scan_pattern_variants_match_upstream_cases() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("DEL").arg("set").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("SADD")
        .arg("set")
        .arg("1")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let mut a_only = sscan_collect(&mut conn, "set", Some("*a*"), Some(100));
    a_only.sort();
    assert_eq!(a_only, vec!["a"]);

    let mut one_only = sscan_collect(&mut conn, "set", Some("*1*"), Some(100));
    one_only.sort();
    assert_eq!(one_only, vec!["1"]);

    let _: i64 = redis::cmd("DEL").arg("mykey").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("SADD")
        .arg("mykey")
        .arg("foo")
        .arg("fab")
        .arg("fiz")
        .arg("foobar")
        .arg("1")
        .arg("2")
        .arg("3")
        .arg("4")
        .query(&mut conn)
        .unwrap();
    let mut set_matches = sscan_collect(&mut conn, "mykey", Some("foo*"), Some(10_000));
    set_matches.sort();
    assert_eq!(set_matches, vec!["foo", "foobar"]);

    let _: i64 = redis::cmd("DEL").arg("mykey").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("mykey")
        .arg(1)
        .arg("foo")
        .arg(2)
        .arg("fab")
        .arg(3)
        .arg("fiz")
        .arg(10)
        .arg("foobar")
        .query::<i64>(&mut conn)
        .unwrap();
    let mut zset_matches = zscan_collect(&mut conn, "mykey", Some("foo*"), Some(10_000));
    zset_matches.sort();
    assert_eq!(zset_matches, vec!["10", "1", "foo", "foobar"]);
}

#[test]
#[ignore = "requires running Senko instance"]
fn zscan_preserves_tiny_nonzero_scores() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("DEL").arg("mykey").query(&mut conn).unwrap();
    for member in 0..500 {
        let _: i64 = redis::cmd("ZADD")
            .arg("mykey")
            .arg("9.8813129168249309e-323")
            .arg(member)
            .query(&mut conn)
            .unwrap();
    }

    let flat = zscan_collect(&mut conn, "mykey", None, None);
    let first_score = flat
        .chunks_exact(2)
        .next()
        .map(|pair| pair[1].clone())
        .unwrap();
    assert_ne!(first_score, "0");
}

#[test]
#[ignore = "requires running Senko instance"]
fn sscan_retains_survivors_during_shrink_regression() {
    let mut conn = must_connect();
    flush(&mut conn);

    for round in 0..100usize {
        let _: i64 = redis::cmd("DEL").arg("set").query(&mut conn).unwrap();
        let _: i64 = redis::cmd("SADD")
            .arg("set")
            .arg("x")
            .query(&mut conn)
            .unwrap();

        let numele = 101 + (round % 1000);
        let mut to_remove = Vec::new();
        let mut add = redis::cmd("SADD");
        add.arg("set");
        for value in 0..numele {
            add.arg(value);
            if value >= 100 {
                to_remove.push(value);
            }
        }
        let _: i64 = add.query(&mut conn).unwrap();

        let mut cursor = "0".to_string();
        let mut iteration = 0usize;
        let delete_iteration = round % 10;
        let mut found = HashSet::new();
        loop {
            let (next, items): (String, Vec<String>) = redis::cmd("SSCAN")
                .arg("set")
                .arg(&cursor)
                .query(&mut conn)
                .unwrap();
            for item in items {
                found.insert(item);
            }
            iteration += 1;
            if iteration == delete_iteration {
                let mut remove = redis::cmd("SREM");
                remove.arg("set");
                for value in &to_remove {
                    remove.arg(value);
                }
                let _: i64 = remove.query(&mut conn).unwrap();
            }
            cursor = next;
            if cursor == "0" {
                break;
            }
        }

        for expected in 0..100 {
            assert!(
                found.contains(&expected.to_string()),
                "SSCAN missing survivor {expected} in round {round}"
            );
        }
    }
}

#[test]
#[ignore = "requires running Senko instance"]
fn scan_match_with_hash_tag_only_returns_implied_slot() {
    let mut conn = must_connect();
    flush(&mut conn);

    for index in 0..100 {
        let _: String = redis::cmd("SET")
            .arg(format!("{{foo}}-{index}"))
            .arg("foo")
            .query(&mut conn)
            .unwrap();
        let _: String = redis::cmd("SET")
            .arg(format!("{{bar}}-{index}"))
            .arg("bar")
            .query(&mut conn)
            .unwrap();
        let _: String = redis::cmd("SET")
            .arg(format!("{{boo}}-{index}"))
            .arg("boo")
            .query(&mut conn)
            .unwrap();
    }

    let matches = scan_collect(&mut conn, Some("{foo}-*"), None, None);
    let unique = matches.into_iter().collect::<BTreeSet<_>>();
    assert_eq!(unique.len(), 100);
    assert!(unique.iter().all(|key| key.starts_with("{foo}-")));
}

#[test]
#[ignore = "requires running Senko instance"]
fn zscan_member_score_pairs_remain_consistent() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("DEL").arg("mykey").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("mykey")
        .arg(1)
        .arg("foo")
        .arg(2)
        .arg("fab")
        .arg(3)
        .arg("fiz")
        .arg(10)
        .arg("foobar")
        .query::<i64>(&mut conn)
        .unwrap();

    let flat = zscan_collect(&mut conn, "mykey", None, Some(10_000));
    let mut pairs = HashMap::new();
    for pair in flat.chunks_exact(2) {
        pairs.insert(pair[0].clone(), pair[1].clone());
    }
    assert_eq!(pairs.get("foo").map(String::as_str), Some("1"));
    assert_eq!(pairs.get("fab").map(String::as_str), Some("2"));
    assert_eq!(pairs.get("fiz").map(String::as_str), Some("3"));
    assert_eq!(pairs.get("foobar").map(String::as_str), Some("10"));
}

#[test]
#[ignore = "requires running Senko instance"]
fn scan_type_specific_commands_reject_wrongtype_keys() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("SET")
        .arg("not-a-collection")
        .arg("v")
        .query(&mut conn)
        .unwrap();

    assert_err_contains(
        redis::cmd("SSCAN")
            .arg("not-a-collection")
            .arg(0)
            .query::<(String, Vec<String>)>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("HSCAN")
            .arg("not-a-collection")
            .arg(0)
            .query::<(String, Vec<String>)>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("ZSCAN")
            .arg("not-a-collection")
            .arg(0)
            .query::<(String, Vec<String>)>(&mut conn),
        "WRONGTYPE",
    );
}
