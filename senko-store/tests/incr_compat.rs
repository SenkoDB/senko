mod support;

use redis::{Connection, Value};

use support::{assert_err_contains, encoding, flush, must_connect};

fn set_string(conn: &mut Connection, key: &str, value: impl ToString) {
    let _: String = redis::cmd("SET")
        .arg(key)
        .arg(value.to_string())
        .query(conn)
        .unwrap();
}

fn get_string(conn: &mut Connection, key: &str) -> Option<String> {
    redis::cmd("GET").arg(key).query(conn).unwrap()
}

#[test]
#[ignore = "requires running Senko instance"]
fn incr_and_decr_basic_paths_match_redis() {
    let mut conn = must_connect();
    flush(&mut conn);

    assert_eq!(
        redis::cmd("INCR")
            .arg("novar")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(get_string(&mut conn, "novar"), Some("1".to_string()));

    assert_eq!(
        redis::cmd("INCR")
            .arg("novar")
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_eq!(
        redis::cmd("DECR")
            .arg("novar")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );

    let _: i64 = redis::cmd("DEL")
        .arg("novar_not_exist")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("DECR")
            .arg("novar_not_exist")
            .query::<i64>(&mut conn)
            .unwrap(),
        -1
    );
    assert_eq!(
        redis::cmd("INCR")
            .arg("novar_not_exist")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    set_string(&mut conn, "novar", 100);
    assert_eq!(
        redis::cmd("INCR")
            .arg("novar")
            .query::<i64>(&mut conn)
            .unwrap(),
        101
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn incr_and_decr_support_large_integer_ranges() {
    let mut conn = must_connect();
    flush(&mut conn);

    set_string(&mut conn, "novar", 17_179_869_184_i64);
    assert_eq!(
        redis::cmd("INCR")
            .arg("novar")
            .query::<i64>(&mut conn)
            .unwrap(),
        17_179_869_185_i64
    );

    set_string(&mut conn, "novar", 17_179_869_184_i64);
    assert_eq!(
        redis::cmd("INCRBY")
            .arg("novar")
            .arg(17_179_869_184_i64)
            .query::<i64>(&mut conn)
            .unwrap(),
        34_359_738_368_i64
    );

    set_string(&mut conn, "novar", 17_179_869_184_i64);
    assert_eq!(
        redis::cmd("DECRBY")
            .arg("novar")
            .arg(17_179_869_185_i64)
            .query::<i64>(&mut conn)
            .unwrap(),
        -1
    );

    let _: i64 = redis::cmd("DEL")
        .arg("key_not_exist")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("DECRBY")
            .arg("key_not_exist")
            .arg(1)
            .query::<i64>(&mut conn)
            .unwrap(),
        -1
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn incr_family_rejects_invalid_integer_inputs() {
    let mut conn = must_connect();
    flush(&mut conn);

    for value in ["    11", "11    ", "    11    "] {
        set_string(&mut conn, "novar", value);
        assert_err_contains(
            redis::cmd("INCR").arg("novar").query::<i64>(&mut conn),
            "ERR value is not an integer or out of range",
        );
    }

    set_string(&mut conn, "x", 0);
    assert_err_contains(
        redis::cmd("DECRBY")
            .arg("x")
            .arg("-9223372036854775808")
            .query::<i64>(&mut conn),
        "ERR value is not an integer or out of range",
    );

    let _: i64 = redis::cmd("DEL").arg("mykeyincr").query(&mut conn).unwrap();
    assert_err_contains(
        redis::cmd("INCR")
            .arg("mykeyincr")
            .arg("v")
            .query::<Value>(&mut conn),
        "ERR wrong number of arguments for 'incr' command",
    );
    assert_err_contains(
        redis::cmd("DECR")
            .arg("mykeyincr")
            .arg("v")
            .query::<Value>(&mut conn),
        "ERR wrong number of arguments for 'decr' command",
    );
    assert_err_contains(
        redis::cmd("INCRBY")
            .arg("mykeyincr")
            .arg("v")
            .query::<i64>(&mut conn),
        "ERR value is not an integer or out of range",
    );
    assert_err_contains(
        redis::cmd("INCRBY")
            .arg("mykeyincr")
            .arg("1.5")
            .query::<i64>(&mut conn),
        "ERR value is not an integer or out of range",
    );
    assert_err_contains(
        redis::cmd("DECRBY")
            .arg("mykeyincr")
            .arg("v")
            .query::<i64>(&mut conn),
        "ERR value is not an integer or out of range",
    );
    assert_err_contains(
        redis::cmd("DECRBY")
            .arg("mykeyincr")
            .arg("1.5")
            .query::<i64>(&mut conn),
        "ERR value is not an integer or out of range",
    );
    assert_err_contains(
        redis::cmd("INCRBYFLOAT")
            .arg("mykeyincr")
            .arg("v")
            .query::<String>(&mut conn),
        "ERR value is not a valid float",
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn incr_family_rejects_wrongtype_keys() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg(1)
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("INCR").arg("mylist").query::<i64>(&mut conn),
        "WRONGTYPE",
    );
    let _: i64 = redis::cmd("DEL").arg("mylist").query(&mut conn).unwrap();

    let _: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg(1)
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("INCRBYFLOAT")
            .arg("mylist")
            .arg("1.0")
            .query::<String>(&mut conn),
        "WRONGTYPE",
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn incrbyfloat_paths_match_redis() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("DEL").arg("novar").query(&mut conn).unwrap();
    assert_eq!(
        redis::cmd("INCRBYFLOAT")
            .arg("novar")
            .arg("1")
            .query::<String>(&mut conn)
            .unwrap(),
        "1"
    );
    assert_eq!(get_string(&mut conn, "novar"), Some("1".to_string()));
    assert_eq!(
        redis::cmd("INCRBYFLOAT")
            .arg("novar")
            .arg("0.25")
            .query::<String>(&mut conn)
            .unwrap(),
        "1.25"
    );
    assert_eq!(get_string(&mut conn, "novar"), Some("1.25".to_string()));

    set_string(&mut conn, "novar", "1.5");
    assert_eq!(
        redis::cmd("INCRBYFLOAT")
            .arg("novar")
            .arg("1.5")
            .query::<String>(&mut conn)
            .unwrap(),
        "3"
    );

    set_string(&mut conn, "novar", 17_179_869_184_i64);
    assert_eq!(
        redis::cmd("INCRBYFLOAT")
            .arg("novar")
            .arg("1.5")
            .query::<String>(&mut conn)
            .unwrap(),
        "17179869185.5"
    );

    set_string(&mut conn, "novar", 17_179_869_184_i64);
    assert_eq!(
        redis::cmd("INCRBYFLOAT")
            .arg("novar")
            .arg(17_179_869_184_i64)
            .query::<String>(&mut conn)
            .unwrap(),
        "34359738368"
    );

    set_string(&mut conn, "foo", 1);
    assert_eq!(
        redis::cmd("INCRBYFLOAT")
            .arg("foo")
            .arg("-1.1")
            .query::<String>(&mut conn)
            .unwrap(),
        "-0.1"
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn incrbyfloat_rejects_invalid_float_inputs() {
    let mut conn = must_connect();
    flush(&mut conn);

    for value in ["    11", "11    ", " 11 "] {
        set_string(&mut conn, "novar", value);
        assert_err_contains(
            redis::cmd("INCRBYFLOAT")
                .arg("novar")
                .arg("1.0")
                .query::<String>(&mut conn),
            "ERR value is not a valid float",
        );
    }

    set_string(&mut conn, "foo", 0);
    assert_err_contains(
        redis::cmd("INCRBYFLOAT")
            .arg("foo")
            .arg("+inf")
            .query::<String>(&mut conn),
        "would produce",
    );

    set_string(&mut conn, "foo", 1);
    let _: i64 = redis::cmd("SETRANGE")
        .arg("foo")
        .arg(2)
        .arg("2")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("INCRBYFLOAT")
            .arg("foo")
            .arg("1")
            .query::<String>(&mut conn),
        "ERR value is not a valid float",
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn incrbyfloat_avoids_negative_zero() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("DEL").arg("foo").query(&mut conn).unwrap();
    let one_over_41 = 1.0f64 / 41.0f64;
    let _: String = redis::cmd("INCRBYFLOAT")
        .arg("foo")
        .arg(one_over_41.to_string())
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("INCRBYFLOAT")
        .arg("foo")
        .arg((-one_over_41).to_string())
        .query(&mut conn)
        .unwrap();

    assert_eq!(get_string(&mut conn, "foo"), Some("0".to_string()));
}

#[test]
#[ignore = "requires running Senko instance"]
fn arithmetic_operations_restore_integer_encoding_after_raw_strings() {
    let mut conn = must_connect();
    flush(&mut conn);

    for (command, expected) in [
        ("INCR", "13"),
        ("DECR", "11"),
        ("INCRBY", "13"),
        ("DECRBY", "11"),
    ] {
        set_string(&mut conn, "foo", 1);
        assert_eq!(encoding(&mut conn, "foo"), "int");
        assert_eq!(get_string(&mut conn, "foo"), Some("1".to_string()));

        let _: i64 = redis::cmd("APPEND")
            .arg("foo")
            .arg(2)
            .query(&mut conn)
            .unwrap();
        assert_eq!(encoding(&mut conn, "foo"), "raw");
        assert_eq!(get_string(&mut conn, "foo"), Some("12".to_string()));

        match command {
            "INCR" | "DECR" => {
                let _: i64 = redis::cmd(command).arg("foo").query(&mut conn).unwrap();
            }
            "INCRBY" | "DECRBY" => {
                let _: i64 = redis::cmd(command)
                    .arg("foo")
                    .arg(1)
                    .query(&mut conn)
                    .unwrap();
            }
            _ => unreachable!(),
        }

        assert_eq!(encoding(&mut conn, "foo"), "int");
        assert_eq!(get_string(&mut conn, "foo"), Some(expected.to_string()));
        let _: i64 = redis::cmd("DEL").arg("foo").query(&mut conn).unwrap();
    }
}
