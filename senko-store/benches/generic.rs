use bytes::Bytes;
use compact_str::CompactString;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use senko_core::{HashObject, QuickList, SenkoValue, SetObject, ZAddOptions, ZSetObject};
use senko_proto::Frame;
use senko_store::{
    Response, Store,
    commands::{
        bitmap,
        generic::{expiry, keys, migrate, object, scan, sort},
    },
    store::{SetOptions, current_unix_ms},
};

const DEL_SINGLE_STRING_OPS: usize = 10_000_000;
const DEL_SINGLE_HASH_1K_OPS: usize = 1_000_000;
const EXISTS_OPS: usize = 10_000_000;
const TYPE_LOOKUP_OPS: usize = 10_000_000;
const RENAME_OPS: usize = 5_000_000;
const EXPIRE_SET_OPS: usize = 10_000_000;
const TTL_LOOKUP_OPS: usize = 10_000_000;
const SCAN_FULL_1K_OPS: usize = 100_000;
const KEYS_ALL_1K_OPS: usize = 100_000;
const OBJECT_ENCODING_OPS: usize = 10_000_000;
const DUMP_STRING_OPS: usize = 1_000_000;
const RESTORE_STRING_OPS: usize = 1_000_000;
const SORT_LIST_1K_OPS: usize = 100_000;
const SORT_LIST_ALPHA_OPS: usize = 100_000;
const TOUCH_10KEYS_OPS: usize = 5_000_000;
const BITCOUNT_1M_OPS: usize = 10_000;

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

fn set_raw(store: &mut Store, key: &str, value: &[u8]) {
    let _ = store.set(
        CompactString::from(key),
        SenkoValue::Raw(Bytes::copy_from_slice(value)),
        SetOptions::default(),
    );
}

fn seed_string_keys(store: &mut Store, count: usize) {
    for i in 0..count {
        let _ = store.set(
            CompactString::from(format!("k{i}")),
            SenkoValue::Int(i as i64),
            SetOptions::default(),
        );
    }
}

fn seed_scan_store() -> Store {
    let mut store = Store::default();
    seed_string_keys(&mut store, 1_000);
    store
}

fn full_scan(store: &mut Store) {
    let mut cursor = 0u64;
    loop {
        let cursor_buf = cursor.to_string().into_bytes();
        let response = scan::scan(store, &[bs(&cursor_buf), bs(b"COUNT"), bs(b"10")]).unwrap();
        let Response::Array(top) = response else {
            panic!("expected scan array");
        };
        let Response::Value(Some(value)) = &top[0] else {
            panic!("expected cursor value");
        };
        cursor = std::str::from_utf8(value.as_bytes().as_ref())
            .unwrap()
            .parse()
            .unwrap();
        if cursor == 0 {
            break;
        }
    }
}

fn build_hash_1k() -> SenkoValue {
    let mut hash = HashObject::default();
    for i in 0..1_000 {
        let _ = hash.set(
            CompactString::from(format!("f{i}")),
            SenkoValue::Int(i as i64),
            None,
        );
    }
    SenkoValue::Hash(Box::new(hash))
}

fn build_numeric_list_1k() -> QuickList {
    let mut list = QuickList::default();
    for i in (0..1_000).rev() {
        list.push_back(i.to_string().as_bytes());
    }
    list
}

fn build_alpha_list_1k() -> QuickList {
    let mut list = QuickList::default();
    for i in (0..1_000).rev() {
        list.push_back(format!("v{i:04}").as_bytes());
    }
    list
}

fn build_touch_store() -> Store {
    let mut store = Store::default();
    for i in 0..10 {
        set_raw(&mut store, &format!("touch:{i}"), b"v");
    }
    store
}

fn build_bitmap_store() -> Store {
    let mut store = Store::default();
    let payload = vec![0xAA; 1024 * 1024];
    set_raw(&mut store, "bitmap:1m", &payload);
    store
}

fn bench_generic(c: &mut Criterion) {
    let mut group = c.benchmark_group("generic_ops");

    group.throughput(Throughput::Elements(DEL_SINGLE_STRING_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_del_single_string", DEL_SINGLE_STRING_OPS),
        |b| {
            b.iter(|| {
                let mut store = Store::default();
                for i in 0..DEL_SINGLE_STRING_OPS {
                    let key = format!("del:s:{i}");
                    let _ = store.set(
                        CompactString::from(key.as_str()),
                        SenkoValue::Int(i as i64),
                        SetOptions::default(),
                    );
                    let _ = keys::del(&mut store, &[Frame::BulkString(key.as_bytes())]).unwrap();
                }
            });
        },
    );

    group.throughput(Throughput::Elements(DEL_SINGLE_HASH_1K_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_del_single_hash_1k", DEL_SINGLE_HASH_1K_OPS),
        |b| {
            b.iter(|| {
                let mut store = Store::default();
                for i in 0..DEL_SINGLE_HASH_1K_OPS {
                    let key = format!("del:h:{i}");
                    let _ = store.set(
                        CompactString::from(key.as_str()),
                        build_hash_1k(),
                        SetOptions::default(),
                    );
                    let _ = keys::del(&mut store, &[Frame::BulkString(key.as_bytes())]).unwrap();
                }
            });
        },
    );

    let mut exists_hit_store = Store::default();
    set_raw(&mut exists_hit_store, "exists:hit", b"v");
    group.throughput(Throughput::Elements(EXISTS_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_exists_hit", EXISTS_OPS), |b| {
        b.iter(|| {
            for _ in 0..EXISTS_OPS {
                let _ = keys::exists(&mut exists_hit_store, &[bs(b"exists:hit")]).unwrap();
            }
        });
    });

    let mut exists_miss_store = Store::default();
    group.throughput(Throughput::Elements(EXISTS_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_exists_miss", EXISTS_OPS), |b| {
        b.iter(|| {
            for _ in 0..EXISTS_OPS {
                let _ = keys::exists(&mut exists_miss_store, &[bs(b"exists:miss")]).unwrap();
            }
        });
    });

    let mut type_store = Store::default();
    set_raw(&mut type_store, "type:string", b"v");
    let mut set = SetObject::default();
    let _ = set.add(b"a");
    let _ = type_store.set(
        CompactString::from("type:set"),
        SenkoValue::Set(Box::new(set)),
        SetOptions::default(),
    );
    let mut zset = ZSetObject::default();
    let _ = zset.add(1.0, CompactString::from("m"), ZAddOptions::default());
    let _ = type_store.set(
        CompactString::from("type:zset"),
        SenkoValue::ZSet(Box::new(zset)),
        SetOptions::default(),
    );
    group.throughput(Throughput::Elements(TYPE_LOOKUP_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_type_lookup", TYPE_LOOKUP_OPS),
        |b| {
            b.iter(|| {
                for i in 0..TYPE_LOOKUP_OPS {
                    let key = match i % 3 {
                        0 => b"type:string".as_slice(),
                        1 => b"type:set".as_slice(),
                        _ => b"type:zset".as_slice(),
                    };
                    let _ = keys::type_cmd(&mut type_store, &[Frame::BulkString(key)]).unwrap();
                }
            });
        },
    );

    let mut rename_store = Store::default();
    set_raw(&mut rename_store, "rename:a", b"v");
    group.throughput(Throughput::Elements(RENAME_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_rename", RENAME_OPS), |b| {
        b.iter(|| {
            for _ in 0..RENAME_OPS {
                let _ =
                    keys::rename(&mut rename_store, &[bs(b"rename:a"), bs(b"rename:b")]).unwrap();
                let _ =
                    keys::rename(&mut rename_store, &[bs(b"rename:b"), bs(b"rename:a")]).unwrap();
            }
        });
    });

    let mut expire_store = Store::default();
    set_raw(&mut expire_store, "expire:key", b"v");
    group.throughput(Throughput::Elements(EXPIRE_SET_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_expire_set", EXPIRE_SET_OPS), |b| {
        b.iter(|| {
            for _ in 0..EXPIRE_SET_OPS {
                let _ = expiry::expire(&mut expire_store, &[bs(b"expire:key"), bs(b"60")]).unwrap();
            }
        });
    });

    let mut ttl_store = Store::default();
    set_raw(&mut ttl_store, "ttl:key", b"v");
    let expires = current_unix_ms() + 60_000;
    ttl_store.set_expiry(b"ttl:key", expires);
    group.throughput(Throughput::Elements(TTL_LOOKUP_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_ttl_lookup", TTL_LOOKUP_OPS), |b| {
        b.iter(|| {
            for _ in 0..TTL_LOOKUP_OPS {
                let _ = expiry::ttl(&mut ttl_store, &[bs(b"ttl:key")]).unwrap();
            }
        });
    });

    group.throughput(Throughput::Elements(SCAN_FULL_1K_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_scan_full_1k_keys", SCAN_FULL_1K_OPS),
        |b| {
            b.iter(|| {
                let mut store = seed_scan_store();
                for _ in 0..SCAN_FULL_1K_OPS {
                    full_scan(&mut store);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(KEYS_ALL_1K_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_keys_all_1k", KEYS_ALL_1K_OPS),
        |b| {
            b.iter(|| {
                let mut store = seed_scan_store();
                for _ in 0..KEYS_ALL_1K_OPS {
                    let _ = scan::keys(&mut store, &[bs(b"*")]).unwrap();
                }
            });
        },
    );

    group.throughput(Throughput::Elements(KEYS_ALL_1K_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_scan_vs_keys", KEYS_ALL_1K_OPS),
        |b| {
            b.iter(|| {
                let mut store = seed_scan_store();
                for _ in 0..KEYS_ALL_1K_OPS {
                    full_scan(&mut store);
                    let _ = scan::keys(&mut store, &[bs(b"*")]).unwrap();
                }
            });
        },
    );

    let mut object_store = Store::default();
    let mut object_set = SetObject::default();
    let _ = object_set.add(b"a");
    let _ = object_store.set(
        CompactString::from("obj:key"),
        SenkoValue::Set(Box::new(object_set)),
        SetOptions::default(),
    );
    group.throughput(Throughput::Elements(OBJECT_ENCODING_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_object_encoding", OBJECT_ENCODING_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..OBJECT_ENCODING_OPS {
                    let _ = object::object_encoding(&mut object_store, &[bs(b"obj:key")]).unwrap();
                }
            });
        },
    );

    let mut dump_store = Store::default();
    set_raw(&mut dump_store, "dump:key", b"value");
    group.throughput(Throughput::Elements(DUMP_STRING_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_dump_string", DUMP_STRING_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..DUMP_STRING_OPS {
                    let _ = migrate::dump(&mut dump_store, &[bs(b"dump:key")]).unwrap();
                }
            });
        },
    );

    let mut restore_store = Store::default();
    set_raw(&mut restore_store, "restore:seed", b"value");
    let Response::Value(Some(SenkoValue::Raw(payload))) =
        migrate::dump(&mut restore_store, &[bs(b"restore:seed")]).unwrap()
    else {
        panic!("expected dump payload");
    };
    group.throughput(Throughput::Elements(RESTORE_STRING_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_restore_string", RESTORE_STRING_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..RESTORE_STRING_OPS {
                    let _ = migrate::restore(
                        &mut restore_store,
                        &[
                            bs(b"restore:key"),
                            bs(b"0"),
                            Frame::BulkString(payload.as_ref()),
                            bs(b"REPLACE"),
                        ],
                    )
                    .unwrap();
                }
            });
        },
    );

    let mut sort_store = Store::default();
    let _ = sort_store.set(
        CompactString::from("sort:num"),
        SenkoValue::List(Box::new(build_numeric_list_1k())),
        SetOptions::default(),
    );
    group.throughput(Throughput::Elements(SORT_LIST_1K_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sort_list_1k", SORT_LIST_1K_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..SORT_LIST_1K_OPS {
                    let _ = sort::sort(&mut sort_store, &[bs(b"sort:num")]).unwrap();
                }
            });
        },
    );

    let mut sort_alpha_store = Store::default();
    let _ = sort_alpha_store.set(
        CompactString::from("sort:alpha"),
        SenkoValue::List(Box::new(build_alpha_list_1k())),
        SetOptions::default(),
    );
    group.throughput(Throughput::Elements(SORT_LIST_ALPHA_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sort_list_alpha", SORT_LIST_ALPHA_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..SORT_LIST_ALPHA_OPS {
                    let _ = sort::sort(&mut sort_alpha_store, &[bs(b"sort:alpha"), bs(b"ALPHA")])
                        .unwrap();
                }
            });
        },
    );

    let mut touch_store = build_touch_store();
    group.throughput(Throughput::Elements(TOUCH_10KEYS_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_touch_10keys", TOUCH_10KEYS_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..TOUCH_10KEYS_OPS {
                    let _ = scan::touch(
                        &mut touch_store,
                        &[
                            bs(b"touch:0"),
                            bs(b"touch:1"),
                            bs(b"touch:2"),
                            bs(b"touch:3"),
                            bs(b"touch:4"),
                            bs(b"touch:5"),
                            bs(b"touch:6"),
                            bs(b"touch:7"),
                            bs(b"touch:8"),
                            bs(b"touch:9"),
                        ],
                    )
                    .unwrap();
                }
            });
        },
    );

    let mut bitmap_store = build_bitmap_store();
    group.throughput(Throughput::Bytes((1024 * 1024 * BITCOUNT_1M_OPS) as u64));
    group.bench_function(
        BenchmarkId::new("bench_bitcount_1m", BITCOUNT_1M_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..BITCOUNT_1M_OPS {
                    let _ = bitmap::bitcount(&mut bitmap_store, &[bs(b"bitmap:1m")]).unwrap();
                }
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_generic);
criterion_main!(benches);
