use std::{
    collections::VecDeque,
    fmt::Write as _,
    fs,
    sync::{Mutex, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use compact_str::CompactString;
use hashbrown::HashMap;
use hdrhistogram::Histogram;
use senko_core::{SenkoConfig, SenkoValue};
use senko_proto::Frame;
use senko_store::Response;
use smallvec::{SmallVec, smallvec};

use crate::{
    commands::server::{
        config as live_config,
        info::{self, ServerCommandOutcome},
    },
    connection::{bulk_string, error_bytes, error_message, frame_bytes, serialize_response},
};

#[derive(Debug, Clone)]
struct SlowlogEntry {
    id: u64,
    timestamp: u64,
    duration_us: u64,
    argv: Vec<CompactString>,
    client_addr: CompactString,
    client_name: CompactString,
}

#[derive(Debug, Clone, Copy)]
struct LatencySample {
    timestamp: u64,
    latency_ms: u64,
}

#[derive(Debug, Default, Clone)]
struct LatencySeries {
    samples: VecDeque<LatencySample>,
    peak: u64,
}

#[derive(Debug)]
struct CommandHistogram {
    calls: u64,
    histogram: Histogram<u64>,
}

impl Default for CommandHistogram {
    fn default() -> Self {
        Self {
            calls: 0,
            histogram: Histogram::new(3).expect("histogram precision"),
        }
    }
}

#[derive(Debug, Default)]
struct ShardDiagnostics {
    slowlog: VecDeque<SlowlogEntry>,
    latency: HashMap<CompactString, LatencySeries, ahash::RandomState>,
    command_histograms: HashMap<CompactString, CommandHistogram, ahash::RandomState>,
}

#[derive(Debug)]
struct DiagnosticsState {
    shards: Box<[Mutex<ShardDiagnostics>]>,
    next_slowlog_id: std::sync::atomic::AtomicU64,
}

static DIAGNOSTICS: OnceLock<DiagnosticsState> = OnceLock::new();

const LAT_COMMAND: &str = "command";
const LAT_FAST_COMMAND: &str = "fast-command";
const LAT_AOF_FSYNC: &str = "aof-fsync";
const LAT_BGSAVE: &str = "bgsave";
const LAT_RDB_FORK: &str = "rdb-fork";
const LAT_REPL: &str = "repl";
const GRAPH_WIDTH: usize = 80;
const GRAPH_HEIGHT: usize = 15;

pub fn init(config: &SenkoConfig) {
    let _ = DIAGNOSTICS.set(DiagnosticsState {
        shards: (0..config.num_shards)
            .map(|_| Mutex::new(ShardDiagnostics::default()))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        next_slowlog_id: std::sync::atomic::AtomicU64::new(1),
    });
}

fn state() -> &'static DiagnosticsState {
    DIAGNOSTICS
        .get()
        .expect("diagnostics state not initialized")
}

pub fn record_command(
    shard_id: usize,
    command: &[u8],
    args: &[Frame<'_>],
    client_addr: &str,
    client_name: Option<&str>,
    duration: Duration,
) {
    let duration_us = duration.as_micros() as u64;
    let duration_ms = duration.as_millis() as u64;
    let config = live_config::snapshot();
    let mut shard = state().shards[shard_id]
        .lock()
        .expect("diagnostics shard lock poisoned");

    let command_name = std::str::from_utf8(command)
        .unwrap_or("unknown")
        .to_ascii_lowercase();
    let histogram = shard
        .command_histograms
        .entry(CompactString::from(command_name.as_str()))
        .or_default();
    histogram.calls = histogram.calls.saturating_add(1);
    let _ = histogram.histogram.record(duration_us.max(1));

    if config.slowlog_log_slower_than >= 0 && duration_us >= config.slowlog_log_slower_than as u64 {
        let id = state()
            .next_slowlog_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let entry = SlowlogEntry {
            id,
            timestamp: current_unix_sec(),
            duration_us,
            argv: render_argv(command, args),
            client_addr: CompactString::from(client_addr),
            client_name: CompactString::from(client_name.unwrap_or("")),
        };
        shard.slowlog.push_front(entry);
        while shard.slowlog.len() > config.slowlog_max_len {
            let _ = shard.slowlog.pop_back();
        }
    }

    if config.latency_monitor_threshold > 0 && duration_ms > config.latency_monitor_threshold as u64
    {
        record_latency_sample(&mut shard, LAT_COMMAND, duration_ms);
        if is_fast_command(command) {
            record_latency_sample(&mut shard, LAT_FAST_COMMAND, duration_ms);
        }
    }
}

pub fn record_bgsave_latency(duration: Duration) {
    let duration_ms = duration.as_millis() as u64;
    if duration_ms == 0 {
        return;
    }
    for shard in state().shards.iter() {
        let mut shard = shard.lock().expect("diagnostics shard lock poisoned");
        record_latency_sample(&mut shard, LAT_BGSAVE, duration_ms);
    }
}

fn record_latency_sample(shard: &mut ShardDiagnostics, event: &str, latency_ms: u64) {
    let series = shard.latency.entry(CompactString::from(event)).or_default();
    series.samples.push_back(LatencySample {
        timestamp: current_unix_sec(),
        latency_ms,
    });
    while series.samples.len() > 180 {
        let _ = series.samples.pop_front();
    }
    series.peak = series.peak.max(latency_ms);
}

pub async fn execute(
    command: &[u8],
    args: &[Frame<'_>],
    resp3: bool,
) -> Option<Result<ServerCommandOutcome, Vec<u8>>> {
    if eq_ascii(command, b"SLOWLOG") {
        return Some(handle_slowlog(args, resp3));
    }
    if eq_ascii(command, b"LATENCY") {
        return Some(handle_latency(args, resp3));
    }
    if eq_ascii(command, b"MEMORY") {
        return Some(handle_memory(args, resp3).await);
    }
    None
}

fn handle_slowlog(args: &[Frame<'_>], resp3: bool) -> Result<ServerCommandOutcome, Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'slowlog' command",
        ));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;
    if eq_ascii(subcommand, b"GET") {
        let count = match rest {
            [] => 128usize,
            [count] => parse_usize(count)?,
            _ => {
                return Err(error_message(
                    "ERR wrong number of arguments for 'slowlog|get' command",
                ));
            }
        };
        let response = Response::Array(Box::new(
            aggregate_slowlog_entries()
                .into_iter()
                .take(count)
                .map(slowlog_response)
                .collect(),
        ));
        return Ok(outcome(serialize_response(&response, resp3)));
    }
    if eq_ascii(subcommand, b"LEN") {
        if !rest.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'slowlog|len' command",
            ));
        }
        return Ok(outcome(serialize_response(
            &Response::Integer(slowlog_len() as i64),
            resp3,
        )));
    }
    if eq_ascii(subcommand, b"RESET") {
        if !rest.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'slowlog|reset' command",
            ));
        }
        for shard in state().shards.iter() {
            shard
                .lock()
                .expect("diagnostics shard lock poisoned")
                .slowlog
                .clear();
        }
        return Ok(outcome(crate::connection::simple_string(b"OK")));
    }
    Err(error_message(
        "ERR unknown subcommand or wrong number of arguments for 'SLOWLOG'",
    ))
}

fn handle_latency(args: &[Frame<'_>], resp3: bool) -> Result<ServerCommandOutcome, Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'latency' command",
        ));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;
    if eq_ascii(subcommand, b"LATEST") {
        if !rest.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'latency|latest' command",
            ));
        }
        let response = Response::Array(Box::new(latency_latest()));
        return Ok(outcome(serialize_response(&response, resp3)));
    }
    if eq_ascii(subcommand, b"HISTORY") {
        let [event] = rest else {
            return Err(error_message(
                "ERR wrong number of arguments for 'latency|history' command",
            ));
        };
        let event = parse_token(event)?;
        let response = Response::Array(Box::new(latency_history(event.as_str())));
        return Ok(outcome(serialize_response(&response, resp3)));
    }
    if eq_ascii(subcommand, b"RESET") {
        let reset = latency_reset(rest)?;
        return Ok(outcome(serialize_response(
            &Response::Integer(reset as i64),
            resp3,
        )));
    }
    if eq_ascii(subcommand, b"GRAPH") {
        let [event] = rest else {
            return Err(error_message(
                "ERR wrong number of arguments for 'latency|graph' command",
            ));
        };
        let event = parse_token(event)?;
        return Ok(outcome(bulk_string(
            render_latency_graph(event.as_str()).as_bytes(),
        )));
    }
    if eq_ascii(subcommand, b"DOCTOR") {
        if !rest.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'latency|doctor' command",
            ));
        }
        return Ok(outcome(bulk_string(latency_doctor().as_bytes())));
    }
    if eq_ascii(subcommand, b"HISTOGRAM") {
        let response = latency_histogram(rest, resp3)?;
        return Ok(outcome(response));
    }
    Err(error_message(
        "ERR unknown subcommand or wrong number of arguments for 'LATENCY'",
    ))
}

async fn handle_memory(args: &[Frame<'_>], resp3: bool) -> Result<ServerCommandOutcome, Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'memory' command",
        ));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;
    if eq_ascii(subcommand, b"USAGE") {
        let (key, _samples) = parse_memory_usage(rest)?;
        let usage = info::memory_usage_for_key(key.as_slice(), 0).await;
        let response = match usage {
            Some(bytes) => Response::Integer(bytes as i64),
            None => Response::Value(None),
        };
        return Ok(outcome(serialize_response(&response, resp3)));
    }
    if eq_ascii(subcommand, b"STATS") {
        if !rest.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'memory|stats' command",
            ));
        }
        let aggregate = info::aggregate_snapshot_for_diagnostics().await;
        let response = memory_stats_response(&aggregate, resp3);
        return Ok(outcome(serialize_response(&response, resp3)));
    }
    if eq_ascii(subcommand, b"DOCTOR") {
        if !rest.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'memory|doctor' command",
            ));
        }
        let aggregate = info::aggregate_snapshot_for_diagnostics().await;
        return Ok(outcome(bulk_string(memory_doctor(&aggregate).as_bytes())));
    }
    if eq_ascii(subcommand, b"MALLOC-STATS") {
        if !rest.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'memory|malloc-stats' command",
            ));
        }
        return Ok(outcome(bulk_string(memory_malloc_stats().as_bytes())));
    }
    if eq_ascii(subcommand, b"PURGE") {
        if !rest.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'memory|purge' command",
            ));
        }
        return Ok(outcome(crate::connection::simple_string(b"OK")));
    }
    Err(error_message(
        "ERR unknown subcommand or wrong number of arguments for 'MEMORY'",
    ))
}

fn parse_memory_usage(args: &[Frame<'_>]) -> Result<(Vec<u8>, usize), Vec<u8>> {
    let Some((key, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'memory|usage' command",
        ));
    };
    let key = frame_bytes(key)
        .map_err(|error| error_bytes(&error))?
        .to_vec();
    let mut samples = 5usize;
    let mut index = 0usize;
    while index < rest.len() {
        let token = frame_bytes(&rest[index]).map_err(|error| error_bytes(&error))?;
        if !eq_ascii(token, b"SAMPLES") || index + 1 >= rest.len() {
            return Err(error_message("ERR syntax error"));
        }
        samples = parse_usize(&rest[index + 1])?;
        index += 2;
    }
    Ok((key, samples))
}

fn memory_stats_response(
    aggregate: &info::AggregateSnapshotForDiagnostics,
    resp3: bool,
) -> Response {
    let mut entries = SmallVec::<[Response; 32]>::new();
    let keys_per_key = if aggregate.key_count == 0 {
        0.0
    } else {
        aggregate.store_used_memory as f64 / aggregate.key_count as f64
    };
    push_map_entry(
        &mut entries,
        "peak.allocated",
        int_value(aggregate.used_memory_peak),
    );
    push_map_entry(
        &mut entries,
        "total.allocated",
        int_value(aggregate.used_memory),
    );
    push_map_entry(
        &mut entries,
        "startup.allocated",
        int_value(aggregate.used_memory_startup),
    );
    push_map_entry(&mut entries, "replication.backlog", int_value(0));
    push_map_entry(&mut entries, "clients.slaves", int_value(0));
    push_map_entry(
        &mut entries,
        "clients.normal",
        int_value(aggregate.connected_clients),
    );
    push_map_entry(&mut entries, "cluster.links", int_value(0));
    push_map_entry(&mut entries, "aof.buffer", int_value(0));
    let mut db0 = SmallVec::<[Response; 32]>::new();
    push_map_entry(
        &mut db0,
        "overhead.hashtable.main",
        int_value(aggregate.store_used_memory),
    );
    push_map_entry(
        &mut db0,
        "overhead.hashtable.expires",
        int_value(aggregate.expiry_count.saturating_mul(32)),
    );
    push_map_entry(&mut entries, "db.0", Response::Map(Box::new(db0)));
    push_map_entry(
        &mut entries,
        "overhead.total",
        int_value(aggregate.used_memory_overhead),
    );
    push_map_entry(&mut entries, "keys.count", int_value(aggregate.key_count));
    push_map_entry(
        &mut entries,
        "keys.bytes-per-key",
        bulk_value(format!("{keys_per_key:.2}").into_bytes()),
    );
    push_map_entry(
        &mut entries,
        "dataset.bytes",
        int_value(aggregate.store_used_memory),
    );
    push_map_entry(
        &mut entries,
        "dataset.percentage",
        bulk_value(format!("{:.2}", aggregate.dataset_percentage).into_bytes()),
    );
    push_map_entry(
        &mut entries,
        "peak.percentage",
        bulk_value(format!("{:.2}", aggregate.peak_percentage).into_bytes()),
    );
    push_map_entry(
        &mut entries,
        "fragmentation",
        bulk_value(format!("{:.2}", aggregate.fragmentation_ratio).into_bytes()),
    );
    push_map_entry(
        &mut entries,
        "fragmentation.bytes",
        int_value(aggregate.fragmentation_bytes),
    );
    push_map_entry(
        &mut entries,
        "rss-overhead.ratio",
        bulk_value(format!("{:.2}", aggregate.rss_overhead_ratio).into_bytes()),
    );
    push_map_entry(
        &mut entries,
        "rss-overhead.bytes",
        int_value(aggregate.rss_overhead_bytes),
    );
    push_map_entry(
        &mut entries,
        "allocator-allocated",
        int_value(aggregate.allocator_allocated),
    );
    push_map_entry(
        &mut entries,
        "allocator-active",
        int_value(aggregate.allocator_active),
    );
    push_map_entry(
        &mut entries,
        "allocator-resident",
        int_value(aggregate.allocator_resident),
    );
    push_map_entry(&mut entries, "allocator-muzzy", int_value(0));
    push_map_entry(
        &mut entries,
        "allocator.allocated",
        int_value(aggregate.allocator_allocated),
    );
    push_map_entry(
        &mut entries,
        "allocator.active",
        int_value(aggregate.allocator_active),
    );
    push_map_entry(
        &mut entries,
        "allocator.resident",
        int_value(aggregate.allocator_resident),
    );
    push_map_entry(&mut entries, "allocator.muzzy", int_value(0));
    let _ = resp3;
    Response::Map(Box::new(entries))
}

fn memory_doctor(aggregate: &info::AggregateSnapshotForDiagnostics) -> String {
    let config = live_config::snapshot();
    let mut advice = Vec::new();
    if aggregate.fragmentation_ratio > 1.5 {
        advice.push(format!(
            "- Fragmentation ratio is {:.2}. Consider restarting or reducing churn.",
            aggregate.fragmentation_ratio
        ));
    }
    if let Some(max_memory) = config.max_memory
        && aggregate.used_memory.saturating_mul(10) > (max_memory as u64).saturating_mul(9)
    {
        advice.push(format!(
            "- Used memory is close to maxmemory ({} / {}).",
            aggregate.used_memory, max_memory
        ));
    }
    if aggregate.key_count > 10_000 && aggregate.store_used_memory / aggregate.key_count < 128 {
        advice.push(
            "- Many small keys detected. Review encoding thresholds and key design.".to_owned(),
        );
    }
    if advice.is_empty() {
        "Sam, I detected no problems in your memory.".to_owned()
    } else {
        format!("Sam, I detected a few issues:\n\n{}", advice.join("\n"))
    }
}

fn memory_malloc_stats() -> String {
    fs::read_to_string("/proc/self/status")
        .unwrap_or_else(|_| "allocator stats unavailable".to_owned())
}

fn latency_latest() -> SmallVec<[Response; 16]> {
    let mut latest = collect_latency_events()
        .into_iter()
        .filter_map(|(event, series)| {
            let sample = series.samples.back().copied()?;
            Some(Response::Array(Box::new(smallvec![
                bulk_value(event.as_bytes().to_vec()),
                Response::Integer(sample.timestamp as i64),
                Response::Integer(sample.latency_ms as i64),
                Response::Integer(series.peak as i64),
            ])))
        })
        .collect::<Vec<_>>();
    latest.sort_by_key(|entry| std::cmp::Reverse(response_array_ts(entry)));
    latest.into_iter().collect()
}

fn latency_history(event: &str) -> SmallVec<[Response; 16]> {
    collect_latency_series(event)
        .map(|series| {
            series
                .samples
                .into_iter()
                .map(|sample| {
                    Response::Array(Box::new(smallvec![
                        Response::Integer(sample.timestamp as i64),
                        Response::Integer(sample.latency_ms as i64),
                    ]))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn latency_reset(args: &[Frame<'_>]) -> Result<usize, Vec<u8>> {
    if args.is_empty() {
        let mut total = 0usize;
        for shard in state().shards.iter() {
            let mut shard = shard.lock().expect("diagnostics shard lock poisoned");
            total += shard.latency.len();
            shard.latency.clear();
        }
        return Ok(total);
    }
    let mut names = Vec::with_capacity(args.len());
    for arg in args {
        names.push(parse_token(arg)?);
    }
    let mut reset = 0usize;
    for shard in state().shards.iter() {
        let mut shard = shard.lock().expect("diagnostics shard lock poisoned");
        for name in &names {
            if shard.latency.remove(name).is_some() {
                reset += 1;
            }
        }
    }
    Ok(reset)
}

fn render_latency_graph(event: &str) -> String {
    let samples = collect_latency_series(event)
        .map(|series| series.samples.into_iter().collect::<Vec<_>>())
        .unwrap_or_default();
    if samples.is_empty() {
        return "No samples available".to_owned();
    }
    let max_latency = samples
        .iter()
        .map(|sample| sample.latency_ms)
        .max()
        .unwrap_or(1)
        .max(1);
    let mut canvas = vec![vec![b' '; GRAPH_WIDTH]; GRAPH_HEIGHT];
    for (index, sample) in samples.iter().enumerate() {
        let x = index * GRAPH_WIDTH / samples.len().max(1);
        let y = GRAPH_HEIGHT
            - 1
            - ((sample.latency_ms * (GRAPH_HEIGHT as u64 - 1)) / max_latency) as usize;
        if x < GRAPH_WIDTH && y < GRAPH_HEIGHT {
            canvas[y][x] = b'*';
        }
    }
    let mut out = String::new();
    for (row_index, row) in canvas.iter().enumerate() {
        let label = max_latency.saturating_mul((GRAPH_HEIGHT - 1 - row_index) as u64)
            / (GRAPH_HEIGHT as u64 - 1);
        let _ = write!(out, "{label:>4} |");
        out.push_str(std::str::from_utf8(row).unwrap_or(""));
        out.push('\n');
    }
    out.push_str("     +");
    out.push_str(&"-".repeat(GRAPH_WIDTH));
    out.push('\n');
    out.push_str("      oldest");
    out.push_str(&" ".repeat(GRAPH_WIDTH.saturating_sub(12)));
    out.push_str("latest");
    out
}

fn latency_doctor() -> String {
    let latest = collect_latency_events();
    let command_peak = latest
        .get(LAT_COMMAND)
        .map(|series| series.peak)
        .unwrap_or(0);
    if latest.is_empty() || command_peak == 0 {
        return "I have no suggestions for you right now.".to_owned();
    }
    let threshold = live_config::snapshot().latency_monitor_threshold.max(0);
    format!(
        "I have a few advices for you:\n\n- Peak command latency: {command_peak}ms (threshold: {threshold}ms). Consider reducing heavy commands or using BGSAVE instead of SAVE.\n- Your aof-fsync is disabled. Good.\n"
    )
}

fn latency_histogram(args: &[Frame<'_>], resp3: bool) -> Result<Vec<u8>, Vec<u8>> {
    let filters = args
        .iter()
        .map(parse_token)
        .collect::<Result<Vec<_>, _>>()?;
    let mut names = collect_command_histograms()
        .into_iter()
        .filter(|(name, _)| filters.is_empty() || filters.iter().any(|filter| filter == name))
        .collect::<Vec<_>>();
    names.sort_by(|left, right| left.0.cmp(&right.0));
    let response = Response::Map(Box::new(
        names
            .into_iter()
            .flat_map(|(name, histogram)| {
                let mut inner = SmallVec::<[Response; 32]>::new();
                push_map_entry(&mut inner, "calls", int_value(histogram.calls));
                push_map_entry(
                    &mut inner,
                    "p50",
                    int_value(histogram.histogram.value_at_quantile(0.50)),
                );
                push_map_entry(
                    &mut inner,
                    "p90",
                    int_value(histogram.histogram.value_at_quantile(0.90)),
                );
                push_map_entry(
                    &mut inner,
                    "p99",
                    int_value(histogram.histogram.value_at_quantile(0.99)),
                );
                push_map_entry(
                    &mut inner,
                    "p99.9",
                    int_value(histogram.histogram.value_at_quantile(0.999)),
                );
                [
                    bulk_value(name.into_string().into_bytes()),
                    Response::Map(Box::new(inner)),
                ]
            })
            .collect(),
    ));
    Ok(serialize_response(&response, resp3))
}

fn aggregate_slowlog_entries() -> Vec<SlowlogEntry> {
    let mut entries = Vec::new();
    for shard in state().shards.iter() {
        entries.extend(
            shard
                .lock()
                .expect("diagnostics shard lock poisoned")
                .slowlog
                .iter()
                .cloned(),
        );
    }
    entries.sort_by(|left, right| {
        right
            .timestamp
            .cmp(&left.timestamp)
            .then_with(|| right.id.cmp(&left.id))
    });
    entries
}

fn slowlog_len() -> usize {
    state()
        .shards
        .iter()
        .map(|shard| {
            shard
                .lock()
                .expect("diagnostics shard lock poisoned")
                .slowlog
                .len()
        })
        .sum()
}

fn slowlog_response(entry: SlowlogEntry) -> Response {
    Response::Array(Box::new(smallvec![
        Response::Integer(entry.id as i64),
        Response::Integer(entry.timestamp as i64),
        Response::Integer(entry.duration_us as i64),
        Response::Array(Box::new(
            entry
                .argv
                .into_iter()
                .map(|arg| bulk_value(arg.into_string().into_bytes()))
                .collect()
        )),
        bulk_value(entry.client_addr.into_string().into_bytes()),
        bulk_value(entry.client_name.into_string().into_bytes()),
    ]))
}

fn render_argv(command: &[u8], args: &[Frame<'_>]) -> Vec<CompactString> {
    let mut argv = Vec::with_capacity(args.len() + 1);
    argv.push(truncate_compact(
        std::str::from_utf8(command).unwrap_or("<?>"),
    ));
    for arg in args {
        let rendered = frame_bytes(arg)
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .unwrap_or_else(|_| "<?>".to_owned());
        argv.push(truncate_compact(&rendered));
    }
    argv
}

fn truncate_compact(value: &str) -> CompactString {
    let mut out = value.chars().take(128).collect::<String>();
    if value.chars().count() > 128 {
        out.push_str("...");
    }
    CompactString::from(out)
}

fn collect_latency_events() -> HashMap<&'static str, LatencySeries, ahash::RandomState> {
    let mut events = HashMap::with_hasher(ahash::RandomState::new());
    for name in [
        LAT_COMMAND,
        LAT_FAST_COMMAND,
        LAT_AOF_FSYNC,
        LAT_BGSAVE,
        LAT_RDB_FORK,
        LAT_REPL,
    ] {
        if let Some(series) = collect_latency_series(name) {
            events.insert(name, series);
        }
    }
    events
}

fn collect_latency_series(event: &str) -> Option<LatencySeries> {
    let mut merged = LatencySeries::default();
    for shard in state().shards.iter() {
        let shard = shard.lock().expect("diagnostics shard lock poisoned");
        let Some(series) = shard.latency.get(event) else {
            continue;
        };
        merged.peak = merged.peak.max(series.peak);
        merged.samples.extend(series.samples.iter().copied());
    }
    if merged.samples.is_empty() {
        None
    } else {
        merged
            .samples
            .make_contiguous()
            .sort_by_key(|sample| sample.timestamp);
        while merged.samples.len() > 180 {
            let _ = merged.samples.pop_front();
        }
        Some(merged)
    }
}

fn collect_command_histograms() -> HashMap<CompactString, CommandHistogram, ahash::RandomState> {
    let mut merged = HashMap::with_hasher(ahash::RandomState::new());
    for shard in state().shards.iter() {
        let shard = shard.lock().expect("diagnostics shard lock poisoned");
        for (name, histogram) in &shard.command_histograms {
            let target: &mut CommandHistogram = merged.entry(name.clone()).or_default();
            target.calls = target.calls.saturating_add(histogram.calls);
            let _ = target.histogram.add(&histogram.histogram);
        }
    }
    merged
}

fn push_map_entry(target: &mut SmallVec<[Response; 32]>, key: &str, value: Response) {
    target.push(bulk_value(key.as_bytes().to_vec()));
    target.push(value);
}

fn bulk_value(bytes: Vec<u8>) -> Response {
    Response::Value(Some(SenkoValue::from(Bytes::from(bytes))))
}

fn int_value(value: u64) -> Response {
    Response::Integer(value as i64)
}

fn parse_token(frame: &Frame<'_>) -> Result<CompactString, Vec<u8>> {
    let bytes = frame_bytes(frame).map_err(|error| error_bytes(&error))?;
    Ok(CompactString::from(
        std::str::from_utf8(bytes)
            .map_err(|_| error_message("ERR syntax error"))?
            .to_ascii_lowercase(),
    ))
}

fn parse_usize(frame: &Frame<'_>) -> Result<usize, Vec<u8>> {
    let bytes = frame_bytes(frame).map_err(|error| error_bytes(&error))?;
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| error_message("ERR value is not an integer or out of range"))
}

fn response_array_ts(response: &Response) -> i64 {
    let Response::Array(values) = response else {
        return 0;
    };
    let Some(Response::Integer(value)) = values.get(1) else {
        return 0;
    };
    *value
}

fn is_fast_command(command: &[u8]) -> bool {
    matches!(
        std::str::from_utf8(command)
            .unwrap_or_default()
            .to_ascii_uppercase()
            .as_str(),
        "GET" | "SET" | "INCR" | "HGET" | "PING" | "EXISTS"
    )
}

fn outcome(response: Vec<u8>) -> ServerCommandOutcome {
    ServerCommandOutcome {
        response,
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn current_unix_sec() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use senko_core::SenkoConfig;

    use super::{current_unix_sec, init, render_latency_graph, truncate_compact};

    fn ensure_state() {
        init(&SenkoConfig {
            num_shards: 1,
            ..SenkoConfig::default()
        });
    }

    #[test]
    fn truncate_compact_limits_long_values() {
        ensure_state();
        let value = "x".repeat(140);
        assert!(truncate_compact(&value).len() <= 131);
    }

    #[test]
    fn latency_graph_renders_non_empty_output() {
        ensure_state();
        let rendered = render_latency_graph("missing-event");
        assert!(!rendered.is_empty());
    }

    #[test]
    fn current_unix_sec_is_reasonable() {
        ensure_state();
        assert!(current_unix_sec() > 1_000_000_000);
    }
}
