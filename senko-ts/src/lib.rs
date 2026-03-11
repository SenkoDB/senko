#![deny(unsafe_code)]

pub mod agg;
pub mod error;
pub mod gorilla;
pub mod series;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use senko_core::{
    CommandRegistry, ModuleCommandContext, ModuleError, ModuleResponse, ModuleResult, SenkoModule,
    ShardState,
};
use smallvec::{SmallVec, smallvec};

pub use agg::{Aggregation, Aggregator};
pub use error::TsError;
pub use gorilla::{BitReader, BitWriter, CompressedChunk};
pub use series::{
    BucketTimestamp, Chunk, ChunkData, CompactionRule, DupPolicy, Encoding, IgnoreConfig,
    SharedTimeSeries, TimeSeries, TsEngine, TsModule,
};

impl SenkoModule for TsModule {
    fn name(&self) -> &'static str {
        "timeseries"
    }

    fn version(&self) -> u64 {
        10_800
    }

    fn register_commands(&self, registry: &mut CommandRegistry) {
        registry.register("TS.CREATE", ts_create);
        registry.register("TS.ALTER", ts_alter);
        registry.register("TS.ADD", ts_add);
        registry.register("TS.MADD", ts_madd);
        registry.register("TS.INCRBY", ts_incrby);
        registry.register("TS.DECRBY", ts_decrby);
        registry.register("TS.GET", ts_get);
        registry.register("TS.INFO", ts_info);
        registry.register("TS.DEL", ts_del);
        registry.register("TS.CREATERULE", ts_createrule);
        registry.register("TS.DELETERULE", ts_deleterule);
        registry.register("TS.RANGE", ts_range);
        registry.register("TS.REVRANGE", ts_revrange);
        registry.register("TS.MRANGE", ts_mrange);
        registry.register("TS.MREVRANGE", ts_mrevrange);
        registry.register("TS.MGET", ts_mget);
        registry.register("TS.QUERYINDEX", ts_queryindex);
    }

    fn init_shard(&self, shard: &mut ShardState) {
        shard.set_extension(Arc::clone(self.engine()));
    }
}

fn ts_create(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() {
        return Err(err("ERR wrong number of arguments for 'ts.create' command"));
    }
    let engine = engine(ctx)?;
    ensure_key_slot_free(ctx, args[0])?;
    if engine.get_series(args[0]).is_some() {
        return Err(TsError::KeyExists.into());
    }
    let options = parse_create_options(&args[1..], false)?;
    let mut series = TimeSeries::default();
    apply_create_options(&mut series, options);
    let key = Bytes::copy_from_slice(args[0]);
    let created = engine.create_series(key.clone(), series)?;
    engine.index_labels(&key, &created.read().labels);
    Ok(ModuleResponse::Simple(b"OK"))
}

fn ts_alter(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() {
        return Err(err("ERR wrong number of arguments for 'ts.alter' command"));
    }
    let engine = engine(ctx)?;
    let series = get_series(engine.as_ref(), ctx, args[0])?;
    let options = parse_create_options(&args[1..], true)?;
    let key = Bytes::copy_from_slice(args[0]);
    let mut guard = series.write();
    let old_labels = guard.labels.clone();
    apply_alter_options(&mut guard, options);
    drop(guard);
    engine.remove_labels(&key, &old_labels);
    let labels = series.read().labels.clone();
    engine.index_labels(&key, &labels);
    Ok(ModuleResponse::Simple(b"OK"))
}

fn ts_add(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 {
        return Err(err("ERR wrong number of arguments for 'ts.add' command"));
    }
    let engine = engine(ctx)?;
    let key = args[0];
    let now_ms = current_unix_ms();
    let timestamp = parse_timestamp(args[1], now_ms)?;
    let value = parse_f64(args[2])?;
    let add_options = parse_add_options(&args[3..])?;
    let series = if let Some(series) = engine.get_series(key) {
        series
    } else {
        ensure_key_slot_free(ctx, key)?;
        let mut created = TimeSeries::default();
        apply_create_options(&mut created, add_options.create);
        let shared = engine.create_series(Bytes::copy_from_slice(key), created)?;
        let labels = shared.read().labels.clone();
        engine.index_labels(&Bytes::copy_from_slice(key), &labels);
        shared
    };
    add_or_update_sample(
        engine.as_ref(),
        key,
        &series,
        timestamp,
        value,
        add_options.on_duplicate,
        now_ms,
    )?;
    Ok(ModuleResponse::Integer(timestamp))
}

fn ts_madd(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 || !args.len().is_multiple_of(3) {
        return Err(err("ERR wrong number of arguments for 'ts.madd' command"));
    }
    let mut out = SmallVec::new();
    for chunk in args.chunks(3) {
        out.push(ts_add(ctx, chunk)?);
    }
    Ok(ModuleResponse::Array(Box::new(out)))
}

fn ts_incrby(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    incr_decr(ctx, args, true)
}

fn ts_decrby(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    incr_decr(ctx, args, false)
}

fn ts_del(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 3 {
        return Err(err("ERR wrong number of arguments for 'ts.del' command"));
    }
    let engine = engine(ctx)?;
    let series = get_series(engine.as_ref(), ctx, args[0])?;
    let from = parse_ts_endpoint(args[1])?;
    let to = parse_ts_endpoint(args[2])?;
    let deleted = series.write().delete_range(from, to);
    Ok(ModuleResponse::Integer(deleted as i64))
}

fn ts_get(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() || args.len() > 2 {
        return Err(err("ERR wrong number of arguments for 'ts.get' command"));
    }
    let engine = engine(ctx)?;
    let Some(series) = maybe_get_series(engine.as_ref(), ctx, args[0])? else {
        return Ok(ModuleResponse::Bulk(None));
    };
    Ok(series
        .read()
        .latest_sample()
        .map(sample_response)
        .unwrap_or(ModuleResponse::Bulk(None)))
}

fn ts_info(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() || args.len() > 2 {
        return Err(err("ERR wrong number of arguments for 'ts.info' command"));
    }
    let debug = args.get(1).is_some_and(|value| eq_ascii(value, b"DEBUG"));
    let engine = engine(ctx)?;
    let series = get_series(engine.as_ref(), ctx, args[0])?;
    let guard = series.read();
    let memory_usage = guard
        .chunks
        .iter()
        .map(|chunk| chunk.approx_size())
        .sum::<usize>() as i64;
    let first_ts = guard.all_samples().first().map(|(ts, _)| *ts).unwrap_or(-1);
    let last_ts = guard.latest_sample().map(|(ts, _)| ts).unwrap_or(-1);
    let mut map = smallvec![
        bulk(b"totalSamples"),
        ModuleResponse::Integer(guard.total_samples as i64),
        bulk(b"memoryUsage"),
        ModuleResponse::Integer(memory_usage),
        bulk(b"firstTimestamp"),
        ModuleResponse::Integer(first_ts),
        bulk(b"lastTimestamp"),
        ModuleResponse::Integer(last_ts),
        bulk(b"retentionTime"),
        ModuleResponse::Integer(guard.retention_ms as i64),
        bulk(b"chunkCount"),
        ModuleResponse::Integer(guard.chunks.len() as i64),
        bulk(b"chunkSize"),
        ModuleResponse::Integer(guard.chunk_size as i64),
        bulk(b"duplicatePolicy"),
        bulk(dup_policy_name(guard.dup_policy)),
        bulk(b"labels"),
        labels_response(&guard.labels, LabelMode::WithLabels, &[]),
        bulk(b"sourceKey"),
        ModuleResponse::Bulk(None),
        bulk(b"rules"),
        rules_response(&guard.rules),
    ];
    if debug {
        map.push(bulk(b"chunks"));
        map.push(ModuleResponse::Array(Box::new(
            guard
                .chunks
                .iter()
                .map(|chunk| {
                    ModuleResponse::Map(Box::new(smallvec![
                        bulk(b"startTimestamp"),
                        ModuleResponse::Integer(chunk.base_ts),
                        bulk(b"endTimestamp"),
                        ModuleResponse::Integer(chunk.max_ts),
                        bulk(b"samples"),
                        ModuleResponse::Integer(chunk.num_samples as i64),
                        bulk(b"size"),
                        ModuleResponse::Integer(chunk.approx_size() as i64),
                    ]))
                })
                .collect(),
        )));
    }
    Ok(ModuleResponse::Map(Box::new(map)))
}

fn ts_createrule(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 5 || !eq_ascii(args[2], b"AGGREGATION") {
        return Err(err(
            "ERR wrong number of arguments for 'ts.createrule' command",
        ));
    }
    let engine = engine(ctx)?;
    let source = get_series(engine.as_ref(), ctx, args[0])?;
    let _dest = get_series(engine.as_ref(), ctx, args[1])?;
    let aggregation = parse_aggregation(args[3])?;
    let bucket_duration = parse_u64(args[4])?;
    let align_ts = args
        .get(5)
        .map(|value| parse_u64(value))
        .transpose()?
        .unwrap_or(0);
    let mut guard = source.write();
    if guard
        .rules
        .iter()
        .any(|rule| rule.dest_key.as_ref() == args[1])
    {
        return Err(TsError::RuleExists.into());
    }
    guard.rules.push(CompactionRule {
        dest_key: Bytes::copy_from_slice(args[1]),
        aggregation,
        bucket_duration,
        align_ts,
        state: None,
    });
    Ok(ModuleResponse::Simple(b"OK"))
}

fn ts_deleterule(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 2 {
        return Err(err(
            "ERR wrong number of arguments for 'ts.deleterule' command",
        ));
    }
    let engine = engine(ctx)?;
    let source = get_series(engine.as_ref(), ctx, args[0])?;
    let mut guard = source.write();
    let original = guard.rules.len();
    guard.rules.retain(|rule| rule.dest_key.as_ref() != args[1]);
    if guard.rules.len() == original {
        return Err(TsError::RuleNotFound.into());
    }
    Ok(ModuleResponse::Simple(b"OK"))
}

fn ts_range(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    range_command(ctx, args, false)
}

fn ts_revrange(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    range_command(ctx, args, true)
}

fn ts_mrange(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    multi_range_command(ctx, args, false)
}

fn ts_mrevrange(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    multi_range_command(ctx, args, true)
}

fn ts_mget(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    let (label_mode, filters, _) = parse_multi_query_tail(args)?;
    let engine = engine(ctx)?;
    let matches = filter_series(engine.as_ref(), &filters);
    Ok(ModuleResponse::Array(Box::new(
        matches
            .into_iter()
            .map(|(key, series)| {
                let guard = series.read();
                ModuleResponse::Array(Box::new(smallvec![
                    bulk(key.as_ref()),
                    labels_response(&guard.labels, label_mode, &[]),
                    guard
                        .latest_sample()
                        .map(sample_response)
                        .unwrap_or(ModuleResponse::Bulk(None)),
                ]))
            })
            .collect(),
    )))
}

fn ts_queryindex(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() {
        return Err(err(
            "ERR wrong number of arguments for 'ts.queryindex' command",
        ));
    }
    let filters = parse_filters(args)?;
    let engine = engine(ctx)?;
    Ok(ModuleResponse::Array(Box::new(
        filter_series(engine.as_ref(), &filters)
            .into_iter()
            .map(|(key, _)| bulk(key.as_ref()))
            .collect(),
    )))
}

#[derive(Clone, Copy)]
enum LabelMode {
    None,
    WithLabels,
    Selected,
}

#[derive(Default, Clone)]
struct CreateOptions {
    retention_ms: Option<u64>,
    encoding: Option<Encoding>,
    chunk_size: Option<usize>,
    dup_policy: Option<DupPolicy>,
    ignore: Option<IgnoreConfig>,
    labels: Option<Vec<(String, String)>>,
}

#[derive(Clone)]
struct AddOptions {
    create: CreateOptions,
    on_duplicate: Option<DupPolicy>,
}

#[derive(Default)]
struct RangeQuery {
    latest: bool,
    filter_ts: Option<ahash::AHashSet<i64>>,
    filter_value: Option<(f64, f64)>,
    count: Option<usize>,
    align: Option<i64>,
    aggregation: Option<(Aggregation, u64, BucketTimestamp, bool)>,
}

#[derive(Clone)]
enum Filter {
    Eq(String, String),
    Ne(String, String),
    Exists(String),
    NotExists(String),
    In(String, BTreeSet<String>),
    NotIn(String, BTreeSet<String>),
}

fn incr_decr(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]], incr: bool) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(if incr {
            "ERR wrong number of arguments for 'ts.incrby' command"
        } else {
            "ERR wrong number of arguments for 'ts.decrby' command"
        }));
    }
    let engine = engine(ctx)?;
    let delta = parse_f64(args[1])?;
    let delta = if incr { delta } else { -delta };
    let now_ms = current_unix_ms();
    let mut timestamp = now_ms;
    let mut create = CreateOptions::default();
    let mut index = 2usize;
    while index < args.len() {
        if eq_ascii(args[index], b"TIMESTAMP") {
            index += 1;
            timestamp = parse_timestamp(
                args.get(index)
                    .ok_or_else(|| err("ERR TSDB: invalid timestamp"))?,
                now_ms,
            )?;
        } else if eq_ascii(args[index], b"RETENTION")
            || eq_ascii(args[index], b"ENCODING")
            || eq_ascii(args[index], b"CHUNK")
            || eq_ascii(args[index], b"DUPLICATE")
            || eq_ascii(args[index], b"IGNORE")
            || eq_ascii(args[index], b"LABELS")
        {
            create = parse_create_options(&args[index..], false)?;
            break;
        }
        index += 1;
    }
    let key = args[0];
    let series = if let Some(series) = engine.get_series(key) {
        series
    } else {
        ensure_key_slot_free(ctx, key)?;
        let mut created = TimeSeries::default();
        apply_create_options(&mut created, create.clone());
        let shared = engine.create_series(Bytes::copy_from_slice(key), created)?;
        let labels = shared.read().labels.clone();
        engine.index_labels(&Bytes::copy_from_slice(key), &labels);
        shared
    };
    let base = series
        .read()
        .latest_sample()
        .map(|(_, value)| value)
        .unwrap_or(0.0);
    let value = base + delta;
    add_or_update_sample(
        engine.as_ref(),
        key,
        &series,
        timestamp,
        value,
        Some(DupPolicy::Last),
        now_ms,
    )?;
    Ok(ModuleResponse::Integer(timestamp))
}

fn range_command(
    ctx: &mut dyn ModuleCommandContext,
    args: &[&[u8]],
    reverse: bool,
) -> ModuleResult {
    if args.len() < 3 {
        return Err(err(if reverse {
            "ERR wrong number of arguments for 'ts.revrange' command"
        } else {
            "ERR wrong number of arguments for 'ts.range' command"
        }));
    }
    let engine = engine(ctx)?;
    let series = get_series(engine.as_ref(), ctx, args[0])?;
    let from = parse_ts_endpoint(args[1])?;
    let to = parse_ts_endpoint(args[2])?;
    let query = parse_range_query(&args[3..])?;
    let samples = render_series_samples(&series.read(), from, to, reverse, &query);
    Ok(samples_response(&samples))
}

fn multi_range_command(
    ctx: &mut dyn ModuleCommandContext,
    args: &[&[u8]],
    reverse: bool,
) -> ModuleResult {
    if args.len() < 4 {
        return Err(err(if reverse {
            "ERR wrong number of arguments for 'ts.mrevrange' command"
        } else {
            "ERR wrong number of arguments for 'ts.mrange' command"
        }));
    }
    let from = parse_ts_endpoint(args[0])?;
    let to = parse_ts_endpoint(args[1])?;
    let (query, label_mode, selected, filters, group_by) = parse_multi_range_query(&args[2..])?;
    let engine = engine(ctx)?;
    let mut entries = filter_series(engine.as_ref(), &filters)
        .into_iter()
        .map(|(key, series)| {
            let guard = series.read();
            let samples = render_series_samples(&guard, from, to, reverse, &query);
            MultiRangeEntry {
                key,
                labels: guard.labels.clone(),
                samples,
            }
        })
        .collect::<Vec<_>>();
    if let Some((label, reducer)) = group_by {
        entries = reduce_grouped(entries, &label, reducer);
    }
    Ok(ModuleResponse::Array(Box::new(
        entries
            .into_iter()
            .map(|entry| {
                ModuleResponse::Array(Box::new(smallvec![
                    bulk(entry.key.as_ref()),
                    labels_response(&entry.labels, label_mode, &selected),
                    samples_response(&entry.samples),
                ]))
            })
            .collect(),
    )))
}

struct MultiRangeEntry {
    key: Bytes,
    labels: Vec<(String, String)>,
    samples: Vec<(i64, f64)>,
}

fn reduce_grouped(
    entries: Vec<MultiRangeEntry>,
    label: &str,
    reducer: Aggregation,
) -> Vec<MultiRangeEntry> {
    let mut grouped: BTreeMap<String, BTreeMap<i64, Aggregator>> = BTreeMap::new();
    for entry in &entries {
        let label_value = entry
            .labels
            .iter()
            .find_map(|(k, v)| (k == label).then_some(v.clone()))
            .unwrap_or_default();
        let series = grouped.entry(label_value).or_default();
        for (ts, value) in &entry.samples {
            series.entry(*ts).or_default().push(*ts, *value);
        }
    }
    grouped
        .into_iter()
        .map(|(label_value, samples)| MultiRangeEntry {
            key: Bytes::from(label_value.clone()),
            labels: vec![(label.to_string(), label_value)],
            samples: samples
                .into_iter()
                .filter_map(|(ts, agg)| agg.value(reducer, ts, ts + 1).map(|value| (ts, value)))
                .collect(),
        })
        .collect()
}

fn render_series_samples(
    series: &TimeSeries,
    from: i64,
    to: i64,
    reverse: bool,
    query: &RangeQuery,
) -> Vec<(i64, f64)> {
    let min = from.min(to);
    let max = from.max(to);
    let mut samples = series.query_range(
        min,
        max,
        false,
        query.filter_ts.as_ref(),
        query.filter_value,
        None,
    );
    if let Some((aggregation, bucket, bucket_ts, empty)) = query.aggregation {
        let align = query.align.unwrap_or(min);
        samples = series.aggregate(
            &samples,
            aggregation,
            bucket,
            align,
            bucket_ts,
            empty,
            min,
            max,
        );
    }
    if reverse {
        samples.reverse();
    }
    if let Some(limit) = query.count {
        samples.truncate(limit);
    }
    samples
}

fn add_or_update_sample(
    engine: &TsEngine,
    key: &[u8],
    series: &SharedTimeSeries,
    timestamp: i64,
    value: f64,
    on_duplicate: Option<DupPolicy>,
    now_ms: i64,
) -> Result<(), ModuleError> {
    let rules = {
        let mut guard = series.write();
        let _ = guard.add_sample(timestamp, value, now_ms, on_duplicate)?;
        guard.rules.clone()
    };
    apply_rules(engine, key, series, timestamp, &rules, now_ms)?;
    Ok(())
}

fn apply_rules(
    engine: &TsEngine,
    _source_key: &[u8],
    source: &SharedTimeSeries,
    timestamp: i64,
    rules: &[CompactionRule],
    now_ms: i64,
) -> Result<(), ModuleError> {
    let source_guard = source.read();
    let all = source_guard.all_samples();
    drop(source_guard);
    for rule in rules {
        let bucket = rule.bucket_duration as i64;
        if bucket <= 0 {
            continue;
        }
        let start = align_bucket(timestamp, bucket, rule.align_ts as i64);
        let end = start.saturating_add(bucket);
        let mut agg = Aggregator::default();
        for (ts, value) in all.iter().copied() {
            if ts >= start && ts < end {
                agg.push(ts, value);
            }
        }
        if let Some(value) = agg.value(rule.aggregation, start, end)
            && let Some(dest) = engine.get_series(rule.dest_key.as_ref())
        {
            let _ = dest
                .write()
                .add_sample(start, value, now_ms, Some(DupPolicy::Last))
                .map_err(ModuleError::from)?;
        }
    }
    Ok(())
}

fn engine(ctx: &dyn ModuleCommandContext) -> Result<Arc<TsEngine>, ModuleError> {
    ctx.shard_extensions()
        .get::<TsEngine>()
        .ok_or_else(|| ModuleError::new("ERR TSDB engine is not initialized"))
}

fn ensure_key_slot_free(ctx: &mut dyn ModuleCommandContext, key: &[u8]) -> Result<(), ModuleError> {
    if ctx.get_value(key).is_some() {
        return Err(TsError::WrongType.into());
    }
    Ok(())
}

fn get_series(
    engine: &TsEngine,
    ctx: &mut dyn ModuleCommandContext,
    key: &[u8],
) -> Result<SharedTimeSeries, ModuleError> {
    maybe_get_series(engine, ctx, key)?.ok_or_else(|| TsError::KeyNotFound.into())
}

fn maybe_get_series(
    engine: &TsEngine,
    ctx: &mut dyn ModuleCommandContext,
    key: &[u8],
) -> Result<Option<SharedTimeSeries>, ModuleError> {
    if let Some(series) = engine.get_series(key) {
        return Ok(Some(series));
    }
    if ctx.get_value(key).is_some() {
        return Err(TsError::WrongType.into());
    }
    Ok(None)
}

fn parse_create_options(args: &[&[u8]], allow_missing: bool) -> Result<CreateOptions, ModuleError> {
    let mut options = CreateOptions::default();
    let mut index = 0usize;
    while index < args.len() {
        if eq_ascii(args[index], b"RETENTION") {
            index += 1;
            options.retention_ms = Some(parse_u64(
                args.get(index)
                    .ok_or_else(|| err("ERR TSDB: invalid retention"))?,
            )?);
        } else if eq_ascii(args[index], b"ENCODING") {
            index += 1;
            options.encoding = Some(parse_encoding(
                args.get(index)
                    .ok_or_else(|| err("ERR TSDB: invalid encoding"))?,
            )?);
        } else if eq_ascii(args[index], b"CHUNK") {
            index += 2;
            options.chunk_size = Some(parse_usize(
                args.get(index - 1)
                    .ok_or_else(|| err("ERR TSDB: invalid chunk size"))?,
            )?);
        } else if eq_ascii(args[index], b"DUPLICATE") {
            index += 2;
            options.dup_policy = Some(parse_dup_policy(
                args.get(index - 1)
                    .ok_or_else(|| err("ERR TSDB: invalid duplicate policy"))?,
            )?);
        } else if eq_ascii(args[index], b"IGNORE") {
            let time = parse_i64(
                args.get(index + 1)
                    .ok_or_else(|| err("ERR TSDB: invalid ignore"))?,
            )?;
            let value = parse_f64(
                args.get(index + 2)
                    .ok_or_else(|| err("ERR TSDB: invalid ignore"))?,
            )?;
            options.ignore = Some(IgnoreConfig {
                max_time_diff: time,
                max_val_diff: value,
            });
            index += 2;
        } else if eq_ascii(args[index], b"LABELS") {
            let labels = parse_labels(&args[index + 1..])?;
            options.labels = Some(labels);
            return Ok(options);
        } else if !allow_missing {
            return Err(ModuleError::new(format!(
                "ERR TSDB: unknown argument '{}'",
                String::from_utf8_lossy(args[index])
            )));
        }
        index += 1;
    }
    Ok(options)
}

fn parse_add_options(args: &[&[u8]]) -> Result<AddOptions, ModuleError> {
    let mut create = CreateOptions::default();
    let mut on_duplicate = None;
    let mut index = 0usize;
    while index < args.len() {
        if eq_ascii(args[index], b"ON")
            && args
                .get(index + 1)
                .is_some_and(|value| eq_ascii(value, b"DUPLICATE"))
        {
            on_duplicate = Some(parse_dup_policy(
                args.get(index + 2)
                    .ok_or_else(|| err("ERR TSDB: invalid duplicate policy"))?,
            )?);
            index += 3;
            continue;
        }
        let parsed = parse_create_options(&args[index..], false)?;
        if parsed.labels.is_some() {
            create.labels = parsed.labels;
            break;
        }
        if parsed.retention_ms.is_some() {
            create.retention_ms = parsed.retention_ms;
        }
        if parsed.encoding.is_some() {
            create.encoding = parsed.encoding;
        }
        if parsed.chunk_size.is_some() {
            create.chunk_size = parsed.chunk_size;
        }
        if parsed.dup_policy.is_some() {
            create.dup_policy = parsed.dup_policy;
        }
        if parsed.ignore.is_some() {
            create.ignore = parsed.ignore;
        }
        break;
    }
    Ok(AddOptions {
        create,
        on_duplicate,
    })
}

fn apply_create_options(series: &mut TimeSeries, options: CreateOptions) {
    if let Some(retention) = options.retention_ms {
        series.retention_ms = retention;
    }
    if let Some(encoding) = options.encoding {
        series.encoding = encoding;
    }
    if let Some(chunk_size) = options.chunk_size {
        series.chunk_size = chunk_size;
    }
    if let Some(dup_policy) = options.dup_policy {
        series.dup_policy = dup_policy;
    }
    if let Some(ignore) = options.ignore {
        series.ignore = ignore;
    }
    if let Some(labels) = options.labels {
        series.labels = labels;
    }
}

fn apply_alter_options(series: &mut TimeSeries, options: CreateOptions) {
    apply_create_options(series, options);
}

fn parse_labels(args: &[&[u8]]) -> Result<Vec<(String, String)>, ModuleError> {
    if !args.len().is_multiple_of(2) {
        return Err(err("ERR TSDB: wrong number of labels"));
    }
    args.chunks(2)
        .map(|pair| Ok((parse_string(pair[0])?, parse_string(pair[1])?)))
        .collect()
}

fn parse_multi_query_tail(
    args: &[&[u8]],
) -> Result<(LabelMode, Vec<Filter>, Vec<String>), ModuleError> {
    let mut label_mode = LabelMode::None;
    let mut selected = Vec::new();
    let mut index = 0usize;
    while index < args.len() {
        if eq_ascii(args[index], b"LATEST") {
            index += 1;
        } else if eq_ascii(args[index], b"WITHLABELS") {
            label_mode = LabelMode::WithLabels;
            index += 1;
        } else if eq_ascii(args[index], b"SELECTED")
            && args
                .get(index + 1)
                .is_some_and(|value| eq_ascii(value, b"LABELS"))
        {
            label_mode = LabelMode::Selected;
            index += 2;
            while index < args.len() && !eq_ascii(args[index], b"FILTER") {
                selected.push(parse_string(args[index])?);
                index += 1;
            }
        } else if eq_ascii(args[index], b"FILTER") {
            return Ok((label_mode, parse_filters(&args[index + 1..])?, selected));
        } else {
            return Err(ModuleError::new(format!(
                "ERR TSDB: unexpected token '{}'",
                String::from_utf8_lossy(args[index])
            )));
        }
    }
    Err(err("ERR TSDB: FILTER argument is missing"))
}

type MultiRangeQuery = (
    RangeQuery,
    LabelMode,
    Vec<String>,
    Vec<Filter>,
    Option<(String, Aggregation)>,
);

fn parse_multi_range_query(args: &[&[u8]]) -> Result<MultiRangeQuery, ModuleError> {
    let mut query = RangeQuery::default();
    let mut label_mode = LabelMode::None;
    let mut selected = Vec::new();
    let mut group_by = None;
    let mut index = 0usize;
    while index < args.len() {
        if eq_ascii(args[index], b"WITHLABELS") {
            label_mode = LabelMode::WithLabels;
            index += 1;
        } else if eq_ascii(args[index], b"SELECTED")
            && args
                .get(index + 1)
                .is_some_and(|value| eq_ascii(value, b"LABELS"))
        {
            label_mode = LabelMode::Selected;
            index += 2;
            while index < args.len() && !eq_ascii(args[index], b"FILTER") {
                selected.push(parse_string(args[index])?);
                index += 1;
            }
        } else if eq_ascii(args[index], b"FILTER") {
            let filters_end = args[index + 1..]
                .iter()
                .position(|value| eq_ascii(value, b"GROUPBY"))
                .map(|pos| index + 1 + pos)
                .unwrap_or(args.len());
            let filters = parse_filters(&args[index + 1..filters_end])?;
            index = filters_end;
            if index < args.len() {
                if args.len() < index + 4
                    || !eq_ascii(args[index], b"GROUPBY")
                    || !eq_ascii(args[index + 2], b"REDUCE")
                {
                    return Err(err("ERR TSDB: invalid GROUPBY/REDUCE clause"));
                }
                group_by = Some((
                    parse_string(args[index + 1])?,
                    parse_aggregation(args[index + 3])?,
                ));
            }
            return Ok((query, label_mode, selected, filters, group_by));
        } else {
            let consumed = parse_range_query_one(&mut query, &args[index..])?;
            index += consumed;
        }
    }
    Err(err("ERR TSDB: FILTER argument is missing"))
}

fn parse_range_query(args: &[&[u8]]) -> Result<RangeQuery, ModuleError> {
    let mut query = RangeQuery::default();
    let mut index = 0usize;
    while index < args.len() {
        let consumed = parse_range_query_one(&mut query, &args[index..])?;
        index += consumed;
    }
    Ok(query)
}

fn parse_range_query_one(query: &mut RangeQuery, args: &[&[u8]]) -> Result<usize, ModuleError> {
    if args.is_empty() {
        return Ok(0);
    }
    if eq_ascii(args[0], b"LATEST") {
        query.latest = true;
        return Ok(1);
    }
    if eq_ascii(args[0], b"FILTER")
        && args.get(1).is_some_and(|value| eq_ascii(value, b"BY"))
        && args.get(2).is_some_and(|value| eq_ascii(value, b"TS"))
    {
        let mut index = 3usize;
        let mut set = ahash::AHashSet::new();
        while index < args.len() && !is_range_keyword(args[index]) {
            set.insert(parse_i64(args[index])?);
            index += 1;
        }
        query.filter_ts = Some(set);
        return Ok(index);
    }
    if eq_ascii(args[0], b"FILTER")
        && args.get(1).is_some_and(|value| eq_ascii(value, b"BY"))
        && args.get(2).is_some_and(|value| eq_ascii(value, b"VALUE"))
    {
        let min = parse_f64(
            args.get(3)
                .ok_or_else(|| err("ERR TSDB: invalid FILTER_BY_VALUE"))?,
        )?;
        let max = parse_f64(
            args.get(4)
                .ok_or_else(|| err("ERR TSDB: invalid FILTER_BY_VALUE"))?,
        )?;
        query.filter_value = Some((min, max));
        return Ok(5);
    }
    if eq_ascii(args[0], b"COUNT") {
        query.count = Some(parse_usize(
            args.get(1).ok_or_else(|| err("ERR TSDB: invalid COUNT"))?,
        )?);
        return Ok(2);
    }
    if eq_ascii(args[0], b"ALIGN") {
        query.align = Some(parse_i64(
            args.get(1).ok_or_else(|| err("ERR TSDB: invalid ALIGN"))?,
        )?);
        return Ok(2);
    }
    if eq_ascii(args[0], b"AGGREGATION") {
        let aggregation = parse_aggregation(
            args.get(1)
                .ok_or_else(|| err("ERR TSDB: invalid AGGREGATION"))?,
        )?;
        let bucket = parse_u64(
            args.get(2)
                .ok_or_else(|| err("ERR TSDB: invalid AGGREGATION"))?,
        )?;
        let mut consumed = 3usize;
        let mut bucket_timestamp = BucketTimestamp::Start;
        let mut empty = false;
        while consumed < args.len() {
            if eq_ascii(args[consumed], b"BUCKETTIMESTAMP") {
                bucket_timestamp = parse_bucket_timestamp(
                    args.get(consumed + 1)
                        .ok_or_else(|| err("ERR TSDB: invalid BUCKETTIMESTAMP"))?,
                )?;
                consumed += 2;
            } else if eq_ascii(args[consumed], b"EMPTY") {
                empty = true;
                consumed += 1;
            } else {
                break;
            }
        }
        query.aggregation = Some((aggregation, bucket, bucket_timestamp, empty));
        return Ok(consumed);
    }
    Err(ModuleError::new(format!(
        "ERR TSDB: unexpected token '{}'",
        String::from_utf8_lossy(args[0])
    )))
}

fn parse_filters(args: &[&[u8]]) -> Result<Vec<Filter>, ModuleError> {
    if args.is_empty() {
        return Err(err("ERR TSDB: FILTER argument is missing"));
    }
    args.iter().map(|raw| parse_filter(raw)).collect()
}

fn parse_filter(raw: &[u8]) -> Result<Filter, ModuleError> {
    let text = parse_string(raw)?;
    if let Some((label, value)) = text.split_once("!=") {
        if value.is_empty() {
            return Ok(Filter::NotExists(label.to_string()));
        }
        if value.starts_with('(') && value.ends_with(')') {
            return Ok(Filter::NotIn(label.to_string(), parse_filter_set(value)?));
        }
        return Ok(Filter::Ne(label.to_string(), value.to_string()));
    }
    if let Some((label, value)) = text.split_once('=') {
        if value.is_empty() {
            return Ok(Filter::Exists(label.to_string()));
        }
        if value.starts_with('(') && value.ends_with(')') {
            return Ok(Filter::In(label.to_string(), parse_filter_set(value)?));
        }
        return Ok(Filter::Eq(label.to_string(), value.to_string()));
    }
    Err(TsError::BadFilter.into())
}

fn filter_series(engine: &TsEngine, filters: &[Filter]) -> Vec<(Bytes, SharedTimeSeries)> {
    let mut out = engine
        .series
        .iter()
        .filter_map(|entry| {
            let key = entry.key().clone();
            let series = Arc::clone(entry.value());
            let matches = {
                let labels = series.read().labels.clone();
                filters.iter().all(|filter| filter_matches(filter, &labels))
            };
            matches.then_some((key, series))
        })
        .collect::<Vec<_>>();
    out.sort_by(|left, right| left.0.cmp(&right.0));
    out
}

fn filter_matches(filter: &Filter, labels: &[(String, String)]) -> bool {
    match filter {
        Filter::Eq(label, value) => labels.iter().any(|(k, v)| k == label && v == value),
        Filter::Ne(label, value) => labels.iter().all(|(k, v)| k != label || v != value),
        Filter::Exists(label) => labels.iter().any(|(k, _)| k == label),
        Filter::NotExists(label) => labels.iter().all(|(k, _)| k != label),
        Filter::In(label, values) => labels.iter().any(|(k, v)| k == label && values.contains(v)),
        Filter::NotIn(label, values) => labels
            .iter()
            .all(|(k, v)| k != label || !values.contains(v)),
    }
}

fn parse_filter_set(raw: &str) -> Result<BTreeSet<String>, ModuleError> {
    Ok(raw[1..raw.len() - 1]
        .split(',')
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect())
}

fn labels_response(
    labels: &[(String, String)],
    mode: LabelMode,
    selected: &[String],
) -> ModuleResponse {
    match mode {
        LabelMode::None => ModuleResponse::Array(Box::default()),
        LabelMode::WithLabels => ModuleResponse::Array(Box::new(
            labels
                .iter()
                .map(|(label, value)| {
                    ModuleResponse::Array(Box::new(smallvec![
                        bulk(label.as_bytes()),
                        bulk(value.as_bytes()),
                    ]))
                })
                .collect(),
        )),
        LabelMode::Selected => ModuleResponse::Array(Box::new(
            labels
                .iter()
                .filter(|(label, _)| selected.iter().any(|selected| selected == label))
                .map(|(label, value)| {
                    ModuleResponse::Array(Box::new(smallvec![
                        bulk(label.as_bytes()),
                        bulk(value.as_bytes()),
                    ]))
                })
                .collect(),
        )),
    }
}

fn rules_response(rules: &[CompactionRule]) -> ModuleResponse {
    ModuleResponse::Array(Box::new(
        rules
            .iter()
            .map(|rule| {
                ModuleResponse::Array(Box::new(smallvec![
                    bulk(rule.dest_key.as_ref()),
                    bulk(aggregation_name(rule.aggregation)),
                    ModuleResponse::Integer(rule.bucket_duration as i64),
                    ModuleResponse::Integer(rule.align_ts as i64),
                ]))
            })
            .collect(),
    ))
}

fn samples_response(samples: &[(i64, f64)]) -> ModuleResponse {
    ModuleResponse::Array(Box::new(
        samples
            .iter()
            .map(|(ts, value)| sample_pair_response(*ts, *value))
            .collect(),
    ))
}

fn sample_response(sample: (i64, f64)) -> ModuleResponse {
    sample_pair_response(sample.0, sample.1)
}

fn sample_pair_response(ts: i64, value: f64) -> ModuleResponse {
    ModuleResponse::Array(Box::new(smallvec![
        ModuleResponse::Integer(ts),
        bulk(format_f64(value)),
    ]))
}

fn bulk(value: impl AsRef<[u8]>) -> ModuleResponse {
    ModuleResponse::Bulk(Some(Bytes::copy_from_slice(value.as_ref())))
}

fn parse_timestamp(raw: &[u8], now_ms: i64) -> Result<i64, ModuleError> {
    if raw == b"*" {
        Ok(now_ms)
    } else {
        parse_i64(raw)
    }
}

fn parse_ts_endpoint(raw: &[u8]) -> Result<i64, ModuleError> {
    match raw {
        b"-" | b"-inf" => Ok(i64::MIN),
        b"+" | b"+inf" => Ok(i64::MAX),
        _ => parse_i64(raw),
    }
}

fn parse_encoding(raw: &[u8]) -> Result<Encoding, ModuleError> {
    if eq_ascii(raw, b"COMPRESSED") {
        Ok(Encoding::Compressed)
    } else if eq_ascii(raw, b"UNCOMPRESSED") {
        Ok(Encoding::Uncompressed)
    } else {
        Err(err("ERR TSDB: invalid encoding"))
    }
}

fn parse_dup_policy(raw: &[u8]) -> Result<DupPolicy, ModuleError> {
    if eq_ascii(raw, b"BLOCK") {
        Ok(DupPolicy::Block)
    } else if eq_ascii(raw, b"FIRST") {
        Ok(DupPolicy::First)
    } else if eq_ascii(raw, b"LAST") {
        Ok(DupPolicy::Last)
    } else if eq_ascii(raw, b"MIN") {
        Ok(DupPolicy::Min)
    } else if eq_ascii(raw, b"MAX") {
        Ok(DupPolicy::Max)
    } else if eq_ascii(raw, b"SUM") {
        Ok(DupPolicy::Sum)
    } else {
        Err(err("ERR TSDB: invalid duplicate policy"))
    }
}

fn parse_aggregation(raw: &[u8]) -> Result<Aggregation, ModuleError> {
    if eq_ascii(raw, b"AVG") {
        Ok(Aggregation::Avg)
    } else if eq_ascii(raw, b"FIRST") {
        Ok(Aggregation::First)
    } else if eq_ascii(raw, b"LAST") {
        Ok(Aggregation::Last)
    } else if eq_ascii(raw, b"MIN") {
        Ok(Aggregation::Min)
    } else if eq_ascii(raw, b"MAX") {
        Ok(Aggregation::Max)
    } else if eq_ascii(raw, b"SUM") {
        Ok(Aggregation::Sum)
    } else if eq_ascii(raw, b"RANGE") {
        Ok(Aggregation::Range)
    } else if eq_ascii(raw, b"COUNT") {
        Ok(Aggregation::Count)
    } else if eq_ascii(raw, b"STD.P") {
        Ok(Aggregation::StdP)
    } else if eq_ascii(raw, b"STD.S") {
        Ok(Aggregation::StdS)
    } else if eq_ascii(raw, b"VAR.P") {
        Ok(Aggregation::VarP)
    } else if eq_ascii(raw, b"VAR.S") {
        Ok(Aggregation::VarS)
    } else if eq_ascii(raw, b"TWA") {
        Ok(Aggregation::Twa)
    } else {
        Err(err("ERR TSDB: invalid aggregation type"))
    }
}

fn parse_bucket_timestamp(raw: &[u8]) -> Result<BucketTimestamp, ModuleError> {
    if eq_ascii(raw, b"-") || eq_ascii(raw, b"LOW") || eq_ascii(raw, b"START") {
        Ok(BucketTimestamp::Start)
    } else if eq_ascii(raw, b"+") || eq_ascii(raw, b"HIGH") || eq_ascii(raw, b"END") {
        Ok(BucketTimestamp::End)
    } else if eq_ascii(raw, b"~") || eq_ascii(raw, b"MID") {
        Ok(BucketTimestamp::Mid)
    } else {
        Err(err("ERR TSDB: invalid bucket timestamp"))
    }
}

fn parse_u64(raw: &[u8]) -> Result<u64, ModuleError> {
    parse_string(raw)?
        .parse::<u64>()
        .map_err(|_| err("ERR TSDB: invalid integer"))
}

fn parse_usize(raw: &[u8]) -> Result<usize, ModuleError> {
    parse_string(raw)?
        .parse::<usize>()
        .map_err(|_| err("ERR TSDB: invalid integer"))
}

fn parse_i64(raw: &[u8]) -> Result<i64, ModuleError> {
    parse_string(raw)?
        .parse::<i64>()
        .map_err(|_| TsError::BadTimestamp.into())
}

fn parse_f64(raw: &[u8]) -> Result<f64, ModuleError> {
    parse_string(raw)?
        .parse::<f64>()
        .map_err(|_| TsError::BadValue.into())
}

fn parse_string(raw: &[u8]) -> Result<String, ModuleError> {
    std::str::from_utf8(raw)
        .map(str::to_string)
        .map_err(|_| err("ERR TSDB: invalid string"))
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn is_range_keyword(raw: &[u8]) -> bool {
    eq_ascii(raw, b"COUNT")
        || eq_ascii(raw, b"ALIGN")
        || eq_ascii(raw, b"AGGREGATION")
        || eq_ascii(raw, b"FILTER")
        || eq_ascii(raw, b"WITHLABELS")
        || eq_ascii(raw, b"SELECTED")
        || eq_ascii(raw, b"GROUPBY")
        || eq_ascii(raw, b"LATEST")
        || eq_ascii(raw, b"BUCKETTIMESTAMP")
        || eq_ascii(raw, b"EMPTY")
}

fn aggregation_name(aggregation: Aggregation) -> &'static [u8] {
    match aggregation {
        Aggregation::Avg => b"avg",
        Aggregation::First => b"first",
        Aggregation::Last => b"last",
        Aggregation::Min => b"min",
        Aggregation::Max => b"max",
        Aggregation::Sum => b"sum",
        Aggregation::Range => b"range",
        Aggregation::Count => b"count",
        Aggregation::StdP => b"std.p",
        Aggregation::StdS => b"std.s",
        Aggregation::VarP => b"var.p",
        Aggregation::VarS => b"var.s",
        Aggregation::Twa => b"twa",
    }
}

fn dup_policy_name(policy: DupPolicy) -> &'static [u8] {
    match policy {
        DupPolicy::Block => b"block",
        DupPolicy::First => b"first",
        DupPolicy::Last => b"last",
        DupPolicy::Min => b"min",
        DupPolicy::Max => b"max",
        DupPolicy::Sum => b"sum",
    }
}

fn format_f64(value: f64) -> Vec<u8> {
    if value.is_nan() {
        return b"nan".to_vec();
    }
    let mut rendered = value.to_string();
    if !rendered.contains('.') && !rendered.contains('e') && !rendered.contains('E') {
        rendered.push_str(".0");
    }
    rendered.into_bytes()
}

fn current_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn align_bucket(timestamp: i64, bucket_duration: i64, align_ts: i64) -> i64 {
    let delta = timestamp.saturating_sub(align_ts);
    let offset = delta.rem_euclid(bucket_duration);
    timestamp.saturating_sub(offset)
}

fn err(message: impl Into<String>) -> ModuleError {
    ModuleError::new(message.into())
}

impl From<TsError> for ModuleError {
    fn from(value: TsError) -> Self {
        ModuleError::new(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use senko_core::{SenkoValue, ShardExtensions};
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct TestContext {
        values: BTreeMap<Vec<u8>, SenkoValue>,
        extensions: Arc<ShardExtensions>,
    }

    impl TestContext {
        fn new() -> Self {
            let extensions = Arc::new(ShardExtensions::default());
            extensions.set(Arc::new(TsEngine::default()));
            Self {
                values: BTreeMap::new(),
                extensions,
            }
        }
    }

    impl ModuleCommandContext for TestContext {
        fn shard_id(&self) -> usize {
            0
        }

        fn shard_extensions(&self) -> &ShardExtensions {
            &self.extensions
        }

        fn get_value(&mut self, key: &[u8]) -> Option<SenkoValue> {
            self.values.get(key).cloned()
        }

        fn set_value(&mut self, key: &[u8], value: SenkoValue) {
            self.values.insert(key.to_vec(), value);
        }

        fn delete_key(&mut self, key: &[u8]) -> u64 {
            u64::from(self.values.remove(key).is_some())
        }
    }

    #[test]
    fn create_add_get_roundtrip() {
        let mut ctx = TestContext::new();
        assert_eq!(
            ts_create(&mut ctx, &[b"s"]),
            Ok(ModuleResponse::Simple(b"OK"))
        );
        assert_eq!(
            ts_add(&mut ctx, &[b"s", b"10", b"2.5"]),
            Ok(ModuleResponse::Integer(10))
        );
        let response = ts_get(&mut ctx, &[b"s"]).unwrap();
        assert_eq!(
            response,
            ModuleResponse::Array(Box::new(smallvec![
                ModuleResponse::Integer(10),
                bulk(b"2.5"),
            ]))
        );
    }

    #[test]
    fn queryindex_filters_by_labels() {
        let mut ctx = TestContext::new();
        let _ = ts_create(
            &mut ctx,
            &[b"a", b"LABELS", b"sensor", b"temp", b"site", b"eu"],
        );
        let _ = ts_create(&mut ctx, &[b"b", b"LABELS", b"sensor", b"humid"]);
        let response = ts_queryindex(&mut ctx, &[b"sensor=temp"]).unwrap();
        assert_eq!(
            response,
            ModuleResponse::Array(Box::new(smallvec![bulk(b"a")]))
        );
    }

    #[test]
    fn range_aggregation_returns_bucketed_samples() {
        let mut ctx = TestContext::new();
        let _ = ts_create(&mut ctx, &[b"s"]);
        for (ts, value) in [(0, "1"), (10, "3"), (20, "5"), (30, "7")] {
            let _ = ts_add(
                &mut ctx,
                &[b"s", ts.to_string().as_bytes(), value.as_bytes()],
            );
        }
        let response = ts_range(
            &mut ctx,
            &[b"s", b"0", b"39", b"AGGREGATION", b"AVG", b"20"],
        )
        .unwrap();
        assert_eq!(
            response,
            ModuleResponse::Array(Box::new(smallvec![
                ModuleResponse::Array(Box::new(smallvec![
                    ModuleResponse::Integer(0),
                    bulk(b"2.0"),
                ])),
                ModuleResponse::Array(Box::new(smallvec![
                    ModuleResponse::Integer(20),
                    bulk(b"6.0"),
                ])),
            ]))
        );
    }
}
