use std::sync::Arc;

use ahash::AHashSet;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;

use crate::{
    agg::{Aggregation, Aggregator},
    error::TsError,
    gorilla::CompressedChunk,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Compressed,
    Uncompressed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DupPolicy {
    Block,
    First,
    Last,
    Min,
    Max,
    Sum,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IgnoreConfig {
    pub max_time_diff: i64,
    pub max_val_diff: f64,
}

impl Default for IgnoreConfig {
    fn default() -> Self {
        Self {
            max_time_diff: 0,
            max_val_diff: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeSeries {
    pub chunks: Vec<Chunk>,
    pub retention_ms: u64,
    pub chunk_size: usize,
    pub encoding: Encoding,
    pub dup_policy: DupPolicy,
    pub ignore: IgnoreConfig,
    pub labels: Vec<(String, String)>,
    pub rules: Vec<CompactionRule>,
    pub last_ts: i64,
    pub last_val: f64,
    pub total_samples: u64,
}

impl Default for TimeSeries {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            retention_ms: 0,
            chunk_size: 4096,
            encoding: Encoding::Compressed,
            dup_policy: DupPolicy::Block,
            ignore: IgnoreConfig::default(),
            labels: Vec::new(),
            rules: Vec::new(),
            last_ts: i64::MIN,
            last_val: 0.0,
            total_samples: 0,
        }
    }
}

impl TimeSeries {
    pub fn latest_sample(&self) -> Option<(i64, f64)> {
        (self.total_samples > 0).then_some((self.last_ts, self.last_val))
    }

    pub fn add_sample(
        &mut self,
        ts: i64,
        value: f64,
        now_ms: i64,
        on_duplicate: Option<DupPolicy>,
    ) -> Result<bool, TsError> {
        if self.should_ignore(ts, value) {
            return Ok(false);
        }

        let mut samples = self.all_samples();
        match samples.binary_search_by_key(&ts, |(sample_ts, _)| *sample_ts) {
            Ok(index) => {
                let policy = on_duplicate.unwrap_or(self.dup_policy);
                match policy {
                    DupPolicy::Block => return Err(TsError::Blocked),
                    DupPolicy::First => return Ok(false),
                    DupPolicy::Last => samples[index].1 = value,
                    DupPolicy::Min => samples[index].1 = samples[index].1.min(value),
                    DupPolicy::Max => samples[index].1 = samples[index].1.max(value),
                    DupPolicy::Sum => samples[index].1 += value,
                }
            }
            Err(index) => samples.insert(index, (ts, value)),
        }

        self.rebuild_from_samples(&samples, now_ms);
        Ok(true)
    }

    pub fn query_range(
        &self,
        from: i64,
        to: i64,
        reverse: bool,
        filter_ts: Option<&AHashSet<i64>>,
        filter_value: Option<(f64, f64)>,
        count: Option<usize>,
    ) -> Vec<(i64, f64)> {
        let mut samples = self
            .all_samples()
            .into_iter()
            .filter(|(ts, value)| {
                *ts >= from
                    && *ts <= to
                    && filter_ts.is_none_or(|allowed| allowed.contains(ts))
                    && filter_value.is_none_or(|(min, max)| *value >= min && *value <= max)
            })
            .collect::<Vec<_>>();
        if reverse {
            samples.reverse();
        }
        if let Some(limit) = count {
            samples.truncate(limit);
        }
        samples
    }

    pub fn aggregate(
        &self,
        samples: &[(i64, f64)],
        aggregation: Aggregation,
        bucket_duration: u64,
        align_ts: i64,
        bucket_timestamp: BucketTimestamp,
        empty: bool,
        from: i64,
        to: i64,
    ) -> Vec<(i64, f64)> {
        if bucket_duration == 0 {
            return samples.to_vec();
        }
        let mut out = Vec::new();
        let mut index = 0usize;
        let bucket_duration = bucket_duration as i64;
        let mut bucket_start = align_bucket(from, bucket_duration, align_ts);
        while bucket_start <= to {
            let bucket_end = bucket_start.saturating_add(bucket_duration);
            let mut aggregator = Aggregator::default();
            while let Some((ts, value)) = samples.get(index).copied() {
                if ts < bucket_start {
                    index += 1;
                    continue;
                }
                if ts >= bucket_end {
                    break;
                }
                aggregator.push(ts, value);
                index += 1;
            }
            if let Some(value) = aggregator.value(aggregation, bucket_start, bucket_end) {
                out.push((bucket_ts(bucket_start, bucket_end, bucket_timestamp), value));
            } else if empty {
                out.push((
                    bucket_ts(bucket_start, bucket_end, bucket_timestamp),
                    f64::NAN,
                ));
            }
            bucket_start = bucket_start.saturating_add(bucket_duration);
        }
        out
    }

    pub fn all_samples(&self) -> Vec<(i64, f64)> {
        let mut out = Vec::new();
        for chunk in &self.chunks {
            out.extend(chunk.samples());
        }
        out
    }

    pub fn delete_range(&mut self, from: i64, to: i64) -> usize {
        let mut deleted = 0usize;
        for chunk in &mut self.chunks {
            let samples = chunk.samples();
            let retained = samples
                .into_iter()
                .filter(|(ts, _)| {
                    let keep = *ts < from || *ts > to;
                    if !keep {
                        deleted += 1;
                    }
                    keep
                })
                .collect::<Vec<_>>();
            chunk.replace_samples(&retained);
        }
        self.recompute_summary();
        deleted
    }

    fn should_ignore(&self, ts: i64, value: f64) -> bool {
        self.total_samples > 0
            && (ts - self.last_ts) <= self.ignore.max_time_diff
            && (value - self.last_val).abs() <= self.ignore.max_val_diff
    }

    fn enforce_retention(&mut self, now_ms: i64) {
        if self.retention_ms == 0 {
            return;
        }
        let cutoff = now_ms.saturating_sub(self.retention_ms as i64);
        while self
            .chunks
            .first()
            .is_some_and(|chunk| chunk.max_ts < cutoff)
        {
            self.chunks.remove(0);
        }
        self.recompute_summary();
    }

    fn rebuild_from_samples(&mut self, samples: &[(i64, f64)], now_ms: i64) {
        self.chunks.clear();
        for &(ts, value) in samples {
            let needs_new_chunk = self
                .chunks
                .last()
                .is_none_or(|chunk| chunk.approx_size() >= self.chunk_size);
            if needs_new_chunk {
                self.chunks.push(Chunk::new(ts, self.encoding));
            }
            self.chunks
                .last_mut()
                .expect("time series chunk exists")
                .push(ts, value);
        }
        self.enforce_retention(now_ms);
        self.recompute_summary();
    }

    fn recompute_summary(&mut self) {
        self.chunks.retain(|chunk| chunk.num_samples > 0);
        self.total_samples = self
            .chunks
            .iter()
            .map(|chunk| u64::from(chunk.num_samples))
            .sum();
        if let Some((ts, value)) = self.all_samples().last().copied() {
            self.last_ts = ts;
            self.last_val = value;
        } else {
            self.last_ts = i64::MIN;
            self.last_val = 0.0;
        }
    }
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub base_ts: i64,
    pub max_ts: i64,
    pub num_samples: u16,
    pub data: ChunkData,
}

impl Chunk {
    pub fn new(base_ts: i64, encoding: Encoding) -> Self {
        Self {
            base_ts,
            max_ts: base_ts,
            num_samples: 0,
            data: match encoding {
                Encoding::Compressed => ChunkData::Compressed(CompressedChunk::new()),
                Encoding::Uncompressed => ChunkData::Uncompressed(Vec::new()),
            },
        }
    }

    pub fn push(&mut self, ts: i64, value: f64) {
        self.max_ts = ts;
        self.num_samples = self.num_samples.saturating_add(1);
        match &mut self.data {
            ChunkData::Compressed(chunk) => chunk.compress_sample(ts, value),
            ChunkData::Uncompressed(samples) => samples.push((ts, value)),
        }
    }

    pub fn samples(&self) -> Vec<(i64, f64)> {
        match &self.data {
            ChunkData::Compressed(chunk) => chunk.decompress_all(),
            ChunkData::Uncompressed(samples) => samples.clone(),
        }
    }

    pub fn replace_samples(&mut self, samples: &[(i64, f64)]) {
        self.num_samples = samples.len() as u16;
        if let Some((first_ts, _)) = samples.first() {
            self.base_ts = *first_ts;
        }
        if let Some((last_ts, _)) = samples.last() {
            self.max_ts = *last_ts;
        }
        self.data = match &self.data {
            ChunkData::Compressed(_) => {
                ChunkData::Compressed(CompressedChunk::from_samples(samples))
            }
            ChunkData::Uncompressed(_) => ChunkData::Uncompressed(samples.to_vec()),
        };
    }

    pub fn approx_size(&self) -> usize {
        match &self.data {
            ChunkData::Compressed(chunk) => chunk.byte_len(),
            ChunkData::Uncompressed(samples) => samples.len() * std::mem::size_of::<(i64, f64)>(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ChunkData {
    Compressed(CompressedChunk),
    Uncompressed(Vec<(i64, f64)>),
}

#[derive(Debug, Clone)]
pub struct CompactionRule {
    pub dest_key: Bytes,
    pub aggregation: Aggregation,
    pub bucket_duration: u64,
    pub align_ts: u64,
    pub state: Option<CompactionState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketTimestamp {
    Start,
    End,
    Mid,
}

#[derive(Debug, Clone)]
pub struct CompactionState {
    pub bucket_start: i64,
    pub bucket_end: i64,
    pub samples: Vec<(i64, f64)>,
}

#[derive(Debug, Default)]
pub struct TsEngine {
    pub series: DashMap<Bytes, SharedTimeSeries>,
    pub label_index: DashMap<String, AHashSet<Bytes>>,
}

impl TsEngine {
    pub fn get_series(&self, key: &[u8]) -> Option<SharedTimeSeries> {
        self.series.get(key).map(|entry| Arc::clone(entry.value()))
    }

    pub fn create_series(
        &self,
        key: Bytes,
        series: TimeSeries,
    ) -> Result<SharedTimeSeries, TsError> {
        let shared = Arc::new(RwLock::new(series));
        match self.series.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(_) => Err(TsError::KeyExists),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&shared));
                Ok(shared)
            }
        }
    }

    pub fn remove_series(&self, key: &[u8]) -> Option<SharedTimeSeries> {
        self.series.remove(key).map(|(_, value)| value)
    }

    pub fn index_labels(&self, key: &Bytes, labels: &[(String, String)]) {
        for (label, value) in labels {
            let compound = format!("{label}:{value}");
            self.label_index
                .entry(compound)
                .or_default()
                .insert(key.clone());
        }
    }

    pub fn remove_labels(&self, key: &Bytes, labels: &[(String, String)]) {
        for (label, value) in labels {
            let compound = format!("{label}:{value}");
            if let Some(mut entry) = self.label_index.get_mut(&compound) {
                entry.remove(key);
                if entry.is_empty() {
                    drop(entry);
                    let _ = self.label_index.remove(&compound);
                }
            }
        }
    }
}

pub struct TsModule {
    engine: Arc<TsEngine>,
}

impl TsModule {
    pub fn new() -> Self {
        Self {
            engine: Arc::new(TsEngine::default()),
        }
    }

    pub fn engine(&self) -> &Arc<TsEngine> {
        &self.engine
    }
}

impl Default for TsModule {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedTimeSeries = Arc<RwLock<TimeSeries>>;

#[cfg(test)]
mod tests {
    use super::{BucketTimestamp, DupPolicy, Encoding, IgnoreConfig, TimeSeries};
    use crate::agg::Aggregation;

    #[test]
    fn retention_drops_whole_old_chunks() {
        let mut series = TimeSeries {
            retention_ms: 5_000,
            chunk_size: 1,
            encoding: Encoding::Uncompressed,
            dup_policy: DupPolicy::Block,
            ignore: IgnoreConfig::default(),
            ..TimeSeries::default()
        };
        for idx in 0..12 {
            let ts = 1_000 + idx * 1_000;
            let _ = series.add_sample(ts, idx as f64, ts, None);
        }
        assert!(series.all_samples().iter().all(|(ts, _)| *ts >= 7_000));
    }

    #[test]
    fn delete_middle_preserves_surrounding_samples() {
        let mut series = TimeSeries::default();
        for idx in 0..10 {
            let _ = series.add_sample(idx * 10, idx as f64, idx * 10, None);
        }
        let deleted = series.delete_range(30, 60);
        assert_eq!(deleted, 4);
        let samples = series.all_samples();
        assert_eq!(samples.first().copied(), Some((0, 0.0)));
        assert_eq!(samples.last().copied(), Some((90, 9.0)));
        assert!(!samples.iter().any(|(ts, _)| (30..=60).contains(ts)));
    }

    #[test]
    fn duplicate_sum_policy_updates_existing_sample() {
        let mut series = TimeSeries {
            dup_policy: DupPolicy::Sum,
            ..TimeSeries::default()
        };
        assert!(series.add_sample(10, 1.0, 10, None).unwrap());
        assert!(series.add_sample(10, 2.5, 10, None).unwrap());
        assert_eq!(series.latest_sample(), Some((10, 3.5)));
    }

    #[test]
    fn aggregate_into_buckets() {
        let mut series = TimeSeries::default();
        for (ts, value) in [(0, 1.0), (10, 3.0), (20, 5.0), (30, 7.0)] {
            let _ = series.add_sample(ts, value, ts, None);
        }
        let aggregated = series.aggregate(
            &series.all_samples(),
            Aggregation::Avg,
            20,
            0,
            BucketTimestamp::Start,
            false,
            0,
            39,
        );
        assert_eq!(aggregated, vec![(0, 2.0), (20, 6.0)]);
    }
}

fn align_bucket(timestamp: i64, bucket_duration: i64, align_ts: i64) -> i64 {
    if bucket_duration <= 0 {
        return timestamp;
    }
    let delta = timestamp.saturating_sub(align_ts);
    let offset = delta.rem_euclid(bucket_duration);
    timestamp.saturating_sub(offset)
}

fn bucket_ts(start: i64, end: i64, bucket_timestamp: BucketTimestamp) -> i64 {
    match bucket_timestamp {
        BucketTimestamp::Start => start,
        BucketTimestamp::End => end.saturating_sub(1),
        BucketTimestamp::Mid => start.saturating_add((end.saturating_sub(start)) / 2),
    }
}
