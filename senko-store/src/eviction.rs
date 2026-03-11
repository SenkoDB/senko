use std::sync::atomic::{AtomicUsize, Ordering};

use compact_str::CompactString;

use crate::store::Entry;

pub const EVICTION_SAMPLE_SIZE: usize = 5;
pub const MEMORY_HIGH_WATERMARK_PERCENT: usize = 90;

#[derive(Debug, Default)]
pub struct MemoryAccountant {
    used_memory: AtomicUsize,
}

impl MemoryAccountant {
    pub fn add(&self, bytes: usize) {
        self.used_memory.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn sub(&self, bytes: usize) {
        self.used_memory.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn get(&self) -> usize {
        self.used_memory.load(Ordering::Relaxed)
    }
}

pub fn should_evict(used_memory: usize, max_memory: Option<usize>) -> bool {
    let Some(max_memory) = max_memory else {
        return false;
    };
    used_memory.saturating_mul(100) > max_memory.saturating_mul(MEMORY_HIGH_WATERMARK_PERCENT)
}

pub fn entry_bytes(key: &CompactString, entry: &Entry) -> usize {
    key.len() + value_bytes(entry) + std::mem::size_of::<Entry>()
}

pub fn value_bytes(entry: &Entry) -> usize {
    match &entry.value {
        senko_core::SenkoValue::Raw(raw) => raw.len(),
        senko_core::SenkoValue::Int(_) => std::mem::size_of::<i64>(),
        senko_core::SenkoValue::Float(_) => std::mem::size_of::<f64>(),
        #[cfg(feature = "json")]
        senko_core::SenkoValue::Json(value) => senko_core::SenkoValue::Json(value.clone())
            .as_bytes()
            .len(),
        #[cfg(feature = "vector")]
        senko_core::SenkoValue::VectorSet(vset) => {
            let guard = vset.read();
            guard.nodes.len() * std::mem::size_of::<senko_core::VectorNode>()
        }
        #[cfg(feature = "prob")]
        senko_core::SenkoValue::BloomFilter(filter) => {
            filter.filters.iter().map(|sub| sub.bits.byte_len()).sum()
        }
        #[cfg(feature = "prob")]
        senko_core::SenkoValue::CuckooFilter(filter) => filter
            .layers
            .iter()
            .map(|layer| layer.num_buckets * layer.bucket_size * std::mem::size_of::<u16>())
            .sum(),
        #[cfg(feature = "prob")]
        senko_core::SenkoValue::CountMinSketch(sketch) => {
            sketch.counters.len() * std::mem::size_of::<u64>()
        }
        #[cfg(feature = "prob")]
        senko_core::SenkoValue::TopK(sketch) => {
            sketch.buckets.len() * std::mem::size_of::<senko_core::HkCell>()
                + sketch
                    .item_counts
                    .iter()
                    .map(|(item, _)| item.len())
                    .sum::<usize>()
        }
        #[cfg(feature = "prob")]
        senko_core::SenkoValue::TDigest(digest) => {
            digest.centroids.len() * std::mem::size_of::<senko_core::Centroid>()
                + digest.unmerged.len() * std::mem::size_of::<(f64, f64)>()
        }
        senko_core::SenkoValue::Hash(hash) => {
            let base = std::mem::size_of_val(hash.as_ref());
            let fields = hash
                .fields
                .iter()
                .map(|(field, value)| field.len() + value.value.as_bytes().len())
                .sum::<usize>();
            base + fields
        }
        senko_core::SenkoValue::List(list) => {
            let nodes = list.iter().map(|value| value.len()).sum::<usize>();
            let headers =
                list.node_count as usize * std::mem::size_of::<senko_core::ListpackNode>();
            std::mem::size_of_val(list.as_ref()) + nodes + headers
        }
        senko_core::SenkoValue::Set(set) => {
            let members = set.iter().map(|value| value.len()).sum::<usize>();
            std::mem::size_of_val(set.as_ref()) + members
        }
        senko_core::SenkoValue::Stream(stream) => {
            std::mem::size_of_val(stream.as_ref())
                + stream
                    .tree
                    .range(
                        senko_core::StreamId::ZERO,
                        senko_core::StreamId::MAX,
                        None,
                    )
                    .map(|(_, fields)| {
                        fields
                            .into_iter()
                            .map(|(field, value)| field.len() + value.len())
                            .sum::<usize>()
                    })
                    .sum::<usize>()
        }
        senko_core::SenkoValue::ZSet(zset) => {
            let members = zset
                .range_by_rank(0, zset.len() as i64 - 1, false, None)
                .map(|(score, member)| std::mem::size_of_val(&score) + member.len())
                .sum::<usize>();
            std::mem::size_of_val(zset.as_ref()) + members
        }
    }
}
