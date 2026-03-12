#![deny(unsafe_code)]

pub mod config;
pub mod error;
pub mod hash;
#[allow(unsafe_code)]
pub mod list;
pub mod module;
#[cfg(feature = "prob")]
pub mod prob;
#[allow(unsafe_code)]
pub mod set;
pub mod stream;
pub mod value;
#[cfg(feature = "vector")]
pub mod vector;
pub mod zset;

pub use config::{
    AppendFsync, ByteSize, ConfigError, ReplicaOf, SenkoConfig, config_get, config_set,
    config_set_startup, human_duration, load_config, parse_duration_seconds, parse_replica_of,
    render_default_config_toml, validate_config,
};
pub use error::{SenkoError, SenkoResult};
pub use hash::{HashField, HashObject};
pub use list::{
    InsertResult, ListpackIter, ListpackNode, QuickList, QuickListRangeIter, lp_byte_size,
    lp_delete_at, lp_find, lp_get, lp_insert_after, lp_insert_before, lp_iter, lp_len, lp_pop_back,
    lp_pop_front, lp_push_back, lp_push_front, lp_set,
};
pub use module::{
    CommandRegistry, ModuleCommandContext, ModuleDescriptor, ModuleError, ModuleRegistry,
    ModuleResponse, ModuleResult, SenkoModule, ShardExtensions, ShardState,
};
#[cfg(feature = "prob")]
pub use prob::{
    BitVec, BloomFilter, Bucket, Centroid, CountMinSketch, CuckooFilter, CuckooLayer, DoubleHasher,
    HkCell, ProbMergeValue, SubFilter, TDigest, TopKSketch, optimal_bits, optimal_hashes,
    xxhash3_128,
};
pub use set::{
    IntSet, IntSetEncoding, SetEncoding, SetIter, SetObject, lp_set_contains, lp_set_insert,
    lp_set_remove,
};
pub use stream::{
    ConsumerGroup, ConsumerState, ListpackMacroNode, MacroNodeIter, PelEntry, RadixNode,
    StreamBorrowedEntry, StreamFieldPairBorrowed, StreamFieldPairOwned, StreamId, StreamObject,
    StreamOwnedEntry, StreamRadixTree, StreamRangeIter, StreamRefMode,
};
pub use value::{FeroxValue, SenkoValue};
#[cfg(feature = "vector")]
pub use vector::{ProjectionMatrix, Quant, QuantizedVec, SharedVectorSet, VectorNode, VectorSet};
pub use zset::{
    BPTree, BPTreeRangeIter, InsertResult as ZSetInsertResult, LexBound, ScoreBound, ZAddCond,
    ZAddOptions, ZAddResult, ZSetEncoding, ZSetObject, ZSetRangeIter,
};

use ahash::RandomState;
use compact_str::CompactString;
use hashbrown::{HashMap, HashSet};

pub type SenkoKey = CompactString;
pub type SenkoMap<K, V> = HashMap<K, V, RandomState>;
pub type SenkoSet<T> = HashSet<T, RandomState>;

pub fn senko_hasher() -> RandomState {
    RandomState::new()
}
