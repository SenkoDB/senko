use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::Bytes;
use crossbeam_channel::{bounded, Receiver, Sender};
use dashmap::DashMap;
use parking_lot::Mutex;
use senko_core::{CommandRegistry, SenkoModule, ShardState};
use tantivy::{Index, IndexReader, IndexWriter};
use thiserror::Error;

pub const REDISEARCH_VERSION: u64 = 20_800;

pub struct SearchModule {
    engine: Arc<SearchEngine>,
}

impl SearchModule {
    pub fn new() -> Self {
        Self {
            engine: Arc::new(SearchEngine::new()),
        }
    }

    pub fn engine(&self) -> &Arc<SearchEngine> {
        &self.engine
    }
}

impl Default for SearchModule {
    fn default() -> Self {
        Self::new()
    }
}

impl SenkoModule for SearchModule {
    fn name(&self) -> &'static str {
        "search"
    }

    fn version(&self) -> u64 {
        REDISEARCH_VERSION
    }

    fn register_commands(&self, _registry: &mut CommandRegistry) {}

    fn init_shard(&self, shard: &mut ShardState) {
        shard.set_extension(Arc::clone(&self.engine));
    }
}

pub struct SearchEngine {
    pub indexes: DashMap<Arc<str>, IndexState>,
    pub aliases: DashMap<Arc<str>, Arc<str>>,
    pub dicts: DashMap<Arc<str>, HashSet<String>>,
    pub synonyms: DashMap<Arc<str>, SynonymMap>,
    tx: Sender<SearchTask>,
    rx: Receiver<SearchTask>,
}

impl SearchEngine {
    pub fn new() -> Self {
        let (tx, rx) = bounded(1024);
        Self {
            indexes: DashMap::new(),
            aliases: DashMap::new(),
            dicts: DashMap::new(),
            synonyms: DashMap::new(),
            tx,
            rx,
        }
    }

    pub fn sender(&self) -> Sender<SearchTask> {
        self.tx.clone()
    }

    pub fn try_recv(&self) -> Result<SearchTask, crossbeam_channel::TryRecvError> {
        self.rx.try_recv()
    }
}

impl Default for SearchEngine {
    fn default() -> Self {
        Self::new()
    }
}

pub struct IndexState {
    pub spec: IndexSpec,
    pub tantivy_idx: Index,
    pub writer: Mutex<IndexWriter>,
    pub reader: IndexReader,
    pub cursors: DashMap<u64, Cursor>,
    pub doc_count: AtomicU64,
}

impl IndexState {
    pub fn doc_count(&self) -> u64 {
        self.doc_count.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cursor {
    pub results: Vec<SearchDocument>,
    pub max_idle: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchDocument {
    pub key: Arc<str>,
    pub fields: Vec<(Arc<str>, Bytes)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SynonymMap {
    pub groups: Vec<Vec<Arc<str>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexSpec {
    pub on: IndexOn,
    pub prefixes: Vec<String>,
    pub filter: Option<String>,
    pub default_lang: Language,
    pub default_score: f64,
    pub flags: IndexFlags,
    pub temporary_ttl: Option<Duration>,
    pub fields: Vec<(String, FieldSpec)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOn {
    Hash,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    English,
    Chinese,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IndexFlags {
    pub no_offsets: bool,
    pub no_highlight: bool,
    pub no_fields: bool,
    pub no_freqs: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldSpec {
    Text {
        weight: f64,
        sortable: bool,
        no_index: bool,
        with_suffix_trie: bool,
        phonetic: Option<PhoneticMatcher>,
    },
    Tag {
        separator: char,
        sortable: bool,
        no_index: bool,
        case_sensitive: bool,
    },
    Numeric {
        sortable: bool,
        no_index: bool,
    },
    Geo,
    Vector {
        algorithm: VectorAlgo,
        dims: usize,
        distance_metric: DistanceMetric,
        capacity: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhoneticMatcher {
    DmEn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorAlgo {
    Hnsw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    L2,
    Cosine,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchTask {
    RefreshIndex(Arc<str>),
    DropIndex(Arc<str>),
}

#[derive(Error, Debug)]
pub enum SearchError {
    #[error("Unknown index name")]
    UnknownIndex,
    #[error("ERR Index already exists")]
    IndexExists,
    #[error("ERR Syntax error at offset {0}")]
    QuerySyntax(usize),
    #[error("ERR No such cursor")]
    NoCursor,
    #[error("ERR {0}")]
    Schema(String),
    #[error("ERR Unsupported dialect {0}")]
    UnsupportedDialect(u8),
    #[error("ERR Vector dimension mismatch: expected {expected}, got {got}")]
    VectorDim { expected: usize, got: usize },
}
