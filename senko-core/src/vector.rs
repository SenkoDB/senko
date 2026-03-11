use std::sync::Arc;

use ahash::AHashMap;
use bytes::Bytes;
use parking_lot::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quant {
    None,
    Q8,
    Bin,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionMatrix {
    pub input_dim: usize,
    pub output_dim: usize,
    pub matrix: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QuantizedVec {
    None(Vec<f32>),
    Q8 {
        dim: usize,
        min: f32,
        range: f32,
        bytes: Vec<i8>,
    },
    Bin {
        dim: usize,
        bytes: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorNode {
    pub vector: QuantizedVec,
    pub element: Bytes,
    pub attrs: Option<Bytes>,
    pub uid: u32,
}

#[derive(Debug, Clone)]
pub struct VectorSet {
    pub uid: u64,
    pub m: usize,
    pub dim: usize,
    pub input_dim: usize,
    pub quant: Quant,
    pub projection: Option<ProjectionMatrix>,
    pub nodes: Vec<VectorNode>,
    pub element_map: AHashMap<Bytes, usize>,
    pub max_uid: u32,
}

impl VectorSet {
    pub fn new(uid: u64, dim: usize, input_dim: usize, quant: Quant, m: usize) -> Self {
        Self {
            uid,
            m,
            dim,
            input_dim,
            quant,
            projection: None,
            nodes: Vec::new(),
            element_map: AHashMap::new(),
            max_uid: 0,
        }
    }
}

pub type SharedVectorSet = Arc<RwLock<VectorSet>>;
