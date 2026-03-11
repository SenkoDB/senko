#![deny(unsafe_code)]

use std::{
    cmp::Ordering,
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
};

use bytes::Bytes;
use parking_lot::RwLock;
use rayon::{ThreadPool, ThreadPoolBuilder};
use senko_core::{
    CommandRegistry, ModuleCommandContext, ModuleError, ModuleResponse, ModuleResult, Quant,
    QuantizedVec, SenkoModule, SenkoValue, ShardState, VectorNode, VectorSet,
};
use serde_json::Value as JsonValue;
use smallvec::smallvec;

#[derive(thiserror::Error, Debug)]
pub enum VectorError {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("ERR Vector set is empty")]
    EmptySet,
    #[error("ERR Element not found")]
    NotFound,
    #[error("ERR Input dimension mismatch: expected {expected}, got {got}")]
    DimMismatch { expected: usize, got: usize },
    #[error(
        "ERR Input dimension mismatch for projection - got {got} but projection expects {expected}"
    )]
    ProjDimMismatch { expected: usize, got: usize },
    #[error("ERR Quantization type mismatch")]
    QuantMismatch,
    #[error("ERR Invalid FP32 blob length")]
    BadFp32Blob,
    #[error("ERR Invalid VALUES count")]
    BadValuesCount,
    #[error("ERR Invalid filter expression")]
    BadFilter,
    #[error("ERR Attributes must be a valid JSON object")]
    BadAttrs,
    #[error("ERR REDUCE dimension must be less than input dimension")]
    BadReduceDim,
}

pub struct VectorModule {
    engine: Arc<VectorEngine>,
}

pub struct VectorEngine {
    pool: ThreadPool,
    next_uid: AtomicU64,
}

impl VectorModule {
    pub fn new() -> Self {
        let pool = ThreadPoolBuilder::new()
            .num_threads(32)
            .thread_name(|index| format!("senko-vsim-{index}"))
            .build()
            .expect("vector thread pool");
        Self {
            engine: Arc::new(VectorEngine {
                pool,
                next_uid: AtomicU64::new(1),
            }),
        }
    }
}

impl Default for VectorModule {
    fn default() -> Self {
        Self::new()
    }
}

impl SenkoModule for VectorModule {
    fn name(&self) -> &'static str {
        "vector_set"
    }

    fn version(&self) -> u64 {
        80_000
    }

    fn register_commands(&self, registry: &mut CommandRegistry) {
        registry.register("VADD", vadd);
        registry.register("VREM", vrem);
        registry.register("VSIM", vsim);
        registry.register("VEMB", vemb);
        registry.register("VCARD", vcard);
        registry.register("VDIM", vdim);
        registry.register("VISMEMBER", vismember);
        registry.register("VGETATTR", vgetattr);
        registry.register("VSETATTR", vsetattr);
        registry.register("VLINKS", vlinks);
        registry.register("VRANDMEMBER", vrandmember);
        registry.register("VRANGE", vrange);
        registry.register("VINFO", vinfo);
    }

    fn init_shard(&self, shard: &mut ShardState) {
        shard.set_extension(Arc::clone(&self.engine));
    }
}

fn vadd(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 {
        return Err(err("ERR wrong number of arguments for 'vadd' command"));
    }
    let engine = engine(ctx)?;
    let key = args[0];
    let mut index = 1usize;
    let mut reduce = None;
    if eq_ascii(args[index], b"REDUCE") {
        index += 1;
        reduce = Some(parse_usize(args.get(index).ok_or_else(|| {
            err("ERR REDUCE dimension must be less than input dimension")
        })?)?);
        index += 1;
    }
    let (input, consumed) = parse_input_vector(&args[index..])?;
    index += consumed;
    let element = Bytes::copy_from_slice(
        args.get(index)
            .ok_or_else(|| err("ERR Element not found"))?,
    );
    index += 1;

    let mut quant = Quant::Q8;
    let mut attrs = None;
    let mut m = 16usize;
    while index < args.len() {
        if eq_ascii(args[index], b"CAS") || eq_ascii(args[index], b"NOTHREAD") {
            index += 1;
        } else if eq_ascii(args[index], b"NOQUANT") {
            quant = Quant::None;
            index += 1;
        } else if eq_ascii(args[index], b"Q8") {
            quant = Quant::Q8;
            index += 1;
        } else if eq_ascii(args[index], b"BIN") {
            quant = Quant::Bin;
            index += 1;
        } else if eq_ascii(args[index], b"SETATTR") {
            index += 1;
            attrs = Some(parse_attrs(args.get(index).ok_or(VectorError::BadAttrs)?)?);
            index += 1;
        } else if eq_ascii(args[index], b"M") {
            index += 1;
            m = parse_usize(
                args.get(index)
                    .ok_or_else(|| err("ERR wrong number of arguments for 'vadd' command"))?,
            )?;
            index += 1;
        } else if eq_ascii(args[index], b"EF") {
            index += 2;
        } else {
            return Err(err(format!(
                "ERR syntax error near '{}'",
                String::from_utf8_lossy(args[index])
            )));
        }
    }

    let existing = ctx.get_value(key);
    let vset = match existing {
        Some(SenkoValue::VectorSet(vset)) => {
            let guard = vset.read();
            if guard.quant != quant {
                return Err(VectorError::QuantMismatch.into());
            }
            if input.len() != guard.input_dim {
                return Err(VectorError::DimMismatch {
                    expected: guard.input_dim,
                    got: input.len(),
                }
                .into());
            }
            drop(guard);
            vset
        }
        Some(_) => return Err(VectorError::WrongType.into()),
        None => {
            let logical_dim = reduce.unwrap_or(input.len());
            if reduce.is_some_and(|dim| dim >= input.len()) {
                return Err(VectorError::BadReduceDim.into());
            }
            let mut set = VectorSet::new(
                engine.next_uid.fetch_add(1, AtomicOrdering::Relaxed),
                logical_dim,
                input.len(),
                quant,
                m,
            );
            if let Some(output_dim) = reduce {
                set.projection = Some(make_projection(input.len(), output_dim));
            }
            let shared = Arc::new(RwLock::new(set));
            ctx.set_value(key, SenkoValue::VectorSet(Arc::clone(&shared)));
            shared
        }
    };

    let mut guard = vset.write();
    let projected = project_if_needed(&guard, &input)?;
    let normalized = normalize_vector(projected);
    let quantized = quantize(&normalized, guard.quant);
    let added = if let Some(&node_index) = guard.element_map.get(&element) {
        let node = &mut guard.nodes[node_index];
        node.vector = quantized;
        node.attrs = attrs;
        0
    } else {
        guard.max_uid = guard.max_uid.saturating_add(1);
        let uid = guard.max_uid;
        let index = guard.nodes.len();
        guard.nodes.push(VectorNode {
            vector: quantized,
            element: element.clone(),
            attrs,
            uid,
        });
        guard.element_map.insert(element, index);
        1
    };
    Ok(ModuleResponse::Integer(added))
}

fn vrem(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 2 {
        return Err(err("ERR wrong number of arguments for 'vrem' command"));
    }
    let vset = vector_set(ctx, args[0])?;
    let mut guard = vset.write();
    let Some(index) = guard.element_map.remove(args[1]) else {
        return Ok(ModuleResponse::Integer(0));
    };
    guard.nodes.swap_remove(index);
    if let Some(node) = guard.nodes.get(index) {
        let element = node.element.clone();
        guard.element_map.insert(element, index);
    }
    Ok(ModuleResponse::Integer(1))
}

fn vsim(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 {
        return Err(err("ERR wrong number of arguments for 'vsim' command"));
    }
    let engine = engine(ctx)?;
    let vset = vector_set(ctx, args[0])?;
    let mut index = 1usize;
    let query = if eq_ascii(args[index], b"ELE") {
        index += 1;
        Query::Element(Bytes::copy_from_slice(
            args.get(index).ok_or(VectorError::NotFound)?,
        ))
    } else {
        let (input, consumed) = parse_input_vector(&args[index..])?;
        index += consumed - 1;
        Query::Vector(input)
    };
    index += 1;

    let mut with_scores = false;
    let mut with_attribs = false;
    let mut count = 10usize;
    let mut epsilon = None;
    let mut filter = None;
    let mut truth = false;
    let mut nothread = false;
    while index < args.len() {
        if eq_ascii(args[index], b"WITHSCORES") {
            with_scores = true;
            index += 1;
        } else if eq_ascii(args[index], b"WITHATTRIBS") {
            with_attribs = true;
            index += 1;
        } else if eq_ascii(args[index], b"COUNT") {
            index += 1;
            count = parse_usize(args.get(index).ok_or_else(|| err("ERR syntax error"))?)?;
            index += 1;
        } else if eq_ascii(args[index], b"EPSILON") {
            index += 1;
            epsilon = Some(parse_f32(
                args.get(index).ok_or_else(|| err("ERR syntax error"))?,
            )?);
            index += 1;
        } else if eq_ascii(args[index], b"EF") || eq_ascii(args[index], b"FILTER-EF") {
            index += 2;
        } else if eq_ascii(args[index], b"FILTER") {
            index += 1;
            filter = Some(parse_filter(
                args.get(index).ok_or(VectorError::BadFilter)?,
            )?);
            index += 1;
        } else if eq_ascii(args[index], b"TRUTH") {
            truth = true;
            index += 1;
        } else if eq_ascii(args[index], b"NOTHREAD") {
            nothread = true;
            index += 1;
        } else {
            return Err(err("ERR syntax error"));
        }
    }

    let search = || -> Result<Vec<SearchResult>, ModuleError> {
        let guard = vset.read();
        if guard.nodes.is_empty() {
            return Ok(Vec::new());
        }
        let query_vec = match &query {
            Query::Element(element) => {
                let Some(&idx) = guard.element_map.get(element.as_ref()) else {
                    return Err(VectorError::NotFound.into());
                };
                dequantize(&guard.nodes[idx].vector)
            }
            Query::Vector(values) => normalize_vector(project_if_needed(&guard, values)?),
        };
        let query_quant = quantize(&query_vec, guard.quant);
        let mut rows = guard
            .nodes
            .iter()
            .filter_map(|node| {
                if let Some(expr) = &filter
                    && !attrs_match(node.attrs.as_ref(), expr)
                {
                    return None;
                }
                let score = similarity(guard.quant, &query_quant, &node.vector);
                if epsilon.is_some_and(|epsilon| score < epsilon) && guard.quant != Quant::Bin {
                    return None;
                }
                Some(SearchResult {
                    element: node.element.clone(),
                    score,
                    attrs: node.attrs.clone(),
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
        });
        rows.truncate(count);
        let _ = truth;
        Ok(rows)
    };

    let rows = if nothread {
        search()?
    } else {
        engine.pool.install(search)?
    };
    Ok(ModuleResponse::Array(Box::new(
        rows.into_iter()
            .map(|row| {
                if with_scores || with_attribs {
                    let mut parts = smallvec![bulk(row.element.as_ref())];
                    if with_scores {
                        parts.push(bulk(format_score(row.score)));
                    }
                    if with_attribs {
                        parts.push(ModuleResponse::Bulk(row.attrs));
                    }
                    ModuleResponse::Array(Box::new(parts))
                } else {
                    bulk(row.element.as_ref())
                }
            })
            .collect(),
    )))
}

fn vemb(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 || args.len() > 3 {
        return Err(err("ERR wrong number of arguments for 'vemb' command"));
    }
    let raw = args.get(2).is_some_and(|value| eq_ascii(value, b"RAW"));
    let vset = vector_set(ctx, args[0])?;
    let guard = vset.read();
    let idx = *guard
        .element_map
        .get(args[1])
        .ok_or(VectorError::NotFound)?;
    let node = &guard.nodes[idx];
    if raw {
        return Ok(ModuleResponse::Bulk(Some(Bytes::from(raw_quantized(
            &node.vector,
        )))));
    }
    Ok(ModuleResponse::Array(Box::new(
        dequantize(&node.vector)
            .into_iter()
            .map(|value| bulk(format_score(value)))
            .collect(),
    )))
}

fn vcard(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 1 {
        return Err(err("ERR wrong number of arguments for 'vcard' command"));
    }
    let vset = vector_set(ctx, args[0])?;
    Ok(ModuleResponse::Integer(vset.read().nodes.len() as i64))
}

fn vdim(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 1 {
        return Err(err("ERR wrong number of arguments for 'vdim' command"));
    }
    let vset = vector_set(ctx, args[0])?;
    Ok(ModuleResponse::Integer(vset.read().dim as i64))
}

fn vismember(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 2 {
        return Err(err("ERR wrong number of arguments for 'vismember' command"));
    }
    let vset = vector_set(ctx, args[0])?;
    Ok(ModuleResponse::Integer(i64::from(
        vset.read().element_map.contains_key(args[1]),
    )))
}

fn vgetattr(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 2 {
        return Err(err("ERR wrong number of arguments for 'vgetattr' command"));
    }
    let vset = vector_set(ctx, args[0])?;
    let guard = vset.read();
    let idx = *guard
        .element_map
        .get(args[1])
        .ok_or(VectorError::NotFound)?;
    Ok(ModuleResponse::Bulk(guard.nodes[idx].attrs.clone()))
}

fn vsetattr(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 3 {
        return Err(err("ERR wrong number of arguments for 'vsetattr' command"));
    }
    let vset = vector_set(ctx, args[0])?;
    let mut guard = vset.write();
    let idx = *guard
        .element_map
        .get(args[1])
        .ok_or(VectorError::NotFound)?;
    guard.nodes[idx].attrs = parse_setattr(args[2])?;
    Ok(ModuleResponse::Simple(b"OK"))
}

fn vlinks(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 || args.len() > 3 {
        return Err(err("ERR wrong number of arguments for 'vlinks' command"));
    }
    let with_scores = args
        .get(2)
        .is_some_and(|value| eq_ascii(value, b"WITHSCORES"));
    let vset = vector_set(ctx, args[0])?;
    let guard = vset.read();
    let idx = *guard
        .element_map
        .get(args[1])
        .ok_or(VectorError::NotFound)?;
    let query = &guard.nodes[idx].vector;
    let mut neighbors = guard
        .nodes
        .iter()
        .filter(|node| node.element.as_ref() != args[1])
        .map(|node| {
            (
                node.element.clone(),
                similarity(guard.quant, query, &node.vector),
            )
        })
        .collect::<Vec<_>>();
    neighbors.sort_by(|left, right| right.1.partial_cmp(&left.1).unwrap_or(Ordering::Equal));
    neighbors.truncate(guard.m.min(neighbors.len()));
    let layer = ModuleResponse::Array(Box::new(
        neighbors
            .into_iter()
            .map(|(element, score)| {
                if with_scores {
                    ModuleResponse::Array(Box::new(smallvec![
                        bulk(element.as_ref()),
                        bulk(format_score(score)),
                    ]))
                } else {
                    bulk(element.as_ref())
                }
            })
            .collect(),
    ));
    Ok(ModuleResponse::Array(Box::new(smallvec![layer])))
}

fn vrandmember(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() || args.len() > 2 {
        return Err(err(
            "ERR wrong number of arguments for 'vrandmember' command",
        ));
    }
    let Some(vset) = maybe_vector_set(ctx, args[0])? else {
        return Ok(if args.len() == 1 {
            ModuleResponse::Bulk(None)
        } else {
            ModuleResponse::Array(Box::default())
        });
    };
    let guard = vset.read();
    if guard.nodes.is_empty() {
        return Ok(if args.len() == 1 {
            ModuleResponse::Bulk(None)
        } else {
            ModuleResponse::Array(Box::default())
        });
    }
    if args.len() == 1 {
        let index = fastrand::usize(..guard.nodes.len());
        return Ok(bulk(guard.nodes[index].element.as_ref()));
    }
    let count = parse_i64(args[1])?;
    let mut items = Vec::new();
    if count >= 0 {
        let mut seen = HashSet::new();
        let target = usize::min(count as usize, guard.nodes.len());
        while items.len() < target {
            let index = fastrand::usize(..guard.nodes.len());
            if seen.insert(index) {
                items.push(guard.nodes[index].element.clone());
            }
        }
    } else {
        for _ in 0..count.unsigned_abs() {
            let index = fastrand::usize(..guard.nodes.len());
            items.push(guard.nodes[index].element.clone());
        }
    }
    Ok(ModuleResponse::Array(Box::new(
        items
            .into_iter()
            .map(|value| bulk(value.as_ref()))
            .collect(),
    )))
}

fn vrange(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 || args.len() > 4 {
        return Err(err("ERR wrong number of arguments for 'vrange' command"));
    }
    let vset = vector_set(ctx, args[0])?;
    let count = args.get(3).map(|value| parse_usize(value)).transpose()?;
    let mut values = vset
        .read()
        .nodes
        .iter()
        .map(|node| node.element.clone())
        .collect::<Vec<_>>();
    values.sort();
    let start = args[1];
    let end = args[2];
    let mut rows = values
        .into_iter()
        .filter(|value| value.as_ref() >= start && value.as_ref() <= end)
        .collect::<Vec<_>>();
    if let Some(limit) = count {
        rows.truncate(limit);
    }
    Ok(ModuleResponse::Array(Box::new(
        rows.into_iter().map(|value| bulk(value.as_ref())).collect(),
    )))
}

fn vinfo(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 1 {
        return Err(err("ERR wrong number of arguments for 'vinfo' command"));
    }
    let vset = vector_set(ctx, args[0])?;
    let guard = vset.read();
    let attrs_count = guard
        .nodes
        .iter()
        .filter(|node| node.attrs.is_some())
        .count();
    Ok(ModuleResponse::Array(Box::new(smallvec![
        bulk(b"quant-type"),
        bulk(match guard.quant {
            Quant::None => b"noquant".as_slice(),
            Quant::Q8 => b"int8".as_slice(),
            Quant::Bin => b"bin".as_slice(),
        }),
        bulk(b"hnsw-m"),
        ModuleResponse::Integer(guard.m as i64),
        bulk(b"vector-dim"),
        ModuleResponse::Integer(guard.dim as i64),
        bulk(b"projection-input-dim"),
        ModuleResponse::Integer(guard.projection.as_ref().map(|p| p.input_dim).unwrap_or(0) as i64),
        bulk(b"size"),
        ModuleResponse::Integer(guard.nodes.len() as i64),
        bulk(b"max-level"),
        ModuleResponse::Integer(0),
        bulk(b"attributes-count"),
        ModuleResponse::Integer(attrs_count as i64),
        bulk(b"vset-uid"),
        ModuleResponse::Integer(guard.uid as i64),
        bulk(b"hnsw-max-node-uid"),
        ModuleResponse::Integer(guard.max_uid as i64),
    ])))
}

enum Query {
    Element(Bytes),
    Vector(Vec<f32>),
}

struct SearchResult {
    element: Bytes,
    score: f32,
    attrs: Option<Bytes>,
}

#[derive(Clone)]
enum FilterExpr {
    Eq(Vec<String>, FilterValue),
    Ne(Vec<String>, FilterValue),
    Lt(Vec<String>, f64),
    Gt(Vec<String>, f64),
    Le(Vec<String>, f64),
    Ge(Vec<String>, f64),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
}

#[derive(Clone)]
enum FilterValue {
    String(String),
    Number(f64),
    Null,
}

fn maybe_vector_set(
    ctx: &mut dyn ModuleCommandContext,
    key: &[u8],
) -> Result<Option<Arc<RwLock<VectorSet>>>, ModuleError> {
    match ctx.get_value(key) {
        Some(SenkoValue::VectorSet(vset)) => Ok(Some(vset)),
        Some(_) => Err(VectorError::WrongType.into()),
        None => Ok(None),
    }
}

fn vector_set(
    ctx: &mut dyn ModuleCommandContext,
    key: &[u8],
) -> Result<Arc<RwLock<VectorSet>>, ModuleError> {
    maybe_vector_set(ctx, key)?.ok_or_else(|| VectorError::EmptySet.into())
}

fn engine(ctx: &dyn ModuleCommandContext) -> Result<Arc<VectorEngine>, ModuleError> {
    ctx.shard_extensions()
        .get::<VectorEngine>()
        .ok_or_else(|| err("ERR vector engine is not initialized"))
}

fn parse_input_vector(args: &[&[u8]]) -> Result<(Vec<f32>, usize), ModuleError> {
    if args.is_empty() {
        return Err(err("ERR syntax error"));
    }
    if eq_ascii(args[0], b"FP32") {
        let blob = *args.get(1).ok_or(VectorError::BadFp32Blob)?;
        if blob.len() % 4 != 0 {
            return Err(VectorError::BadFp32Blob.into());
        }
        let mut out = Vec::with_capacity(blob.len() / 4);
        for chunk in blob.chunks_exact(4) {
            out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        return Ok((out, 2));
    }
    if eq_ascii(args[0], b"VALUES") {
        let count = parse_usize(args.get(1).ok_or(VectorError::BadValuesCount)?)?;
        if args.len() < count + 2 {
            return Err(VectorError::BadValuesCount.into());
        }
        let mut out = Vec::with_capacity(count);
        for raw in &args[2..2 + count] {
            out.push(parse_f32(raw)?);
        }
        return Ok((out, count + 2));
    }
    Err(err("ERR syntax error"))
}

fn parse_attrs(raw: &[u8]) -> Result<Bytes, ModuleError> {
    let value: JsonValue = serde_json::from_slice(raw).map_err(|_| VectorError::BadAttrs)?;
    if !value.is_object() {
        return Err(VectorError::BadAttrs.into());
    }
    Ok(Bytes::copy_from_slice(raw))
}

fn parse_setattr(raw: &[u8]) -> Result<Option<Bytes>, ModuleError> {
    if raw.is_empty() || eq_ascii(raw, b"null") {
        return Ok(None);
    }
    Ok(Some(parse_attrs(raw)?))
}

fn make_projection(input_dim: usize, output_dim: usize) -> senko_core::ProjectionMatrix {
    let scale = (output_dim as f32).sqrt();
    let mut matrix = Vec::with_capacity(input_dim * output_dim);
    for _ in 0..input_dim * output_dim {
        let u1 = fastrand::f32().max(f32::MIN_POSITIVE);
        let u2 = fastrand::f32();
        let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos();
        matrix.push(z / scale);
    }
    senko_core::ProjectionMatrix {
        input_dim,
        output_dim,
        matrix,
    }
}

fn project_if_needed(vset: &VectorSet, input: &[f32]) -> Result<Vec<f32>, ModuleError> {
    if let Some(proj) = &vset.projection {
        if input.len() != proj.input_dim {
            return Err(VectorError::ProjDimMismatch {
                expected: proj.input_dim,
                got: input.len(),
            }
            .into());
        }
        let mut output = vec![0.0_f32; proj.output_dim];
        for (row, input_value) in input.iter().enumerate() {
            for (col, out) in output.iter_mut().enumerate().take(proj.output_dim) {
                *out += input_value * proj.matrix[row * proj.output_dim + col];
            }
        }
        Ok(output)
    } else {
        if input.len() != vset.dim {
            return Err(VectorError::DimMismatch {
                expected: vset.dim,
                got: input.len(),
            }
            .into());
        }
        Ok(input.to_vec())
    }
}

fn normalize_vector(mut input: Vec<f32>) -> Vec<f32> {
    let norm = input.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut input {
            *value /= norm;
        }
    }
    input
}

fn quantize(input: &[f32], quant: Quant) -> QuantizedVec {
    match quant {
        Quant::None => QuantizedVec::None(input.to_vec()),
        Quant::Q8 => {
            let min = input.iter().copied().fold(f32::INFINITY, f32::min);
            let max = input.iter().copied().fold(f32::NEG_INFINITY, f32::max);
            let range = (max - min).max(f32::EPSILON);
            let bytes = input
                .iter()
                .map(|value| (((value - min) / range) * 127.0).round() as i8)
                .collect();
            QuantizedVec::Q8 {
                dim: input.len(),
                min,
                range,
                bytes,
            }
        }
        Quant::Bin => {
            let mut bytes = vec![0_u8; input.len().div_ceil(8)];
            for (index, value) in input.iter().enumerate() {
                if *value >= 0.0 {
                    bytes[index / 8] |= 1 << (index % 8);
                }
            }
            QuantizedVec::Bin {
                dim: input.len(),
                bytes,
            }
        }
    }
}

fn dequantize(vector: &QuantizedVec) -> Vec<f32> {
    match vector {
        QuantizedVec::None(values) => values.clone(),
        QuantizedVec::Q8 {
            min, range, bytes, ..
        } => bytes
            .iter()
            .map(|value| (f32::from(*value) / 127.0) * *range + *min)
            .collect(),
        QuantizedVec::Bin { dim, bytes } => (0..*dim)
            .map(|index| {
                if bytes[index / 8] & (1 << (index % 8)) != 0 {
                    1.0
                } else {
                    -1.0
                }
            })
            .collect(),
    }
}

fn raw_quantized(vector: &QuantizedVec) -> Vec<u8> {
    match vector {
        QuantizedVec::None(values) => values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect(),
        QuantizedVec::Q8 {
            min, range, bytes, ..
        } => {
            let mut out = Vec::with_capacity(8 + bytes.len());
            out.extend_from_slice(&min.to_le_bytes());
            out.extend_from_slice(&range.to_le_bytes());
            out.extend(bytes.iter().map(|value| *value as u8));
            out
        }
        QuantizedVec::Bin { bytes, .. } => bytes.clone(),
    }
}

fn similarity(quant: Quant, left: &QuantizedVec, right: &QuantizedVec) -> f32 {
    match quant {
        Quant::None => cosine(&dequantize(left), &dequantize(right)),
        Quant::Q8 => q8_cosine(left, right),
        Quant::Bin => bin_similarity(left, right),
    }
}

fn cosine(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(a, b)| a * b).sum()
}

fn q8_cosine(left: &QuantizedVec, right: &QuantizedVec) -> f32 {
    let (
        QuantizedVec::Q8 {
            bytes: left_bytes, ..
        },
        QuantizedVec::Q8 {
            bytes: right_bytes, ..
        },
    ) = (left, right)
    else {
        return cosine(&dequantize(left), &dequantize(right));
    };
    let dot = left_bytes
        .iter()
        .zip(right_bytes)
        .map(|(a, b)| i32::from(*a) * i32::from(*b))
        .sum::<i32>();
    dot as f32 / (127.0 * 127.0 * left_bytes.len() as f32).max(1.0)
}

fn bin_similarity(left: &QuantizedVec, right: &QuantizedVec) -> f32 {
    let (
        QuantizedVec::Bin {
            dim,
            bytes: left_bytes,
        },
        QuantizedVec::Bin {
            bytes: right_bytes, ..
        },
    ) = (left, right)
    else {
        return cosine(&dequantize(left), &dequantize(right));
    };
    let distance = left_bytes
        .iter()
        .zip(right_bytes)
        .map(|(a, b)| (a ^ b).count_ones())
        .sum::<u32>() as f32;
    1.0 - distance / *dim as f32
}

fn parse_filter(raw: &[u8]) -> Result<FilterExpr, ModuleError> {
    let text = std::str::from_utf8(raw).map_err(|_| VectorError::BadFilter)?;
    parse_filter_expr(text.trim())
}

fn parse_filter_expr(text: &str) -> Result<FilterExpr, ModuleError> {
    if let Some(index) = text.find("||") {
        return Ok(FilterExpr::Or(
            Box::new(parse_filter_expr(text[..index].trim())?),
            Box::new(parse_filter_expr(text[index + 2..].trim())?),
        ));
    }
    if let Some(index) = text.find("&&") {
        return Ok(FilterExpr::And(
            Box::new(parse_filter_expr(text[..index].trim())?),
            Box::new(parse_filter_expr(text[index + 2..].trim())?),
        ));
    }
    if let Some(rest) = text.strip_prefix('!') {
        return Ok(FilterExpr::Not(Box::new(parse_filter_expr(rest.trim())?)));
    }
    for op in ["==", "!=", "<=", ">=", "<", ">"] {
        if let Some(index) = text.find(op) {
            let field = parse_field_path(text[..index].trim())?;
            let value = text[index + op.len()..].trim();
            return match op {
                "==" => Ok(FilterExpr::Eq(field, parse_filter_value(value)?)),
                "!=" => Ok(FilterExpr::Ne(field, parse_filter_value(value)?)),
                "<" => Ok(FilterExpr::Lt(field, parse_numeric(value)?)),
                ">" => Ok(FilterExpr::Gt(field, parse_numeric(value)?)),
                "<=" => Ok(FilterExpr::Le(field, parse_numeric(value)?)),
                ">=" => Ok(FilterExpr::Ge(field, parse_numeric(value)?)),
                _ => Err(VectorError::BadFilter.into()),
            };
        }
    }
    Err(VectorError::BadFilter.into())
}

fn parse_field_path(text: &str) -> Result<Vec<String>, ModuleError> {
    let text = text.strip_prefix('.').ok_or(VectorError::BadFilter)?;
    Ok(text.split('.').map(ToString::to_string).collect())
}

fn parse_filter_value(text: &str) -> Result<FilterValue, ModuleError> {
    if text == "null" {
        Ok(FilterValue::Null)
    } else if text.starts_with('"') && text.ends_with('"') && text.len() >= 2 {
        Ok(FilterValue::String(text[1..text.len() - 1].to_string()))
    } else {
        Ok(FilterValue::Number(parse_numeric(text)?))
    }
}

fn parse_numeric(text: &str) -> Result<f64, ModuleError> {
    text.parse::<f64>()
        .map_err(|_| VectorError::BadFilter.into())
}

fn attrs_match(raw: Option<&Bytes>, filter: &FilterExpr) -> bool {
    let Some(raw) = raw else {
        return false;
    };
    let Ok(json) = serde_json::from_slice::<JsonValue>(raw) else {
        return false;
    };
    eval_filter(&json, filter)
}

fn eval_filter(json: &JsonValue, filter: &FilterExpr) -> bool {
    match filter {
        FilterExpr::Eq(path, value) => {
            json_at(json, path).is_some_and(|current| match (current, value) {
                (JsonValue::String(current), FilterValue::String(value)) => current == value,
                (JsonValue::Number(current), FilterValue::Number(value)) => {
                    current.as_f64() == Some(*value)
                }
                (JsonValue::Null, FilterValue::Null) => true,
                _ => false,
            })
        }
        FilterExpr::Ne(path, value) => {
            !eval_filter(json, &FilterExpr::Eq(path.clone(), value.clone()))
        }
        FilterExpr::Lt(path, value) => json_at(json, path)
            .and_then(JsonValue::as_f64)
            .is_some_and(|current| current < *value),
        FilterExpr::Gt(path, value) => json_at(json, path)
            .and_then(JsonValue::as_f64)
            .is_some_and(|current| current > *value),
        FilterExpr::Le(path, value) => json_at(json, path)
            .and_then(JsonValue::as_f64)
            .is_some_and(|current| current <= *value),
        FilterExpr::Ge(path, value) => json_at(json, path)
            .and_then(JsonValue::as_f64)
            .is_some_and(|current| current >= *value),
        FilterExpr::And(left, right) => eval_filter(json, left) && eval_filter(json, right),
        FilterExpr::Or(left, right) => eval_filter(json, left) || eval_filter(json, right),
        FilterExpr::Not(inner) => !eval_filter(json, inner),
    }
}

fn json_at<'a>(json: &'a JsonValue, path: &[String]) -> Option<&'a JsonValue> {
    let mut current = json;
    for segment in path {
        current = current.get(segment)?;
    }
    Some(current)
}

fn parse_usize(raw: &[u8]) -> Result<usize, ModuleError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| err("ERR invalid integer"))
}

fn parse_i64(raw: &[u8]) -> Result<i64, ModuleError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| err("ERR invalid integer"))
}

fn parse_f32(raw: &[u8]) -> Result<f32, ModuleError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<f32>().ok())
        .ok_or(VectorError::BadValuesCount.into())
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn bulk(value: impl AsRef<[u8]>) -> ModuleResponse {
    ModuleResponse::Bulk(Some(Bytes::copy_from_slice(value.as_ref())))
}

fn format_score(value: f32) -> Vec<u8> {
    let mut text = value.to_string();
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        text.push_str(".0");
    }
    text.into_bytes()
}

fn err(message: impl Into<String>) -> ModuleError {
    ModuleError::new(message.into())
}

impl From<VectorError> for ModuleError {
    fn from(value: VectorError) -> Self {
        ModuleError::new(value.to_string())
    }
}
