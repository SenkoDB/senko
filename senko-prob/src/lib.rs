#![deny(unsafe_code)]

use bytes::Bytes;
use senko_core::{
    BloomFilter, CommandRegistry, CountMinSketch, CuckooFilter, ModuleCommandContext, ModuleError,
    ModuleResponse, ModuleResult, ProbMergeValue, SenkoModule, SenkoValue, ShardState, TDigest,
    TopKSketch,
};
use smallvec::SmallVec;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProbError {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("ERR item exists")]
    KeyExists,
    #[error("ERR not found")]
    KeyNotFound,
    #[error("ERR filter is full")]
    FilterFull,
    #[error("ERR max iterations reached - cuckoo filter is full")]
    CuckooFull,
    #[error("ERR wrong number of arguments")]
    WrongArity,
    #[error("ERR error rate must be between 0 and 1")]
    BadErrorRate,
    #[error("ERR capacity must be a positive integer")]
    BadCapacity,
    #[error("ERR sketches must have identical width and depth")]
    SketchDimMismatch,
    #[error("ERR compression must be between 1 and 1000")]
    BadCompression,
    #[error("ERR quantile must be between 0 and 1")]
    BadQuantile,
    #[error("ERR key is empty")]
    EmptyKey,
    #[error("ERR NOTCREATED: NOCREATE specified and key does not exist")]
    NoCreate,
    #[error("ERR syntax error")]
    Syntax,
}

impl From<ProbError> for ModuleError {
    fn from(value: ProbError) -> Self {
        ModuleError::new(value.to_string())
    }
}

pub struct ProbModule;

impl SenkoModule for ProbModule {
    fn name(&self) -> &'static str {
        "bf"
    }

    fn version(&self) -> u64 {
        20802
    }

    fn register_commands(&self, registry: &mut CommandRegistry) {
        registry.register("BF.RESERVE", bf_reserve);
        registry.register("BF.ADD", bf_add);
        registry.register("BF.MADD", bf_madd);
        registry.register("BF.INSERT", bf_insert);
        registry.register("BF.EXISTS", bf_exists);
        registry.register("BF.MEXISTS", bf_mexists);
        registry.register("BF.CARD", bf_card);
        registry.register("BF.INFO", bf_info);
        registry.register("BF.SCANDUMP", bf_scandump);
        registry.register("BF.LOADCHUNK", bf_loadchunk);

        registry.register("CF.RESERVE", cf_reserve);
        registry.register("CF.ADD", cf_add);
        registry.register("CF.ADDNX", cf_addnx);
        registry.register("CF.INSERT", cf_insert);
        registry.register("CF.INSERTNX", cf_insertnx);
        registry.register("CF.EXISTS", cf_exists);
        registry.register("CF.MEXISTS", cf_mexists);
        registry.register("CF.DEL", cf_del);
        registry.register("CF.COUNT", cf_count);
        registry.register("CF.INFO", cf_info);
        registry.register("CF.SCANDUMP", cf_scandump);
        registry.register("CF.LOADCHUNK", cf_loadchunk);

        registry.register("CMS.INITBYDIM", cms_initbydim);
        registry.register("CMS.INITBYPROB", cms_initbyprob);
        registry.register("CMS.INCRBY", cms_incrby);
        registry.register("CMS.QUERY", cms_query);
        registry.register("CMS.MERGE", cms_merge);
        registry.register("CMS.INFO", cms_info);

        registry.register("TOPK.RESERVE", topk_reserve);
        registry.register("TOPK.ADD", topk_add);
        registry.register("TOPK.INCRBY", topk_incrby);
        registry.register("TOPK.QUERY", topk_query);
        registry.register("TOPK.COUNT", topk_count);
        registry.register("TOPK.LIST", topk_list);
        registry.register("TOPK.INFO", topk_info);

        registry.register("TDIGEST.CREATE", tdigest_create);
        registry.register("TDIGEST.RESET", tdigest_reset);
        registry.register("TDIGEST.ADD", tdigest_add);
        registry.register("TDIGEST.MERGE", tdigest_merge);
        registry.register("TDIGEST.MIN", tdigest_min);
        registry.register("TDIGEST.MAX", tdigest_max);
        registry.register("TDIGEST.MEAN", tdigest_mean);
        registry.register("TDIGEST.QUANTILE", tdigest_quantile);
        registry.register("TDIGEST.CDF", tdigest_cdf);
        registry.register("TDIGEST.TRIMMED_MEAN", tdigest_trimmed_mean);
        registry.register("TDIGEST.RANK", tdigest_rank);
        registry.register("TDIGEST.REVRANK", tdigest_revrank);
        registry.register("TDIGEST.BYRANK", tdigest_byrank);
        registry.register("TDIGEST.BYREVRANK", tdigest_byrevrank);
        registry.register("TDIGEST.INFO", tdigest_info);
    }

    fn init_shard(&self, _shard: &mut ShardState) {}
}

fn bulk(value: impl Into<Bytes>) -> ModuleResponse {
    ModuleResponse::Bulk(Some(value.into()))
}

fn nil() -> ModuleResponse {
    ModuleResponse::Bulk(None)
}

fn int(value: impl Into<i64>) -> ModuleResponse {
    ModuleResponse::Integer(value.into())
}

fn array(values: impl IntoIterator<Item = ModuleResponse>) -> ModuleResponse {
    ModuleResponse::Array(Box::new(
        values
            .into_iter()
            .collect::<SmallVec<[ModuleResponse; 16]>>(),
    ))
}

fn map(values: impl IntoIterator<Item = ModuleResponse>) -> ModuleResponse {
    ModuleResponse::Map(Box::new(
        values
            .into_iter()
            .collect::<SmallVec<[ModuleResponse; 32]>>(),
    ))
}

fn ok() -> ModuleResponse {
    ModuleResponse::Simple(b"OK")
}

fn err(error: ProbError) -> ModuleError {
    error.into()
}

fn parse_u64(raw: &[u8]) -> Result<u64, ModuleError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or_else(|| err(ProbError::BadCapacity))
}

fn parse_usize(raw: &[u8]) -> Result<usize, ModuleError> {
    parse_u64(raw).map(|value| value as usize)
}

fn parse_f64(raw: &[u8]) -> Result<f64, ModuleError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<f64>().ok())
        .ok_or_else(|| err(ProbError::Syntax))
}

fn key<'a>(args: &'a [&'a [u8]]) -> Result<&'a [u8], ModuleError> {
    args.first()
        .copied()
        .ok_or_else(|| err(ProbError::WrongArity))
}

#[cfg(test)]
fn type_string(value: &SenkoValue) -> &'static [u8] {
    match value {
        SenkoValue::BloomFilter(_) => b"MBbloom--",
        SenkoValue::CuckooFilter(_) => b"cuckooFilter",
        SenkoValue::CountMinSketch(_) => b"CMSk--",
        SenkoValue::TopK(_) => b"topk",
        SenkoValue::TDigest(_) => b"TDIS-TYPE",
        _ => b"",
    }
}

fn get_prob_value(
    ctx: &mut dyn ModuleCommandContext,
    key: &[u8],
) -> Result<Option<SenkoValue>, ModuleError> {
    Ok(ctx.get_value(key))
}

fn set_value(ctx: &mut dyn ModuleCommandContext, key: &[u8], value: SenkoValue) {
    ctx.set_value(key, value);
}

fn bloom_mut(
    ctx: &mut dyn ModuleCommandContext,
    key: &[u8],
    create: impl FnOnce() -> BloomFilter,
) -> Result<BloomFilter, ModuleError> {
    match get_prob_value(ctx, key)? {
        Some(SenkoValue::BloomFilter(filter)) => Ok(*filter),
        Some(_) => Err(err(ProbError::WrongType)),
        None => Ok(create()),
    }
}

fn cuckoo_mut(
    ctx: &mut dyn ModuleCommandContext,
    key: &[u8],
    create: impl FnOnce() -> CuckooFilter,
) -> Result<CuckooFilter, ModuleError> {
    match get_prob_value(ctx, key)? {
        Some(SenkoValue::CuckooFilter(filter)) => Ok(*filter),
        Some(_) => Err(err(ProbError::WrongType)),
        None => Ok(create()),
    }
}

fn cms_mut(
    ctx: &mut dyn ModuleCommandContext,
    key: &[u8],
    create: impl FnOnce() -> CountMinSketch,
) -> Result<CountMinSketch, ModuleError> {
    match get_prob_value(ctx, key)? {
        Some(SenkoValue::CountMinSketch(sketch)) => Ok(*sketch),
        Some(_) => Err(err(ProbError::WrongType)),
        None => Ok(create()),
    }
}

fn topk_mut(
    ctx: &mut dyn ModuleCommandContext,
    key: &[u8],
    create: impl FnOnce() -> TopKSketch,
) -> Result<TopKSketch, ModuleError> {
    match get_prob_value(ctx, key)? {
        Some(SenkoValue::TopK(sketch)) => Ok(*sketch),
        Some(_) => Err(err(ProbError::WrongType)),
        None => Ok(create()),
    }
}

fn tdigest_mut(
    ctx: &mut dyn ModuleCommandContext,
    key: &[u8],
    create: impl FnOnce() -> TDigest,
) -> Result<TDigest, ModuleError> {
    match get_prob_value(ctx, key)? {
        Some(SenkoValue::TDigest(digest)) => Ok(*digest),
        Some(_) => Err(err(ProbError::WrongType)),
        None => Ok(create()),
    }
}

fn bf_reserve(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 {
        return Err(err(ProbError::WrongArity));
    }
    if ctx.get_value(args[0]).is_some() {
        return Err(err(ProbError::KeyExists));
    }
    let error_rate = parse_f64(args[1])?;
    if !(0.0 < error_rate && error_rate < 1.0) {
        return Err(err(ProbError::BadErrorRate));
    }
    let capacity = parse_u64(args[2])?;
    if capacity == 0 {
        return Err(err(ProbError::BadCapacity));
    }
    let mut expansion = 2u8;
    let mut non_scaling = false;
    let mut index = 3usize;
    while index < args.len() {
        if args[index].eq_ignore_ascii_case(b"EXPANSION") {
            expansion =
                parse_u64(args.get(index + 1).ok_or_else(|| err(ProbError::Syntax))?)? as u8;
            index += 2;
        } else if args[index].eq_ignore_ascii_case(b"NONSCALING") {
            non_scaling = true;
            index += 1;
        } else {
            return Err(err(ProbError::Syntax));
        }
    }
    set_value(
        ctx,
        args[0],
        SenkoValue::BloomFilter(Box::new(BloomFilter::new(
            capacity,
            error_rate,
            expansion,
            non_scaling,
        ))),
    );
    Ok(ok())
}

fn bf_add_like(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]], multi: bool) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let mut filter = bloom_mut(ctx, args[0], || BloomFilter::new(100, 0.01, 2, false))?;
    let mut out = SmallVec::<[ModuleResponse; 16]>::new();
    for item in &args[1..] {
        let inserted = filter.add(item).map_err(|_| err(ProbError::FilterFull))?;
        if multi {
            out.push(int(inserted as i64));
        } else {
            set_value(ctx, args[0], SenkoValue::BloomFilter(Box::new(filter)));
            return Ok(int(inserted as i64));
        }
    }
    set_value(ctx, args[0], SenkoValue::BloomFilter(Box::new(filter)));
    Ok(array(out))
}

fn bf_add(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    bf_add_like(ctx, args, false)
}
fn bf_madd(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    bf_add_like(ctx, args, true)
}

fn bf_insert(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 {
        return Err(err(ProbError::WrongArity));
    }
    let key = args[0];
    let mut capacity = 100u64;
    let mut error_rate = 0.01;
    let mut expansion = 2u8;
    let mut non_scaling = false;
    let mut nocreate = false;
    let mut index = 1usize;
    while index < args.len() && !args[index].eq_ignore_ascii_case(b"ITEMS") {
        match args[index] {
            token if token.eq_ignore_ascii_case(b"CAPACITY") => {
                capacity = parse_u64(args.get(index + 1).ok_or_else(|| err(ProbError::Syntax))?)?;
                index += 2;
            }
            token if token.eq_ignore_ascii_case(b"ERROR") => {
                error_rate =
                    parse_f64(args.get(index + 1).ok_or_else(|| err(ProbError::Syntax))?)?;
                index += 2;
            }
            token if token.eq_ignore_ascii_case(b"EXPANSION") => {
                expansion =
                    parse_u64(args.get(index + 1).ok_or_else(|| err(ProbError::Syntax))?)? as u8;
                index += 2;
            }
            token if token.eq_ignore_ascii_case(b"NOCREATE") => {
                nocreate = true;
                index += 1;
            }
            token if token.eq_ignore_ascii_case(b"NONSCALING") => {
                non_scaling = true;
                index += 1;
            }
            _ => return Err(err(ProbError::Syntax)),
        }
    }
    if index >= args.len() || !args[index].eq_ignore_ascii_case(b"ITEMS") {
        return Err(err(ProbError::Syntax));
    }
    let existing = ctx.get_value(key);
    let mut filter = match existing {
        Some(SenkoValue::BloomFilter(filter)) => *filter,
        Some(_) => return Err(err(ProbError::WrongType)),
        None if nocreate => return Err(err(ProbError::NoCreate)),
        None => BloomFilter::new(capacity, error_rate, expansion, non_scaling),
    };
    let mut out = SmallVec::<[ModuleResponse; 16]>::new();
    for item in &args[index + 1..] {
        out.push(int(
            filter.add(item).map_err(|_| err(ProbError::FilterFull))? as i64,
        ));
    }
    set_value(ctx, key, SenkoValue::BloomFilter(Box::new(filter)));
    Ok(array(out))
}

fn bf_exists_like(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]], multi: bool) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let Some(value) = ctx.get_value(args[0]) else {
        return Ok(if multi {
            array(Vec::from_iter(args[1..].iter().map(|_| int(0))))
        } else {
            int(0)
        });
    };
    let SenkoValue::BloomFilter(filter) = value else {
        return Err(err(ProbError::WrongType));
    };
    if multi {
        Ok(array(
            args[1..].iter().map(|item| int(filter.exists(item) as i64)),
        ))
    } else {
        Ok(int(filter.exists(args[1]) as i64))
    }
}

fn bf_exists(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    bf_exists_like(ctx, args, false)
}
fn bf_mexists(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    bf_exists_like(ctx, args, true)
}

fn bf_card(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    let key = key(args)?;
    let Some(SenkoValue::BloomFilter(filter)) = ctx.get_value(key) else {
        return Ok(int(0));
    };
    Ok(int(filter.total_items as i64))
}

fn bf_info(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    let key = key(args)?;
    let Some(value) = ctx.get_value(key) else {
        return Err(err(ProbError::KeyNotFound));
    };
    let SenkoValue::BloomFilter(filter) = value else {
        return Err(err(ProbError::WrongType));
    };
    let size = filter
        .filters
        .iter()
        .map(|sub| sub.bits.byte_len() as i64)
        .sum::<i64>();
    Ok(map([
        bulk("Capacity"),
        int(filter.capacity as i64),
        bulk("Size"),
        int(size),
        bulk("Number of filters"),
        int(filter.filters.len() as i64),
        bulk("Number of items inserted"),
        int(filter.total_items as i64),
        bulk("Expansion rate"),
        int(filter.expansion as i64),
    ]))
}

fn bf_scandump(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 2 {
        return Err(err(ProbError::WrongArity));
    }
    let iter = parse_usize(args[1])?;
    let Some(value) = ctx.get_value(args[0]) else {
        return Ok(array([int(0), bulk(Bytes::new())]));
    };
    let SenkoValue::BloomFilter(filter) = value else {
        return Err(err(ProbError::WrongType));
    };
    if let Some((next, chunk)) = filter.scandump(iter) {
        Ok(array([int(next as i64), bulk(chunk)]))
    } else {
        Ok(array([int(0), bulk(Bytes::new())]))
    }
}

fn bf_loadchunk(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 3 {
        return Err(err(ProbError::WrongArity));
    }
    let iter = parse_usize(args[1])?;
    let mut filter = bloom_mut(ctx, args[0], || BloomFilter::new(100, 0.01, 2, false))?;
    if let Some(last) = filter.filters.last_mut() {
        last.bits.load_chunk(iter, args[2]);
    }
    set_value(ctx, args[0], SenkoValue::BloomFilter(Box::new(filter)));
    Ok(ok())
}

fn cf_reserve(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    if ctx.get_value(args[0]).is_some() {
        return Err(err(ProbError::KeyExists));
    }
    let capacity = parse_usize(args[1])?;
    let mut bucket_size = 2usize;
    let mut max_iterations = 500usize;
    let mut expansion = 1usize;
    let mut index = 2usize;
    while index < args.len() {
        match args[index] {
            token if token.eq_ignore_ascii_case(b"BUCKETSIZE") => {
                bucket_size =
                    parse_usize(args.get(index + 1).ok_or_else(|| err(ProbError::Syntax))?)?;
                index += 2;
            }
            token if token.eq_ignore_ascii_case(b"MAXITERATIONS") => {
                max_iterations =
                    parse_usize(args.get(index + 1).ok_or_else(|| err(ProbError::Syntax))?)?;
                index += 2;
            }
            token if token.eq_ignore_ascii_case(b"EXPANSION") => {
                expansion =
                    parse_usize(args.get(index + 1).ok_or_else(|| err(ProbError::Syntax))?)?;
                index += 2;
            }
            _ => return Err(err(ProbError::Syntax)),
        }
    }
    set_value(
        ctx,
        args[0],
        SenkoValue::CuckooFilter(Box::new(CuckooFilter::new(
            capacity,
            bucket_size,
            8,
            max_iterations,
            expansion,
        ))),
    );
    Ok(ok())
}

fn cf_add_like(
    ctx: &mut dyn ModuleCommandContext,
    args: &[&[u8]],
    nx: bool,
    multi: bool,
) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let mut filter = cuckoo_mut(ctx, args[0], || CuckooFilter::new(1024, 2, 8, 500, 1))?;
    let mut out = SmallVec::<[ModuleResponse; 16]>::new();
    for item in &args[1..] {
        let inserted = if nx {
            filter
                .add_nx(item)
                .map_err(|_| err(ProbError::CuckooFull))?
        } else {
            filter.add(item).map_err(|_| err(ProbError::CuckooFull))?;
            true
        };
        if multi {
            out.push(int(inserted as i64));
        } else {
            set_value(ctx, args[0], SenkoValue::CuckooFilter(Box::new(filter)));
            return Ok(int(inserted as i64));
        }
    }
    set_value(ctx, args[0], SenkoValue::CuckooFilter(Box::new(filter)));
    Ok(array(out))
}

fn cf_add(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    cf_add_like(ctx, args, false, false)
}
fn cf_addnx(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    cf_add_like(ctx, args, true, false)
}

fn cf_insert_impl(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]], nx: bool) -> ModuleResult {
    if args.len() < 3 {
        return Err(err(ProbError::WrongArity));
    }
    let key = args[0];
    let mut capacity = 1024usize;
    let mut nocreate = false;
    let mut index = 1usize;
    while index < args.len() && !args[index].eq_ignore_ascii_case(b"ITEMS") {
        match args[index] {
            token if token.eq_ignore_ascii_case(b"CAPACITY") => {
                capacity =
                    parse_usize(args.get(index + 1).ok_or_else(|| err(ProbError::Syntax))?)?;
                index += 2;
            }
            token if token.eq_ignore_ascii_case(b"NOCREATE") => {
                nocreate = true;
                index += 1;
            }
            _ => return Err(err(ProbError::Syntax)),
        }
    }
    if index >= args.len() {
        return Err(err(ProbError::Syntax));
    }
    let existing = ctx.get_value(key);
    let mut filter = match existing {
        Some(SenkoValue::CuckooFilter(filter)) => *filter,
        Some(_) => return Err(err(ProbError::WrongType)),
        None if nocreate => return Err(err(ProbError::NoCreate)),
        None => CuckooFilter::new(capacity, 2, 8, 500, 1),
    };
    let mut out = SmallVec::<[ModuleResponse; 16]>::new();
    for item in &args[index + 1..] {
        let inserted = if nx {
            filter
                .add_nx(item)
                .map_err(|_| err(ProbError::CuckooFull))?
        } else {
            filter.add(item).map_err(|_| err(ProbError::CuckooFull))?;
            true
        };
        out.push(int(inserted as i64));
    }
    set_value(ctx, key, SenkoValue::CuckooFilter(Box::new(filter)));
    Ok(array(out))
}

fn cf_insert(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    cf_insert_impl(ctx, args, false)
}
fn cf_insertnx(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    cf_insert_impl(ctx, args, true)
}

fn cf_exists(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 2 {
        return Err(err(ProbError::WrongArity));
    }
    let Some(value) = ctx.get_value(args[0]) else {
        return Ok(int(0));
    };
    let SenkoValue::CuckooFilter(filter) = value else {
        return Err(err(ProbError::WrongType));
    };
    Ok(int(filter.exists(args[1]) as i64))
}

fn cf_mexists(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let Some(value) = ctx.get_value(args[0]) else {
        return Ok(array(args[1..].iter().map(|_| int(0))));
    };
    let SenkoValue::CuckooFilter(filter) = value else {
        return Err(err(ProbError::WrongType));
    };
    Ok(array(
        args[1..].iter().map(|item| int(filter.exists(item) as i64)),
    ))
}

fn cf_del(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 2 {
        return Err(err(ProbError::WrongArity));
    }
    let mut filter = cuckoo_mut(ctx, args[0], || CuckooFilter::new(1024, 2, 8, 500, 1))?;
    let deleted = filter.delete(args[1]);
    set_value(ctx, args[0], SenkoValue::CuckooFilter(Box::new(filter)));
    Ok(int(deleted as i64))
}

fn cf_count(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 2 {
        return Err(err(ProbError::WrongArity));
    }
    let Some(value) = ctx.get_value(args[0]) else {
        return Ok(int(0));
    };
    let SenkoValue::CuckooFilter(filter) = value else {
        return Err(err(ProbError::WrongType));
    };
    Ok(int(filter.count(args[1]) as i64))
}

fn cf_info(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    let key = key(args)?;
    let Some(value) = ctx.get_value(key) else {
        return Err(err(ProbError::KeyNotFound));
    };
    let SenkoValue::CuckooFilter(filter) = value else {
        return Err(err(ProbError::WrongType));
    };
    let layer = filter.layers.last().expect("layer");
    let size = (layer.num_buckets * filter.bucket_size * std::mem::size_of::<u16>()) as i64;
    Ok(map([
        bulk("Size"),
        int(size),
        bulk("Number of buckets"),
        int(layer.num_buckets as i64),
        bulk("Number of filters"),
        int(filter.layers.len() as i64),
        bulk("Number of items inserted"),
        int(filter.num_items as i64),
        bulk("Number of items deleted"),
        int(filter.num_deletes as i64),
        bulk("Bucket size"),
        int(filter.bucket_size as i64),
        bulk("Expansion rate"),
        int(filter.expansion as i64),
        bulk("Max iterations"),
        int(filter.max_iterations as i64),
    ]))
}

fn cf_scandump(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 2 {
        return Err(err(ProbError::WrongArity));
    }
    let iter = parse_usize(args[1])?;
    let Some(value) = ctx.get_value(args[0]) else {
        return Ok(array([int(0), bulk(Bytes::new())]));
    };
    let SenkoValue::CuckooFilter(filter) = value else {
        return Err(err(ProbError::WrongType));
    };
    let layer = filter.layers.last().expect("layer");
    let mut raw = Vec::new();
    for bucket in &layer.buckets {
        for slot in &bucket.slots {
            raw.extend_from_slice(&slot.to_le_bytes());
        }
    }
    let start = iter * 8192;
    if start >= raw.len() {
        return Ok(array([int(0), bulk(Bytes::new())]));
    }
    let end = (start + 8192).min(raw.len());
    Ok(array([
        int((iter + 1) as i64),
        bulk(Bytes::from(raw[start..end].to_vec())),
    ]))
}

fn cf_loadchunk(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 3 {
        return Err(err(ProbError::WrongArity));
    }
    let iter = parse_usize(args[1])?;
    let mut filter = cuckoo_mut(ctx, args[0], || CuckooFilter::new(1024, 2, 8, 500, 1))?;
    let layer = filter.layers.last_mut().expect("layer");
    let mut offset = iter * 8192;
    for bucket in &mut layer.buckets {
        for slot in &mut bucket.slots {
            if offset + 2 > args[2].len() + iter * 8192 {
                break;
            }
            let local = offset - iter * 8192;
            if local + 2 <= args[2].len() {
                *slot = u16::from_le_bytes([args[2][local], args[2][local + 1]]);
            }
            offset += 2;
        }
    }
    set_value(ctx, args[0], SenkoValue::CuckooFilter(Box::new(filter)));
    Ok(ok())
}

fn cms_initbydim(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 3 {
        return Err(err(ProbError::WrongArity));
    }
    let width = parse_usize(args[1])?;
    let depth = parse_usize(args[2])?;
    set_value(
        ctx,
        args[0],
        SenkoValue::CountMinSketch(Box::new(CountMinSketch::new(width, depth))),
    );
    Ok(ok())
}

fn cms_initbyprob(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 3 {
        return Err(err(ProbError::WrongArity));
    }
    let error = parse_f64(args[1])?;
    let delta = parse_f64(args[2])?;
    set_value(
        ctx,
        args[0],
        SenkoValue::CountMinSketch(Box::new(CountMinSketch::new(
            CountMinSketch::width_from_error(error),
            CountMinSketch::depth_from_confidence(delta),
        ))),
    );
    Ok(ok())
}

fn cms_incrby(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return Err(err(ProbError::WrongArity));
    }
    let mut sketch = cms_mut(ctx, args[0], || CountMinSketch::new(2000, 5))?;
    let mut out = SmallVec::<[ModuleResponse; 16]>::new();
    let mut index = 1usize;
    while index < args.len() {
        let increment = parse_u64(args[index + 1])?;
        out.push(int(sketch.incrby(args[index], increment) as i64));
        index += 2;
    }
    set_value(ctx, args[0], SenkoValue::CountMinSketch(Box::new(sketch)));
    Ok(array(out))
}

fn cms_query(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let Some(value) = ctx.get_value(args[0]) else {
        return Ok(array(args[1..].iter().map(|_| int(0))));
    };
    let SenkoValue::CountMinSketch(sketch) = value else {
        return Err(err(ProbError::WrongType));
    };
    Ok(array(
        args[1..].iter().map(|item| int(sketch.query(item) as i64)),
    ))
}

fn cms_merge(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 {
        return Err(err(ProbError::WrongArity));
    }
    let dest_key = args[0];
    let num_keys = parse_usize(args[1])?;
    if args.len() < 2 + num_keys {
        return Err(err(ProbError::WrongArity));
    }
    let mut weights = vec![1u64; num_keys];
    if args.len() > 2 + num_keys {
        if !args[2 + num_keys].eq_ignore_ascii_case(b"WEIGHTS") {
            return Err(err(ProbError::Syntax));
        }
        if args.len() != 3 + num_keys * 2 {
            return Err(err(ProbError::Syntax));
        }
        for i in 0..num_keys {
            weights[i] = parse_u64(args[3 + num_keys + i])?;
        }
    }
    let mut merged = None::<CountMinSketch>;
    for (source_key, weight) in args[2..2 + num_keys].iter().zip(weights.into_iter()) {
        for value in ctx.get_prob_merge_values(source_key) {
            let ProbMergeValue::CountMinSketch(sketch) = value else {
                return Err(err(ProbError::SketchDimMismatch));
            };
            match &mut merged {
                Some(dest) => {
                    if !dest.merge_from(&sketch, weight) {
                        return Err(err(ProbError::SketchDimMismatch));
                    }
                }
                None => {
                    let mut dest = (*sketch).clone();
                    if weight > 1 {
                        for cell in &mut dest.counters {
                            *cell = cell.saturating_mul(weight);
                        }
                    }
                    merged = Some(dest);
                }
            }
        }
    }
    let merged = merged.ok_or_else(|| err(ProbError::KeyNotFound))?;
    set_value(ctx, dest_key, SenkoValue::CountMinSketch(Box::new(merged)));
    Ok(ok())
}

fn cms_info(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    let key = key(args)?;
    let Some(value) = ctx.get_value(key) else {
        return Err(err(ProbError::KeyNotFound));
    };
    let SenkoValue::CountMinSketch(sketch) = value else {
        return Err(err(ProbError::WrongType));
    };
    Ok(map([
        bulk("width"),
        int(sketch.width as i64),
        bulk("depth"),
        int(sketch.depth as i64),
        bulk("count"),
        int(sketch.total_count as i64),
    ]))
}

fn topk_defaults(k: usize) -> (usize, usize, f64) {
    let log = (k.max(2) as f64).log2().ceil() as usize;
    (k.max(1) * log.max(1), log.max(5), 0.9)
}

fn topk_reserve(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let k = parse_usize(args[1])?;
    let (default_width, default_depth, default_decay) = topk_defaults(k);
    let width = args
        .get(2)
        .map(|raw| parse_usize(raw))
        .transpose()?
        .unwrap_or(default_width);
    let depth = args
        .get(3)
        .map(|raw| parse_usize(raw))
        .transpose()?
        .unwrap_or(default_depth);
    let decay = args
        .get(4)
        .map(|raw| parse_f64(raw))
        .transpose()?
        .unwrap_or(default_decay);
    set_value(
        ctx,
        args[0],
        SenkoValue::TopK(Box::new(TopKSketch::new(k, width, depth, decay))),
    );
    Ok(ok())
}

fn topk_add(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let mut sketch = topk_mut(ctx, args[0], || {
        let (width, depth, decay) = topk_defaults(10);
        TopKSketch::new(10, width, depth, decay)
    })?;
    let responses = args[1..]
        .iter()
        .map(|item| sketch.add(item, 1).map_or_else(nil, bulk))
        .collect::<Vec<_>>();
    set_value(ctx, args[0], SenkoValue::TopK(Box::new(sketch)));
    Ok(array(responses))
}

fn topk_incrby(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return Err(err(ProbError::WrongArity));
    }
    let mut sketch = topk_mut(ctx, args[0], || {
        let (width, depth, decay) = topk_defaults(10);
        TopKSketch::new(10, width, depth, decay)
    })?;
    let mut out = SmallVec::<[ModuleResponse; 16]>::new();
    let mut index = 1usize;
    while index < args.len() {
        let increment = parse_u64(args[index + 1])?;
        out.push(sketch.add(args[index], increment).map_or_else(nil, bulk));
        index += 2;
    }
    set_value(ctx, args[0], SenkoValue::TopK(Box::new(sketch)));
    Ok(array(out))
}

fn topk_query(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let Some(value) = ctx.get_value(args[0]) else {
        return Ok(array(args[1..].iter().map(|_| int(0))));
    };
    let SenkoValue::TopK(sketch) = value else {
        return Err(err(ProbError::WrongType));
    };
    Ok(array(
        args[1..].iter().map(|item| int(sketch.query(item) as i64)),
    ))
}

fn topk_count(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let Some(value) = ctx.get_value(args[0]) else {
        return Ok(array(args[1..].iter().map(|_| int(0))));
    };
    let SenkoValue::TopK(sketch) = value else {
        return Err(err(ProbError::WrongType));
    };
    Ok(array(
        args[1..].iter().map(|item| int(sketch.count(item) as i64)),
    ))
}

fn topk_list(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() {
        return Err(err(ProbError::WrongArity));
    }
    let with_count = args
        .get(1)
        .is_some_and(|token| token.eq_ignore_ascii_case(b"WITHCOUNT"));
    let Some(value) = ctx.get_value(args[0]) else {
        return Ok(array([]));
    };
    let SenkoValue::TopK(sketch) = value else {
        return Err(err(ProbError::WrongType));
    };
    let mut out = SmallVec::<[ModuleResponse; 16]>::new();
    for (item, count) in sketch.list() {
        out.push(bulk(item));
        if with_count {
            out.push(int(count as i64));
        }
    }
    Ok(array(out))
}

fn topk_info(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    let key = key(args)?;
    let Some(value) = ctx.get_value(key) else {
        return Err(err(ProbError::KeyNotFound));
    };
    let SenkoValue::TopK(sketch) = value else {
        return Err(err(ProbError::WrongType));
    };
    Ok(map([
        bulk("k"),
        int(sketch.k as i64),
        bulk("width"),
        int(sketch.width as i64),
        bulk("depth"),
        int(sketch.depth as i64),
        bulk("decay"),
        bulk(Bytes::from(sketch.decay.to_string())),
    ]))
}

fn tdigest_create(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() {
        return Err(err(ProbError::WrongArity));
    }
    let mut compression = 100.0;
    if args.len() == 3 {
        if !args[1].eq_ignore_ascii_case(b"COMPRESSION") {
            return Err(err(ProbError::Syntax));
        }
        compression = parse_f64(args[2])?;
    } else if args.len() != 1 {
        return Err(err(ProbError::Syntax));
    }
    if !(1.0..=1000.0).contains(&compression) {
        return Err(err(ProbError::BadCompression));
    }
    set_value(
        ctx,
        args[0],
        SenkoValue::TDigest(Box::new(TDigest::new(compression))),
    );
    Ok(ok())
}

fn tdigest_reset(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    let mut digest = tdigest_mut(ctx, key(args)?, || TDigest::new(100.0))?;
    digest.reset();
    set_value(ctx, args[0], SenkoValue::TDigest(Box::new(digest)));
    Ok(ok())
}

fn tdigest_add(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let mut digest = tdigest_mut(ctx, args[0], || TDigest::new(100.0))?;
    for raw in &args[1..] {
        digest.add(parse_f64(raw)?);
    }
    set_value(ctx, args[0], SenkoValue::TDigest(Box::new(digest)));
    Ok(ok())
}

fn tdigest_merge(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 {
        return Err(err(ProbError::WrongArity));
    }
    let dest_key = args[0];
    let num_keys = parse_usize(args[1])?;
    if args.len() < 2 + num_keys {
        return Err(err(ProbError::WrongArity));
    }
    let mut compression = None::<f64>;
    let mut override_dest = false;
    let mut index = 2 + num_keys;
    while index < args.len() {
        match args[index] {
            token if token.eq_ignore_ascii_case(b"COMPRESSION") => {
                compression = Some(parse_f64(
                    args.get(index + 1).ok_or_else(|| err(ProbError::Syntax))?,
                )?);
                index += 2;
            }
            token if token.eq_ignore_ascii_case(b"OVERRIDE") => {
                override_dest = true;
                index += 1;
            }
            _ => return Err(err(ProbError::Syntax)),
        }
    }
    let mut dest = if !override_dest {
        tdigest_mut(ctx, dest_key, || TDigest::new(compression.unwrap_or(100.0)))?
    } else {
        TDigest::new(compression.unwrap_or(100.0))
    };
    for source_key in &args[2..2 + num_keys] {
        for value in ctx.get_prob_merge_values(source_key) {
            let ProbMergeValue::TDigest(mut source) = value else {
                return Err(err(ProbError::WrongType));
            };
            dest.merge_from(&mut source);
        }
    }
    set_value(ctx, dest_key, SenkoValue::TDigest(Box::new(dest)));
    Ok(ok())
}

fn tdigest_scalar(
    ctx: &mut dyn ModuleCommandContext,
    args: &[&[u8]],
    which: fn(&mut TDigest) -> Option<f64>,
) -> ModuleResult {
    let mut digest = tdigest_mut(ctx, key(args)?, || TDigest::new(100.0))?;
    let result = which(&mut digest);
    set_value(ctx, args[0], SenkoValue::TDigest(Box::new(digest)));
    Ok(result.map_or_else(nil, |value| bulk(Bytes::from(value.to_string()))))
}

fn tdigest_min(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    tdigest_scalar(ctx, args, |digest| {
        (digest.total_weight > 0.0 || !digest.unmerged.is_empty()).then_some(digest.min)
    })
}

fn tdigest_max(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    tdigest_scalar(ctx, args, |digest| {
        (digest.total_weight > 0.0 || !digest.unmerged.is_empty()).then_some(digest.max)
    })
}

fn tdigest_mean(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    tdigest_scalar(ctx, args, TDigest::mean)
}

fn tdigest_quantile(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let mut digest = tdigest_mut(ctx, args[0], || TDigest::new(100.0))?;
    let mut out = SmallVec::<[ModuleResponse; 16]>::new();
    for raw in &args[1..] {
        let q = parse_f64(raw)?;
        if !(0.0..=1.0).contains(&q) {
            return Err(err(ProbError::BadQuantile));
        }
        out.push(
            digest
                .quantile(q)
                .map_or_else(nil, |value| bulk(Bytes::from(value.to_string()))),
        );
    }
    set_value(ctx, args[0], SenkoValue::TDigest(Box::new(digest)));
    Ok(array(out))
}

fn tdigest_cdf(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let mut digest = tdigest_mut(ctx, args[0], || TDigest::new(100.0))?;
    let out = args[1..]
        .iter()
        .map(|raw| parse_f64(raw).map(|value| bulk(Bytes::from(digest.cdf(value).to_string()))))
        .collect::<Result<Vec<_>, _>>()?;
    set_value(ctx, args[0], SenkoValue::TDigest(Box::new(digest)));
    Ok(array(out))
}

fn tdigest_trimmed_mean(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 3 {
        return Err(err(ProbError::WrongArity));
    }
    let mut digest = tdigest_mut(ctx, args[0], || TDigest::new(100.0))?;
    let low = parse_f64(args[1])?;
    let high = parse_f64(args[2])?;
    let out = digest
        .trimmed_mean(low, high)
        .map_or_else(nil, |value| bulk(Bytes::from(value.to_string())));
    set_value(ctx, args[0], SenkoValue::TDigest(Box::new(digest)));
    Ok(out)
}

fn tdigest_rank(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let mut digest = tdigest_mut(ctx, args[0], || TDigest::new(100.0))?;
    let out = args[1..]
        .iter()
        .map(|raw| parse_f64(raw).map(|value| int(digest.rank(value) as i64)))
        .collect::<Result<Vec<_>, _>>()?;
    set_value(ctx, args[0], SenkoValue::TDigest(Box::new(digest)));
    Ok(array(out))
}

fn tdigest_revrank(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let mut digest = tdigest_mut(ctx, args[0], || TDigest::new(100.0))?;
    let out = args[1..]
        .iter()
        .map(|raw| parse_f64(raw).map(|value| int(digest.rev_rank(value) as i64)))
        .collect::<Result<Vec<_>, _>>()?;
    set_value(ctx, args[0], SenkoValue::TDigest(Box::new(digest)));
    Ok(array(out))
}

fn tdigest_byrank(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let mut digest = tdigest_mut(ctx, args[0], || TDigest::new(100.0))?;
    let out = args[1..]
        .iter()
        .map(|raw| {
            parse_u64(raw).map(|rank| {
                digest
                    .by_rank(rank)
                    .map_or_else(nil, |value| bulk(Bytes::from(value.to_string())))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    set_value(ctx, args[0], SenkoValue::TDigest(Box::new(digest)));
    Ok(array(out))
}

fn tdigest_byrevrank(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(ProbError::WrongArity));
    }
    let mut digest = tdigest_mut(ctx, args[0], || TDigest::new(100.0))?;
    let out = args[1..]
        .iter()
        .map(|raw| {
            parse_u64(raw).map(|rank| {
                digest
                    .by_rev_rank(rank)
                    .map_or_else(nil, |value| bulk(Bytes::from(value.to_string())))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    set_value(ctx, args[0], SenkoValue::TDigest(Box::new(digest)));
    Ok(array(out))
}

fn tdigest_info(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    let digest = tdigest_mut(ctx, key(args)?, || TDigest::new(100.0))?;
    let response = map([
        bulk("Compression"),
        bulk(Bytes::from(digest.compression.to_string())),
        bulk("Capacity"),
        int((digest.compression * 5.0) as i64),
        bulk("Merges"),
        int(digest.merges as i64),
        bulk("Nodes"),
        int(digest.centroids.len() as i64),
        bulk("Total compressions"),
        int(digest.total_compressions as i64),
        bulk("Memory usage"),
        int((digest.centroids.len() * std::mem::size_of::<(f64, f64)>()) as i64),
        bulk("Total weight"),
        bulk(Bytes::from(digest.total_weight.to_string())),
        bulk("Min"),
        bulk(Bytes::from(if digest.min.is_finite() {
            digest.min.to_string()
        } else {
            "nan".to_string()
        })),
        bulk("Max"),
        bulk(Bytes::from(if digest.max.is_finite() {
            digest.max.to_string()
        } else {
            "nan".to_string()
        })),
    ]);
    set_value(ctx, args[0], SenkoValue::TDigest(Box::new(digest)));
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::{type_string, ProbError, ProbModule};
    use senko_core::{CommandRegistry, SenkoModule, SenkoValue, TDigest, TopKSketch};

    #[test]
    fn module_registers_expected_command_groups() {
        let module = ProbModule;
        let mut registry = CommandRegistry::default();
        module.register_commands(&mut registry);
    }

    #[test]
    fn type_strings_match_expected_wire_values() {
        assert_eq!(
            type_string(&SenkoValue::TDigest(Box::new(TDigest::new(100.0)))),
            b"TDIS-TYPE"
        );
        assert_eq!(
            type_string(&SenkoValue::TopK(Box::new(TopKSketch::new(3, 8, 5, 0.9)))),
            b"topk"
        );
    }

    #[test]
    fn error_messages_match_spec() {
        assert_eq!(
            ProbError::NoCreate.to_string(),
            "ERR NOTCREATED: NOCREATE specified and key does not exist"
        );
    }
}
