#![deny(unsafe_code)]

pub mod error;
pub mod path;

use std::sync::Arc;

use bytes::Bytes;
use senko_core::{
    CommandRegistry, ModuleCommandContext, ModuleError, ModuleResponse, ModuleResult, SenkoModule,
    SenkoValue, ShardState,
};
use smallvec::SmallVec;
use sonic_rs::{Array, JsonContainerTrait, JsonValueMutTrait, JsonValueTrait, Value as JsonValue};

pub use error::{JsonError, JsonPathError};
pub use path::{JsonPathToken, normalize_path, parse_path};

#[inline]
pub fn parse_document(input: &str) -> Result<Arc<JsonValue>, JsonError> {
    sonic_rs::from_str::<JsonValue>(input)
        .map(Arc::new)
        .map_err(|err| JsonError::ParseError(err.to_string()))
}

#[inline]
pub fn type_name(value: &JsonValue) -> &'static str {
    match value.get_type() {
        sonic_rs::JsonType::Object => "object",
        sonic_rs::JsonType::Array => "array",
        sonic_rs::JsonType::String => "string",
        sonic_rs::JsonType::Boolean => "boolean",
        sonic_rs::JsonType::Null => "null",
        sonic_rs::JsonType::Number => {
            if value.is_i64() || value.is_u64() {
                "integer"
            } else {
                "number"
            }
        }
    }
}

#[inline]
pub fn object_encoding(value: &JsonValue) -> &'static str {
    match sonic_rs::to_string(value) {
        Ok(rendered) if rendered.len() <= 44 => "embstr",
        Ok(_) => "raw",
        Err(_) => "raw",
    }
}

pub fn get_path<'a>(root: &'a JsonValue, path: &str) -> Result<Vec<&'a JsonValue>, JsonPathError> {
    path::eval_read(root, path)
}

pub fn set_root(input: &str) -> Result<Arc<JsonValue>, JsonError> {
    parse_document(input)
}

pub fn set_path(
    doc: &Arc<JsonValue>,
    path: &str,
    input: &str,
) -> Result<Arc<JsonValue>, JsonError> {
    let mut cloned = (**doc).clone();
    let new_value = sonic_rs::from_str::<JsonValue>(input)
        .map_err(|err| JsonError::ParseError(err.to_string()))?;
    path::replace_path(&mut cloned, path, new_value).map_err(|err| match err {
        JsonPathError::Missing(path) => JsonError::PathNotFound(path),
        JsonPathError::InvalidPath => JsonError::InvalidPath,
        JsonPathError::Unsupported => {
            JsonError::ParseError("unsupported JSONPath expression".into())
        }
    })?;
    Ok(Arc::new(cloned))
}

#[derive(Debug, Default, Clone, Copy)]
pub struct JsonModule;

impl SenkoModule for JsonModule {
    fn name(&self) -> &'static str {
        "ReJSON"
    }

    fn version(&self) -> u64 {
        20_600
    }

    fn register_commands(&self, registry: &mut CommandRegistry) {
        registry.register("JSON.SET", json_set);
        registry.register("JSON.GET", json_get);
        registry.register("JSON.MGET", json_mget);
        registry.register("JSON.MSET", json_mset);
        registry.register("JSON.DEL", json_del);
        registry.register("JSON.FORGET", json_del);
        registry.register("JSON.MERGE", json_merge);
        registry.register("JSON.TYPE", json_type);
        registry.register("JSON.TOGGLE", json_toggle);
        registry.register("JSON.CLEAR", json_clear);
        registry.register("JSON.NUMINCRBY", json_numincrby);
        registry.register("JSON.NUMMULTBY", json_nummultby);
        registry.register("JSON.STRLEN", json_strlen);
        registry.register("JSON.STRAPPEND", json_strappend);
        registry.register("JSON.ARRLEN", json_arrlen);
        registry.register("JSON.ARRAPPEND", json_arrappend);
        registry.register("JSON.ARRINSERT", json_arrinsert);
        registry.register("JSON.ARRPOP", json_arrpop);
        registry.register("JSON.ARRTRIM", json_arrtrim);
        registry.register("JSON.ARRINDEX", json_arrindex);
        registry.register("JSON.OBJLEN", json_objlen);
        registry.register("JSON.OBJKEYS", json_objkeys);
        registry.register("JSON.DEBUG", json_debug);
        registry.register("JSON.RESP", json_resp);
    }

    fn init_shard(&self, _shard: &mut ShardState) {}
}

fn json_set(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 || args.len() > 4 {
        return Err(err("ERR wrong number of arguments for 'json.set' command"));
    }
    let key = args[0];
    let path = as_utf8(args[1])?;
    let value = as_utf8(args[2])?;
    let mode = args.get(3).map(|raw| as_utf8(raw)).transpose()?;

    let existing = ctx.get_value(key);
    let doc = match &existing {
        Some(SenkoValue::Json(doc)) => Some(Arc::clone(doc)),
        Some(_) => return Err(JsonError::WrongType.into()),
        None => None,
    };

    if matches!(mode, Some("NX")) && existing.is_some() && is_root_path(path) {
        return Ok(ModuleResponse::Bulk(None));
    }
    if matches!(mode, Some("XX")) && existing.is_none() {
        return Ok(ModuleResponse::Bulk(None));
    }

    let new_doc = if is_root_path(path) {
        parse_document(value)?
    } else if let Some(doc) = doc {
        set_path(&doc, path, value)?
    } else {
        return Err(JsonError::PathNotFound(path.to_string()).into());
    };

    ctx.set_value(key, SenkoValue::Json(new_doc));
    Ok(ModuleResponse::Simple(b"OK"))
}

fn json_get(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() {
        return Err(err("ERR wrong number of arguments for 'json.get' command"));
    }
    let key = args[0];
    let Some(doc) = get_json(ctx, key)? else {
        return Ok(ModuleResponse::Bulk(None));
    };

    let mut pretty = false;
    let mut paths = Vec::new();
    let mut index = 1usize;
    while index < args.len() {
        let token = as_utf8(args[index])?;
        if matches!(token, "INDENT" | "NEWLINE" | "SPACE") {
            pretty = true;
            index += 2;
            continue;
        }
        paths.push(token.to_string());
        index += 1;
    }
    if paths.is_empty() {
        paths.push("$".to_string());
    }

    let value = if paths.len() == 1 {
        let matches = get_path(doc.as_ref(), &paths[0]).map_err(map_path_error)?;
        render_matches(&matches, pretty)
    } else {
        let mut obj = JsonValue::new_object();
        let obj_map = obj.as_object_mut().expect("object");
        for path in &paths {
            let matches = get_path(doc.as_ref(), path).map_err(map_path_error)?;
            let value = collect_matches(&matches);
            obj_map.insert(path, value);
        }
        render_json(&obj, pretty)
    };
    Ok(bulk(value))
}

fn json_mget(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err("ERR wrong number of arguments for 'json.mget' command"));
    }
    let path = as_utf8(args.last().copied().expect("path exists"))?;
    let mut out = SmallVec::new();
    for key in &args[..args.len() - 1] {
        match get_json(ctx, key)? {
            Some(doc) => {
                let matches = get_path(doc.as_ref(), path).map_err(map_path_error)?;
                out.push(bulk(render_matches(&matches, false)));
            }
            None => out.push(ModuleResponse::Bulk(None)),
        }
    }
    Ok(ModuleResponse::Array(Box::new(out)))
}

fn json_mset(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 || !args.len().is_multiple_of(3) {
        return Err(err("ERR wrong number of arguments for 'json.mset' command"));
    }

    let mut pending: Vec<(Vec<u8>, Arc<JsonValue>)> = Vec::with_capacity(args.len() / 3);
    for chunk in args.chunks(3) {
        let key = chunk[0];
        let path = as_utf8(chunk[1])?;
        let value = as_utf8(chunk[2])?;
        let existing = pending
            .iter()
            .rev()
            .find(|(pending_key, _)| pending_key.as_slice() == key)
            .map(|(_, value)| Arc::clone(value));
        let existing = match existing {
            Some(doc) => Some(doc),
            None => get_json(ctx, key)?,
        };
        let new_doc = if is_root_path(path) {
            parse_document(value)?
        } else if let Some(doc) = existing {
            set_path(&doc, path, value)?
        } else {
            return Err(JsonError::PathNotFound(path.to_string()).into());
        };
        pending.push((key.to_vec(), new_doc));
    }

    for (key, doc) in pending {
        ctx.set_value(&key, SenkoValue::Json(doc));
    }
    Ok(ModuleResponse::Simple(b"OK"))
}

fn json_del(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() || args.len() > 2 {
        return Err(err("ERR wrong number of arguments for 'json.del' command"));
    }
    let key = args[0];
    let path = args
        .get(1)
        .map(|raw| as_utf8(raw))
        .transpose()?
        .unwrap_or("$");
    if is_root_path(path) {
        return Ok(ModuleResponse::Integer(ctx.delete_key(key) as i64));
    }
    let Some(doc) = get_json(ctx, key)? else {
        return Ok(ModuleResponse::Integer(0));
    };
    let mut cloned = (*doc).clone();
    let removed = remove_path(&mut cloned, path)?;
    if removed > 0 {
        ctx.set_value(key, SenkoValue::Json(Arc::new(cloned)));
    }
    Ok(ModuleResponse::Integer(removed as i64))
}

fn json_merge(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 3 {
        return Err(err(
            "ERR wrong number of arguments for 'json.merge' command",
        ));
    }
    let key = args[0];
    let path = as_utf8(args[1])?;
    let patch = sonic_rs::from_str::<JsonValue>(as_utf8(args[2])?)
        .map_err(|error| JsonError::ParseError(error.to_string()))?;
    let Some(doc) = get_json(ctx, key)? else {
        return Err(JsonError::PathNotFound(path.to_string()).into());
    };
    let mut cloned = (*doc).clone();
    let target = locate_mut(&mut cloned, path)?;
    merge_patch(target, patch);
    ctx.set_value(key, SenkoValue::Json(Arc::new(cloned)));
    Ok(ModuleResponse::Simple(b"OK"))
}

fn json_type(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() || args.len() > 2 {
        return Err(err("ERR wrong number of arguments for 'json.type' command"));
    }
    let key = args[0];
    let path = args
        .get(1)
        .map(|raw| as_utf8(raw))
        .transpose()?
        .unwrap_or("$");
    let Some(doc) = get_json(ctx, key)? else {
        return Ok(ModuleResponse::Bulk(None));
    };
    let matches = get_path(doc.as_ref(), path).map_err(map_path_error)?;
    if matches.len() <= 1 {
        return Ok(matches
            .first()
            .map(|value| bulk(type_name(value)))
            .unwrap_or(ModuleResponse::Bulk(None)));
    }
    Ok(ModuleResponse::Array(Box::new(
        matches
            .into_iter()
            .map(|value| bulk(type_name(value)))
            .collect(),
    )))
}

fn json_toggle(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    mutate_scalar_array(ctx, args, "json.toggle", |value| {
        let current = value.as_bool().ok_or(JsonError::NotBoolean)?;
        *value = JsonValue::new_bool(!current);
        Ok(ModuleResponse::Integer(i64::from(!current)))
    })
}

fn json_clear(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() || args.len() > 2 {
        return Err(err(
            "ERR wrong number of arguments for 'json.clear' command",
        ));
    }
    let key = args[0];
    let path = args
        .get(1)
        .map(|raw| as_utf8(raw))
        .transpose()?
        .unwrap_or("$");
    let Some(doc) = get_json(ctx, key)? else {
        return Ok(ModuleResponse::Integer(0));
    };
    let mut cloned = (*doc).clone();
    let value = locate_mut(&mut cloned, path)?;
    let cleared = clear_value(value);
    ctx.set_value(key, SenkoValue::Json(Arc::new(cloned)));
    Ok(ModuleResponse::Integer(cleared))
}

fn json_numincrby(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    numeric_op(ctx, args, "json.numincrby", |current, rhs| current + rhs)
}

fn json_nummultby(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    numeric_op(ctx, args, "json.nummultby", |current, rhs| current * rhs)
}

fn json_strlen(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    read_len(ctx, args, "json.strlen", |value| {
        value.as_str().map(|text| text.chars().count() as i64)
    })
}

fn json_strappend(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 3 {
        return Err(err(
            "ERR wrong number of arguments for 'json.strappend' command",
        ));
    }
    let suffix = sonic_rs::from_str::<JsonValue>(as_utf8(args[2])?)
        .map_err(|error| JsonError::ParseError(error.to_string()))?;
    let suffix = suffix.as_str().ok_or(JsonError::NotString)?.to_string();
    mutate_scalar_array(ctx, &args[..2], "json.strappend", |value| {
        let current = value.as_str().ok_or(JsonError::NotString)?.to_string();
        let merged = format!("{current}{suffix}");
        *value = JsonValue::from(merged.as_str());
        Ok(ModuleResponse::Integer(
            (current.len() + suffix.len()) as i64,
        ))
    })
}

fn json_arrlen(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    read_len(ctx, args, "json.arrlen", |value| {
        value.as_array().map(|arr| arr.len() as i64)
    })
}

fn json_arrappend(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 {
        return Err(err(
            "ERR wrong number of arguments for 'json.arrappend' command",
        ));
    }
    let values = parse_values(&args[2..])?;
    mutate_scalar_array(ctx, &args[..2], "json.arrappend", move |value| {
        let arr = value.as_array_mut().ok_or(JsonError::NotArray)?;
        for item in &values {
            arr.push(item.clone());
        }
        Ok(ModuleResponse::Integer(arr.len() as i64))
    })
}

fn json_arrinsert(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 4 {
        return Err(err(
            "ERR wrong number of arguments for 'json.arrinsert' command",
        ));
    }
    let index = parse_i64(as_utf8(args[2])?)?;
    let values = parse_values(&args[3..])?;
    mutate_scalar_array(ctx, &args[..2], "json.arrinsert", move |value| {
        let arr = value.as_array_mut().ok_or(JsonError::NotArray)?;
        let mut idx = normalize_insert_index(arr.len(), index);
        for item in &values {
            arr.insert(idx, item.clone());
            idx += 1;
        }
        Ok(ModuleResponse::Integer(arr.len() as i64))
    })
}

fn json_arrpop(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() || args.len() > 3 {
        return Err(err(
            "ERR wrong number of arguments for 'json.arrpop' command",
        ));
    }
    let key = args[0];
    let path = args
        .get(1)
        .map(|raw| as_utf8(raw))
        .transpose()?
        .unwrap_or("$");
    let index = args
        .get(2)
        .map(|raw| as_utf8(raw).and_then(parse_i64))
        .transpose()?
        .unwrap_or(-1);
    let Some(doc) = get_json(ctx, key)? else {
        return Ok(ModuleResponse::Bulk(None));
    };
    let mut cloned = (*doc).clone();
    let arr = locate_mut(&mut cloned, path)?
        .as_array_mut()
        .ok_or(JsonError::NotArray)?;
    if arr.is_empty() {
        return Ok(ModuleResponse::Bulk(None));
    }
    let idx = normalize_existing_index(arr.len(), index as isize).ok_or(JsonError::NotArray)?;
    let popped = arr.get(idx).cloned().ok_or(JsonError::NotArray)?;
    arr.remove(idx);
    ctx.set_value(key, SenkoValue::Json(Arc::new(cloned)));
    Ok(bulk(render_json(&popped, false)))
}

fn json_arrtrim(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() != 4 {
        return Err(err(
            "ERR wrong number of arguments for 'json.arrtrim' command",
        ));
    }
    let start = parse_i64(as_utf8(args[2])?)?;
    let stop = parse_i64(as_utf8(args[3])?)?;
    mutate_scalar_array(ctx, &args[..2], "json.arrtrim", move |value| {
        let arr = value.as_array_mut().ok_or(JsonError::NotArray)?;
        let trimmed = trim_array(arr, start, stop);
        Ok(ModuleResponse::Integer(trimmed as i64))
    })
}

fn json_arrindex(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 3 || args.len() > 5 {
        return Err(err(
            "ERR wrong number of arguments for 'json.arrindex' command",
        ));
    }
    let needle = sonic_rs::from_str::<JsonValue>(as_utf8(args[2])?)
        .map_err(|error| JsonError::ParseError(error.to_string()))?;
    let start = args
        .get(3)
        .map(|raw| as_utf8(raw).and_then(parse_i64))
        .transpose()?
        .unwrap_or(0);
    let stop = args
        .get(4)
        .map(|raw| as_utf8(raw).and_then(parse_i64))
        .transpose()?
        .unwrap_or(i64::MAX);
    let path = args
        .get(1)
        .map(|raw| as_utf8(raw))
        .transpose()?
        .unwrap_or("$");
    let Some(doc) = get_json(ctx, args[0])? else {
        return Ok(ModuleResponse::Integer(-1));
    };
    let matches = get_path(doc.as_ref(), path).map_err(map_path_error)?;
    let Some(value) = matches.first().copied() else {
        return Ok(ModuleResponse::Integer(-1));
    };
    let arr = value.as_array().ok_or(JsonError::NotArray)?;
    let start = normalize_search_bound(arr.len(), start);
    let stop = normalize_search_bound(arr.len(), stop).min(arr.len().saturating_sub(1));
    for idx in start..=stop {
        if arr.get(idx).is_some_and(|item| item == &needle) {
            return Ok(ModuleResponse::Integer(idx as i64));
        }
    }
    Ok(ModuleResponse::Integer(-1))
}

fn json_objlen(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    read_len(ctx, args, "json.objlen", |value| {
        value.as_object().map(|obj| obj.len() as i64)
    })
}

fn json_objkeys(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() || args.len() > 2 {
        return Err(err(
            "ERR wrong number of arguments for 'json.objkeys' command",
        ));
    }
    let path = args
        .get(1)
        .map(|raw| as_utf8(raw))
        .transpose()?
        .unwrap_or("$");
    let Some(doc) = get_json(ctx, args[0])? else {
        return Ok(ModuleResponse::Bulk(None));
    };
    let matches = get_path(doc.as_ref(), path).map_err(map_path_error)?;
    let Some(value) = matches.first().copied() else {
        return Ok(ModuleResponse::Bulk(None));
    };
    let obj = value.as_object().ok_or(JsonError::NotObject)?;
    Ok(ModuleResponse::Array(Box::new(
        obj.iter().map(|(key, _)| bulk(key.as_bytes())).collect(),
    )))
}

fn json_debug(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.len() < 2 {
        return Err(err(
            "ERR wrong number of arguments for 'json.debug' command",
        ));
    }
    if !as_utf8(args[0])?.eq_ignore_ascii_case("MEMORY") {
        return Err(err("ERR unknown subcommand for JSON.DEBUG"));
    }
    let path = args
        .get(2)
        .map(|raw| as_utf8(raw))
        .transpose()?
        .unwrap_or("$");
    let Some(doc) = get_json(ctx, args[1])? else {
        return Ok(ModuleResponse::Bulk(None));
    };
    let matches = get_path(doc.as_ref(), path).map_err(map_path_error)?;
    let Some(value) = matches.first().copied() else {
        return Ok(ModuleResponse::Bulk(None));
    };
    Ok(ModuleResponse::Integer(memory_usage(value) as i64))
}

fn json_resp(ctx: &mut dyn ModuleCommandContext, args: &[&[u8]]) -> ModuleResult {
    if args.is_empty() || args.len() > 2 {
        return Err(err("ERR wrong number of arguments for 'json.resp' command"));
    }
    let path = args
        .get(1)
        .map(|raw| as_utf8(raw))
        .transpose()?
        .unwrap_or("$");
    let Some(doc) = get_json(ctx, args[0])? else {
        return Ok(ModuleResponse::Bulk(None));
    };
    let matches = get_path(doc.as_ref(), path).map_err(map_path_error)?;
    let Some(value) = matches.first().copied() else {
        return Ok(ModuleResponse::Bulk(None));
    };
    Ok(to_resp(value))
}

fn numeric_op(
    ctx: &mut dyn ModuleCommandContext,
    args: &[&[u8]],
    command: &'static str,
    op: fn(f64, f64) -> f64,
) -> ModuleResult {
    if args.len() != 3 {
        return Err(err_wrong_arity(command));
    }
    let rhs = as_utf8(args[2])?
        .parse::<f64>()
        .map_err(|_| JsonError::NotNumber)?;
    mutate_scalar_array(ctx, &args[..2], command, move |value| {
        let current = value.as_f64().ok_or(JsonError::NotNumber)?;
        let updated = op(current, rhs);
        *value = JsonValue::new_f64(updated).ok_or(JsonError::NotNumber)?;
        Ok(bulk(updated.to_string()))
    })
}

fn read_len(
    ctx: &mut dyn ModuleCommandContext,
    args: &[&[u8]],
    command: &'static str,
    extractor: fn(&JsonValue) -> Option<i64>,
) -> ModuleResult {
    if args.is_empty() || args.len() > 2 {
        return Err(err_wrong_arity(command));
    }
    let path = args
        .get(1)
        .map(|raw| as_utf8(raw))
        .transpose()?
        .unwrap_or("$");
    let Some(doc) = get_json(ctx, args[0])? else {
        return Ok(ModuleResponse::Bulk(None));
    };
    let matches = get_path(doc.as_ref(), path).unwrap_or_default();
    if matches.len() <= 1 {
        return Ok(matches
            .first()
            .and_then(|value| extractor(value))
            .map(ModuleResponse::Integer)
            .unwrap_or(ModuleResponse::Bulk(None)));
    }
    Ok(ModuleResponse::Array(Box::new(
        matches
            .into_iter()
            .map(|value| {
                extractor(value)
                    .map(ModuleResponse::Integer)
                    .unwrap_or(ModuleResponse::Bulk(None))
            })
            .collect(),
    )))
}

fn mutate_scalar_array(
    ctx: &mut dyn ModuleCommandContext,
    args: &[&[u8]],
    command: &'static str,
    op: impl Fn(&mut JsonValue) -> Result<ModuleResponse, JsonError>,
) -> ModuleResult {
    if args.len() != 2 {
        return Err(err_wrong_arity(command));
    }
    let Some(doc) = get_json(ctx, args[0])? else {
        return Ok(ModuleResponse::Bulk(None));
    };
    let path = as_utf8(args[1])?;
    let mut cloned = (*doc).clone();
    let value = locate_mut(&mut cloned, path)?;
    let result = op(value)?;
    ctx.set_value(args[0], SenkoValue::Json(Arc::new(cloned)));
    Ok(result)
}

fn get_json(
    ctx: &mut dyn ModuleCommandContext,
    key: &[u8],
) -> Result<Option<Arc<JsonValue>>, ModuleError> {
    match ctx.get_value(key) {
        Some(SenkoValue::Json(doc)) => Ok(Some(doc)),
        Some(_) => Err(JsonError::WrongType.into()),
        None => Ok(None),
    }
}

fn locate_mut<'a>(root: &'a mut JsonValue, path: &str) -> Result<&'a mut JsonValue, ModuleError> {
    let normalized = normalize_path(path).map_err(map_path_error)?;
    if normalized == "$" {
        return Ok(root);
    }
    let tokens = parse_path(&normalized).map_err(map_path_error)?;
    let mut current = root;
    for token in tokens {
        match token {
            JsonPathToken::Key(key) => {
                current = current
                    .as_object_mut()
                    .and_then(|obj| obj.get_mut(&key))
                    .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;
            }
            JsonPathToken::Index(index) => {
                current = current
                    .as_array_mut()
                    .and_then(|arr| resolve_index_mut(arr, index))
                    .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;
            }
            JsonPathToken::Wildcard => {
                return Err(JsonError::ParseError("unsupported JSONPath expression".into()).into());
            }
        }
    }
    Ok(current)
}

fn remove_path(root: &mut JsonValue, path: &str) -> Result<usize, ModuleError> {
    let normalized = normalize_path(path).map_err(map_path_error)?;
    let tokens = parse_path(&normalized).map_err(map_path_error)?;
    if tokens.is_empty() {
        return Ok(0);
    }
    let mut current = root;
    for token in &tokens[..tokens.len() - 1] {
        current = match token {
            JsonPathToken::Key(key) => current
                .as_object_mut()
                .and_then(|obj| obj.get_mut(key))
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?,
            JsonPathToken::Index(index) => current
                .as_array_mut()
                .and_then(|arr| resolve_index_mut(arr, *index))
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?,
            JsonPathToken::Wildcard => {
                return Err(JsonError::ParseError("unsupported JSONPath expression".into()).into());
            }
        };
    }
    match tokens.last().expect("tokens exist") {
        JsonPathToken::Key(key) => Ok(current
            .as_object_mut()
            .and_then(|obj| obj.remove(key))
            .map(|_| 1)
            .unwrap_or(0)),
        JsonPathToken::Index(index) => {
            let Some(arr) = current.as_array_mut() else {
                return Ok(0);
            };
            let Some(idx) = normalize_existing_index(arr.len(), *index) else {
                return Ok(0);
            };
            arr.remove(idx);
            Ok(1)
        }
        JsonPathToken::Wildcard => {
            Err(JsonError::ParseError("unsupported JSONPath expression".into()).into())
        }
    }
}

fn resolve_index_mut(arr: &mut Array, index: isize) -> Option<&mut JsonValue> {
    let len = arr.len() as isize;
    let idx = if index < 0 { len + index } else { index };
    if idx < 0 {
        None
    } else {
        arr.get_mut(idx as usize)
    }
}

fn clear_value(value: &mut JsonValue) -> i64 {
    if let Some(arr) = value.as_array_mut() {
        if arr.is_empty() {
            return 0;
        }
        arr.clear();
        return 1;
    }
    if let Some(obj) = value.as_object_mut() {
        if obj.is_empty() {
            return 0;
        }
        obj.clear();
        return 1;
    }
    if value.is_number() {
        *value = JsonValue::from(0);
        return 1;
    }
    0
}

fn merge_patch(target: &mut JsonValue, patch: JsonValue) {
    if let Some(patch_obj) = patch.as_object() {
        if !target.is_object() {
            *target = JsonValue::new_object();
        }
        let Some(target_obj) = target.as_object_mut() else {
            return;
        };
        for (key, value) in patch_obj.iter() {
            if value.is_null() {
                let _ = target_obj.remove(&key);
            } else if let Some(existing) = target_obj.get_mut(&key) {
                merge_patch(existing, value.clone());
            } else {
                target_obj.insert(&key, value.clone());
            }
        }
    } else {
        *target = patch;
    }
}

fn memory_usage(value: &JsonValue) -> usize {
    std::mem::size_of_val(value)
        + value
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| k.len() + memory_usage(v))
                    .sum::<usize>()
            })
            .unwrap_or(0)
        + value
            .as_array()
            .map(|arr| arr.iter().map(memory_usage).sum::<usize>())
            .unwrap_or(0)
        + value.as_str().map(str::len).unwrap_or(0)
}

fn to_resp(value: &JsonValue) -> ModuleResponse {
    if value.is_null() {
        return ModuleResponse::Bulk(None);
    }
    if let Some(boolean) = value.as_bool() {
        return bulk(if boolean { "true" } else { "false" });
    }
    if let Some(number) = value.as_i64() {
        return ModuleResponse::Integer(number);
    }
    if let Some(number) = value.as_f64() {
        return bulk(number.to_string());
    }
    if let Some(text) = value.as_str() {
        return bulk(text);
    }
    if let Some(arr) = value.as_array() {
        return ModuleResponse::Array(Box::new(arr.iter().map(to_resp).collect()));
    }
    if let Some(obj) = value.as_object() {
        let mut out = SmallVec::new();
        for (key, value) in obj.iter() {
            out.push(bulk(key.as_bytes()));
            out.push(to_resp(value));
        }
        return ModuleResponse::Array(Box::new(out));
    }
    ModuleResponse::Bulk(None)
}

fn parse_values(args: &[&[u8]]) -> Result<Vec<JsonValue>, ModuleError> {
    args.iter()
        .map(|raw| {
            sonic_rs::from_str::<JsonValue>(as_utf8(raw)?)
                .map_err(|error| JsonError::ParseError(error.to_string()).into())
        })
        .collect()
}

fn render_matches(matches: &[&JsonValue], pretty: bool) -> String {
    let value = collect_matches(matches);
    render_json(&value, pretty)
}

fn collect_matches(matches: &[&JsonValue]) -> JsonValue {
    if matches.len() == 1 {
        matches[0].clone()
    } else {
        {
            let mut array = JsonValue::new_array_with(matches.len());
            let arr = array.as_array_mut().expect("array");
            for value in matches {
                arr.push((*value).clone());
            }
            array
        }
    }
}

fn render_json(value: &JsonValue, pretty: bool) -> String {
    if pretty {
        sonic_rs::to_string_pretty(value).unwrap_or_else(|_| "null".to_string())
    } else {
        sonic_rs::to_string(value).unwrap_or_else(|_| "null".to_string())
    }
}

fn bulk(value: impl AsRef<[u8]>) -> ModuleResponse {
    ModuleResponse::Bulk(Some(Bytes::copy_from_slice(value.as_ref())))
}

fn as_utf8(bytes: &[u8]) -> Result<&str, ModuleError> {
    std::str::from_utf8(bytes)
        .map_err(|_| JsonError::ParseError("value is not valid UTF-8".into()).into())
}

fn parse_i64(value: &str) -> Result<i64, ModuleError> {
    value
        .parse::<i64>()
        .map_err(|_| JsonError::ParseError("invalid integer".into()).into())
}

fn is_root_path(path: &str) -> bool {
    matches!(path, "$" | ".")
}

fn normalize_insert_index(len: usize, index: i64) -> usize {
    if index < 0 {
        len.saturating_sub(index.unsigned_abs() as usize)
    } else {
        (index as usize).min(len)
    }
}

fn normalize_existing_index(len: usize, index: isize) -> Option<usize> {
    let len = len as isize;
    let idx = if index < 0 { len + index } else { index };
    (idx >= 0 && idx < len).then_some(idx as usize)
}

fn normalize_search_bound(len: usize, index: i64) -> usize {
    if len == 0 {
        return 0;
    }
    if index < 0 {
        len.saturating_sub(index.unsigned_abs() as usize)
    } else {
        (index as usize).min(len - 1)
    }
}

fn trim_array(arr: &mut Array, start: i64, stop: i64) -> usize {
    if arr.is_empty() {
        return 0;
    }
    let len = arr.len();
    let start = normalize_search_bound(len, start);
    let stop = normalize_search_bound(len, stop);
    if start > stop {
        arr.clear();
        return 0;
    }
    let kept: Vec<_> = arr
        .iter()
        .skip(start)
        .take(stop - start + 1)
        .cloned()
        .collect();
    arr.clear();
    for value in kept {
        arr.push(value);
    }
    arr.len()
}

fn map_path_error(error: JsonPathError) -> ModuleError {
    match error {
        JsonPathError::Missing(path) => JsonError::PathNotFound(path).into(),
        JsonPathError::InvalidPath => JsonError::InvalidPath.into(),
        JsonPathError::Unsupported => {
            JsonError::ParseError("unsupported JSONPath expression".into()).into()
        }
    }
}

fn err(message: &'static str) -> ModuleError {
    ModuleError::new(message)
}

fn err_wrong_arity(command: &'static str) -> ModuleError {
    ModuleError::new(format!(
        "ERR wrong number of arguments for '{command}' command"
    ))
}

impl From<JsonError> for ModuleError {
    fn from(value: JsonError) -> Self {
        ModuleError::new(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct TestContext {
        values: BTreeMap<Vec<u8>, SenkoValue>,
    }

    impl ModuleCommandContext for TestContext {
        fn shard_id(&self) -> usize {
            0
        }

        fn shard_extensions(&self) -> &senko_core::ShardExtensions {
            static EXTENSIONS: std::sync::OnceLock<Arc<senko_core::ShardExtensions>> =
                std::sync::OnceLock::new();
            EXTENSIONS.get_or_init(|| Arc::new(senko_core::ShardExtensions::default()))
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

    fn response_bulk(response: ModuleResponse) -> Option<Vec<u8>> {
        match response {
            ModuleResponse::Bulk(value) => value.map(|value| value.to_vec()),
            other => panic!("expected bulk response, got {other:?}"),
        }
    }

    fn response_int(response: ModuleResponse) -> i64 {
        match response {
            ModuleResponse::Integer(value) => value,
            other => panic!("expected integer response, got {other:?}"),
        }
    }

    #[test]
    fn json_get_rejects_invalid_path() {
        let mut ctx = TestContext::default();
        let set = json_set(&mut ctx, &[b"k", b"$", br#"{"a":1}"#]);
        assert_eq!(set, Ok(ModuleResponse::Simple(b"OK")));

        let err = json_get(&mut ctx, &[b"k", b"["]).expect_err("invalid path should error");
        assert_eq!(
            err.message(),
            "ERR Path must be either absolute JSONPath (starts with '$') or legacy path"
        );
    }

    #[test]
    fn json_mget_rejects_unsupported_path() {
        let mut ctx = TestContext::default();
        let set = json_set(&mut ctx, &[b"k", b"$", br#"{"a":[1]}"#]);
        assert_eq!(set, Ok(ModuleResponse::Simple(b"OK")));

        let err =
            json_mget(&mut ctx, &[b"k", b"$[abc]"]).expect_err("unsupported path should error");
        assert_eq!(err.message(), "ERR unsupported JSONPath expression");
    }

    #[test]
    fn json_mset_is_atomic() {
        let mut ctx = TestContext::default();
        let set = json_set(&mut ctx, &[b"doc", b"$", br#"{"a":1}"#]);
        assert_eq!(set, Ok(ModuleResponse::Simple(b"OK")));

        let err = json_mset(
            &mut ctx,
            &[b"doc", b"$.a", b"2", b"missing", b"$.nested", b"3"],
        )
        .expect_err("mset should fail when one path is invalid");
        assert_eq!(err.message(), "ERR Path '$.nested' does not exist");

        let current = json_get(&mut ctx, &[b"doc", b"$"]).expect("json.get should succeed");
        assert_eq!(response_bulk(current), Some(br#"{"a":1}"#.to_vec()));
        assert!(ctx.get_value(b"missing").is_none());
    }

    #[test]
    fn json_arrindex_returns_expected_offset() {
        let mut ctx = TestContext::default();
        let set = json_set(&mut ctx, &[b"arr", b"$", br#"[1,2,3,2]"#]);
        assert_eq!(set, Ok(ModuleResponse::Simple(b"OK")));

        let response =
            json_arrindex(&mut ctx, &[b"arr", b"$", b"2", b"1", b"3"]).expect("arrindex works");
        assert_eq!(response_int(response), 1);
    }
}
