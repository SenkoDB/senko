use smallvec::SmallVec;
use sonic_rs::{Array, JsonContainerTrait, JsonValueMutTrait, Value};

use crate::error::JsonPathError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonPathToken {
    Key(String),
    Index(isize),
    Wildcard,
}

pub fn normalize_path(path: &str) -> Result<String, JsonPathError> {
    if path.is_empty() {
        return Err(JsonPathError::InvalidPath);
    }
    if path == "." {
        return Ok("$".to_string());
    }
    if path.starts_with('$') {
        return Ok(path.to_string());
    }
    if !path.starts_with('.') {
        return Err(JsonPathError::InvalidPath);
    }
    Ok(format!("${path}"))
}

pub fn parse_path(path: &str) -> Result<SmallVec<[JsonPathToken; 8]>, JsonPathError> {
    let normalized = normalize_path(path)?;
    let bytes = normalized.as_bytes();
    let mut i = 1usize;
    let mut out = SmallVec::new();
    while i < bytes.len() {
        match bytes[i] {
            b'.' => {
                i += 1;
                if i < bytes.len() && bytes[i] == b'*' {
                    out.push(JsonPathToken::Wildcard);
                    i += 1;
                    continue;
                }
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'-')
                {
                    i += 1;
                }
                if start == i {
                    return Err(JsonPathError::InvalidPath);
                }
                out.push(JsonPathToken::Key(normalized[start..i].to_string()));
            }
            b'[' => {
                i += 1;
                if i >= bytes.len() {
                    return Err(JsonPathError::InvalidPath);
                }
                if bytes[i] == b'*' {
                    i += 1;
                    if i >= bytes.len() || bytes[i] != b']' {
                        return Err(JsonPathError::InvalidPath);
                    }
                    out.push(JsonPathToken::Wildcard);
                    i += 1;
                    continue;
                }
                if bytes[i] == b'\'' || bytes[i] == b'"' {
                    let quote = bytes[i];
                    i += 1;
                    let start = i;
                    while i < bytes.len() && bytes[i] != quote {
                        i += 1;
                    }
                    if i >= bytes.len() {
                        return Err(JsonPathError::InvalidPath);
                    }
                    let key = normalized[start..i].to_string();
                    i += 1;
                    if i >= bytes.len() || bytes[i] != b']' {
                        return Err(JsonPathError::InvalidPath);
                    }
                    i += 1;
                    out.push(JsonPathToken::Key(key));
                    continue;
                }
                let start = i;
                while i < bytes.len() && bytes[i] != b']' {
                    i += 1;
                }
                if i >= bytes.len() {
                    return Err(JsonPathError::InvalidPath);
                }
                let raw = normalized[start..i].trim();
                i += 1;
                let index = raw
                    .parse::<isize>()
                    .map_err(|_| JsonPathError::Unsupported)?;
                out.push(JsonPathToken::Index(index));
            }
            _ => return Err(JsonPathError::Unsupported),
        }
    }
    Ok(out)
}

pub fn eval_read<'a>(root: &'a Value, path: &str) -> Result<Vec<&'a Value>, JsonPathError> {
    let tokens = parse_path(path)?;
    let mut current = vec![root];
    for token in tokens {
        let mut next = Vec::new();
        for value in current {
            match token {
                JsonPathToken::Key(ref key) => {
                    if let Some(obj) = value.as_object()
                        && let Some(found) = obj.get(key)
                    {
                        next.push(found);
                    }
                }
                JsonPathToken::Index(index) => {
                    if let Some(arr) = value.as_array() {
                        let len = arr.len() as isize;
                        let idx = if index < 0 { len + index } else { index };
                        if idx >= 0
                            && let Some(found) = arr.get(idx as usize)
                        {
                            next.push(found);
                        }
                    }
                }
                JsonPathToken::Wildcard => {
                    if let Some(arr) = value.as_array() {
                        next.extend(arr.iter());
                    } else if let Some(obj) = value.as_object() {
                        next.extend(obj.iter().map(|(_, value)| value));
                    }
                }
            }
        }
        current = next;
    }
    Ok(current)
}

pub fn replace_path(root: &mut Value, path: &str, replacement: Value) -> Result<(), JsonPathError> {
    let normalized = normalize_path(path)?;
    if normalized == "$" {
        *root = replacement;
        return Ok(());
    }
    let tokens = parse_path(&normalized)?;
    if tokens.is_empty() {
        *root = replacement;
        return Ok(());
    }
    let mut current = root;
    for token in &tokens[..tokens.len() - 1] {
        match token {
            JsonPathToken::Key(key) => {
                current = current
                    .as_object_mut()
                    .and_then(|obj| obj.get_mut(key))
                    .ok_or_else(|| JsonPathError::Missing(normalized.clone()))?;
            }
            JsonPathToken::Index(index) => {
                current = current
                    .as_array_mut()
                    .and_then(|arr| resolve_index_mut(arr, *index))
                    .ok_or_else(|| JsonPathError::Missing(normalized.clone()))?;
            }
            JsonPathToken::Wildcard => return Err(JsonPathError::Unsupported),
        }
    }
    match tokens.last().expect("non-empty tokens") {
        JsonPathToken::Key(key) => {
            let obj = current
                .as_object_mut()
                .ok_or_else(|| JsonPathError::Missing(normalized.clone()))?;
            obj.insert(key.as_str(), replacement);
            Ok(())
        }
        JsonPathToken::Index(index) => {
            let slot = current
                .as_array_mut()
                .and_then(|arr| resolve_index_mut(arr, *index))
                .ok_or_else(|| JsonPathError::Missing(normalized.clone()))?;
            *slot = replacement;
            Ok(())
        }
        JsonPathToken::Wildcard => Err(JsonPathError::Unsupported),
    }
}

fn resolve_index_mut(arr: &mut Array, index: isize) -> Option<&mut Value> {
    let len = arr.len() as isize;
    let idx = if index < 0 { len + index } else { index };
    if idx < 0 {
        return None;
    }
    arr.get_mut(idx as usize)
}
