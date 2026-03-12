use std::time::{SystemTime, UNIX_EPOCH};

use ahash::RandomState;
use bitflags::bitflags;
use blake3::Hasher;
use bytes::Bytes;
use hashbrown::HashMap;
use mlua::{Function as LuaFunction, Lua, RegistryKey};

use crate::error::LuaError;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FunctionFlags: u8 {
        const NO_WRITES = 1 << 0;
        const ALLOW_OOM = 1 << 1;
        const ALLOW_REPLICATION = 1 << 2;
    }
}

pub struct FunctionDefinition {
    pub name: String,
    pub description: Option<String>,
    pub flags: FunctionFlags,
    pub callback: LuaFunction,
}

pub struct FunctionEntry {
    pub name: String,
    pub description: Option<String>,
    pub flags: FunctionFlags,
    pub chunk_key: RegistryKey,
}

pub struct Library {
    pub name: String,
    pub engine: String,
    pub code: Bytes,
    pub functions: HashMap<String, FunctionEntry, RandomState>,
    pub loaded_at: u64,
    pub fingerprint: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub name: String,
    pub description: Option<String>,
    pub flags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LibraryInfo {
    pub library_name: String,
    pub engine: String,
    pub functions: Vec<FunctionInfo>,
    pub library_code: Option<Bytes>,
}

pub struct FunctionRegistry {
    libraries: HashMap<String, Library, RandomState>,
    function_index: HashMap<String, String, RandomState>,
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self {
            libraries: HashMap::with_hasher(RandomState::new()),
            function_index: HashMap::with_hasher(RandomState::new()),
        }
    }
}

impl FunctionRegistry {
    pub fn load_library(
        &mut self,
        lua: &Lua,
        library_name: &str,
        code: &str,
        definitions: Vec<FunctionDefinition>,
        replace: bool,
    ) -> Result<(), LuaError> {
        validate_name(library_name, "library")?;
        if definitions.is_empty() {
            return Err(LuaError::redis_error("No functions registered"));
        }
        if self.libraries.contains_key(library_name) && !replace {
            return Err(LuaError::LibraryExists(library_name.to_owned()));
        }

        let mut functions = HashMap::with_hasher(RandomState::new());
        for definition in definitions {
            validate_name(&definition.name, "function")?;
            if let Some(existing_lib) = self.function_index.get(&definition.name)
                && existing_lib != library_name
                && !(replace && existing_lib == library_name)
            {
                return Err(LuaError::redis_error(format!(
                    "Function {} already exists",
                    definition.name
                )));
            }
            let chunk_key = lua.create_registry_value(definition.callback)?;
            functions.insert(
                definition.name.clone(),
                FunctionEntry {
                    name: definition.name,
                    description: definition.description,
                    flags: definition.flags,
                    chunk_key,
                },
            );
        }

        if replace && let Some(existing) = self.libraries.remove(library_name) {
            for function in existing.functions.into_values() {
                let _ = lua.remove_registry_value(function.chunk_key);
            }
            self.function_index
                .retain(|_, owner| owner.as_str() != library_name);
        }

        let mut hasher = Hasher::new();
        hasher.update(code.as_bytes());
        let fingerprint = *hasher.finalize().as_bytes();
        for function_name in functions.keys() {
            self.function_index
                .insert(function_name.clone(), library_name.to_owned());
        }
        self.libraries.insert(
            library_name.to_owned(),
            Library {
                name: library_name.to_owned(),
                engine: "LUA".to_owned(),
                code: Bytes::copy_from_slice(code.as_bytes()),
                functions,
                loaded_at: unix_ms(),
                fingerprint,
            },
        );
        Ok(())
    }

    pub fn get_function(
        &self,
        lua: &Lua,
        function_name: &str,
    ) -> Result<Option<(String, String, FunctionFlags, LuaFunction)>, LuaError> {
        let (library_name, function_name) = resolve_function_name(self, function_name)?;
        let Some(library) = self.libraries.get(library_name.as_str()) else {
            return Ok(None);
        };
        let Some(function) = library.functions.get(function_name.as_str()) else {
            return Ok(None);
        };
        Ok(Some((
            library.name.clone(),
            function.name.clone(),
            function.flags,
            lua.registry_value(&function.chunk_key)?,
        )))
    }

    pub fn list(&self, pattern: Option<&[u8]>, with_code: bool) -> Vec<LibraryInfo> {
        let mut libraries = self
            .libraries
            .values()
            .filter(|library| {
                pattern.is_none_or(|pattern| {
                    senko_store::pattern::glob_match(pattern, library.name.as_bytes())
                })
            })
            .map(|library| LibraryInfo {
                library_name: library.name.clone(),
                engine: library.engine.clone(),
                functions: library
                    .functions
                    .values()
                    .map(|function| FunctionInfo {
                        name: function.name.clone(),
                        description: function.description.clone(),
                        flags: flag_names(function.flags),
                    })
                    .collect(),
                library_code: with_code.then(|| library.code.clone()),
            })
            .collect::<Vec<_>>();
        libraries.sort_by(|left, right| left.library_name.cmp(&right.library_name));
        libraries
    }

    pub fn delete(&mut self, lua: &Lua, library_name: &str) -> Result<(), LuaError> {
        let Some(library) = self.libraries.remove(library_name) else {
            return Err(LuaError::LibraryNotFound);
        };
        for function in library.functions.into_values() {
            lua.remove_registry_value(function.chunk_key)?;
        }
        self.function_index
            .retain(|_, owner| owner.as_str() != library_name);
        Ok(())
    }

    pub fn flush(&mut self, lua: &Lua) -> Result<(), LuaError> {
        let libraries = std::mem::take(&mut self.libraries);
        self.function_index.clear();
        for library in libraries.into_values() {
            for function in library.functions.into_values() {
                lua.remove_registry_value(function.chunk_key)?;
            }
        }
        Ok(())
    }

    pub fn dump(&self) -> Bytes {
        let mut out = Vec::new();
        out.extend_from_slice(b"SENKOFN1");
        out.extend_from_slice(&(self.libraries.len() as u32).to_le_bytes());
        let mut libraries = self.libraries.values().collect::<Vec<_>>();
        libraries.sort_by(|left, right| left.name.cmp(&right.name));
        for library in libraries {
            write_len_prefixed(&mut out, library.name.as_bytes());
            write_len_prefixed(&mut out, library.engine.as_bytes());
            write_len_prefixed(&mut out, &library.code);
        }
        Bytes::from(out)
    }

    pub fn restore<F>(
        &mut self,
        lua: &Lua,
        payload: &[u8],
        mut loader: F,
        mode: RestoreMode,
    ) -> Result<(), LuaError>
    where
        F: FnMut(&Lua, &str, &str, bool) -> Result<Vec<FunctionDefinition>, LuaError>,
    {
        let mut cursor = payload;
        if cursor.len() < 12 || &cursor[..8] != b"SENKOFN1" {
            return Err(LuaError::redis_error("Invalid function dump payload"));
        }
        cursor = &cursor[8..];
        let count = take_u32(&mut cursor)? as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let name = String::from_utf8(read_len_prefixed(&mut cursor)?.to_vec())?;
            let engine = String::from_utf8(read_len_prefixed(&mut cursor)?.to_vec())?;
            if !engine.eq_ignore_ascii_case("lua") {
                return Err(LuaError::redis_error("Unsupported function engine"));
            }
            let code = String::from_utf8(read_len_prefixed(&mut cursor)?.to_vec())?;
            entries.push((name, code));
        }

        match mode {
            RestoreMode::Flush => self.flush(lua)?,
            RestoreMode::Append => {}
            RestoreMode::Replace => {}
        }

        for (name, code) in entries {
            if matches!(mode, RestoreMode::Append) && self.libraries.contains_key(name.as_str()) {
                return Err(LuaError::LibraryExists(name));
            }
            let replace = matches!(mode, RestoreMode::Replace);
            let definitions = loader(lua, name.as_str(), code.as_str(), replace)?;
            self.load_library(lua, name.as_str(), code.as_str(), definitions, replace)?;
        }
        Ok(())
    }

    pub fn library_count(&self) -> usize {
        self.libraries.len()
    }

    pub fn function_count(&self) -> usize {
        self.function_index.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreMode {
    Flush,
    Append,
    Replace,
}

pub fn parse_shebang(source: &str) -> Result<(String, String), LuaError> {
    let Some((first_line, rest)) = source.split_once('\n') else {
        return Err(LuaError::redis_error("Missing library metadata header"));
    };
    if !first_line.starts_with("#!") {
        return Err(LuaError::redis_error("Missing library metadata header"));
    }
    let mut engine = None;
    let mut name = None;
    for part in first_line[2..].split_whitespace() {
        if engine.is_none() {
            engine = Some(part.to_ascii_lowercase());
            continue;
        }
        if let Some(value) = part.strip_prefix("name=") {
            name = Some(value.to_owned());
        }
    }
    let engine = engine.ok_or_else(|| LuaError::redis_error("Missing library engine"))?;
    if engine != "lua" {
        return Err(LuaError::redis_error("Only LUA engine is supported"));
    }
    let name = name.ok_or_else(|| LuaError::redis_error("Missing library name"))?;
    validate_name(&name, "library")?;
    Ok((name, rest.to_owned()))
}

pub fn parse_flag_names(flags: &[String]) -> Result<FunctionFlags, LuaError> {
    let mut out = FunctionFlags::empty();
    for flag in flags {
        match flag.as_str() {
            "no-writes" => out |= FunctionFlags::NO_WRITES,
            "allow-oom" => out |= FunctionFlags::ALLOW_OOM,
            "allow-replication" => out |= FunctionFlags::ALLOW_REPLICATION,
            _ => {
                return Err(LuaError::redis_error(format!(
                    "Unknown function flag '{flag}'"
                )));
            }
        }
    }
    Ok(out)
}

fn resolve_function_name<'a>(
    registry: &'a FunctionRegistry,
    function_name: &'a str,
) -> Result<(String, String), LuaError> {
    if let Some((library, function)) = function_name.split_once('.') {
        return Ok((library.to_owned(), function.to_owned()));
    }
    let Some(library) = registry.function_index.get(function_name) else {
        return Err(LuaError::FunctionNotFound);
    };
    Ok((library.clone(), function_name.to_owned()))
}

fn flag_names(flags: FunctionFlags) -> Vec<String> {
    let mut out = Vec::new();
    if flags.contains(FunctionFlags::NO_WRITES) {
        out.push("no-writes".to_owned());
    }
    if flags.contains(FunctionFlags::ALLOW_OOM) {
        out.push("allow-oom".to_owned());
    }
    if flags.contains(FunctionFlags::ALLOW_REPLICATION) {
        out.push("allow-replication".to_owned());
    }
    out
}

fn validate_name(name: &str, kind: &str) -> Result<(), LuaError> {
    if name.is_empty()
        || name.len() > 64
        || name.contains(char::is_whitespace)
        || !name.is_ascii()
        || !name.chars().all(|ch| (' '..='~').contains(&ch))
    {
        return Err(LuaError::redis_error(format!("Invalid {kind} name")));
    }
    Ok(())
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| value.as_millis() as u64)
}

fn write_len_prefixed(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

fn take_u32(cursor: &mut &[u8]) -> Result<u32, LuaError> {
    if cursor.len() < 4 {
        return Err(LuaError::redis_error("Invalid function dump payload"));
    }
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&cursor[..4]);
    *cursor = &cursor[4..];
    Ok(u32::from_le_bytes(bytes))
}

fn read_len_prefixed<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8], LuaError> {
    let len = take_u32(cursor)? as usize;
    if cursor.len() < len {
        return Err(LuaError::redis_error("Invalid function dump payload"));
    }
    let (head, tail) = cursor.split_at(len);
    *cursor = tail;
    Ok(head)
}
