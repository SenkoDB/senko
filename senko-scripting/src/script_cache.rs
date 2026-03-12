use std::time::{SystemTime, UNIX_EPOCH};

use ahash::RandomState;
use bytes::Bytes;
use hashbrown::HashMap;
use mlua::{Function, Lua, RegistryKey};
use sha1::{Digest, Sha1};

use crate::error::LuaError;

#[derive(Debug)]
pub struct CachedScript {
    pub sha1: [u8; 20],
    pub source: Bytes,
    pub compiled: RegistryKey,
    pub loaded_at: u64,
}

#[derive(Debug)]
pub struct ScriptCache {
    scripts: HashMap<[u8; 20], CachedScript, RandomState>,
}

impl Default for ScriptCache {
    fn default() -> Self {
        Self {
            scripts: HashMap::with_hasher(RandomState::new()),
        }
    }
}

impl ScriptCache {
    pub fn load(&mut self, lua: &Lua, source: &str) -> Result<String, LuaError> {
        let sha1 = sha1_bytes(source.as_bytes());
        if self.scripts.contains_key(&sha1) {
            return Ok(hex_sha1(&sha1));
        }
        let function = lua.load(source).into_function()?;
        let compiled = lua.create_registry_value(function)?;
        self.scripts.insert(
            sha1,
            CachedScript {
                sha1,
                source: Bytes::copy_from_slice(source.as_bytes()),
                compiled,
                loaded_at: unix_ms(),
            },
        );
        Ok(hex_sha1(&sha1))
    }

    pub fn exists(&self, sha1s: &[&str]) -> Vec<bool> {
        sha1s
            .iter()
            .map(|sha1| parse_sha1_hex(sha1).is_ok_and(|raw| self.scripts.contains_key(&raw)))
            .collect()
    }

    pub fn flush(&mut self, lua: &Lua) -> Result<(), LuaError> {
        for (_, cached) in self.scripts.drain() {
            lua.remove_registry_value(cached.compiled)?;
        }
        Ok(())
    }

    pub fn source_for(&self, sha1: &[u8; 20]) -> Option<&Bytes> {
        self.scripts.get(sha1).map(|cached| &cached.source)
    }

    pub fn get(&self, lua: &Lua, sha1: &[u8; 20]) -> Result<Option<Function>, LuaError> {
        match self.scripts.get(sha1) {
            Some(cached) => Ok(Some(lua.registry_value(&cached.compiled)?)),
            None => Ok(None),
        }
    }

    pub fn has(&self, sha1: &[u8; 20]) -> bool {
        self.scripts.contains_key(sha1)
    }
}

pub fn sha1_bytes(bytes: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = [0u8; 20];
    out.copy_from_slice(&digest);
    out
}

pub fn hex_sha1(bytes: &[u8; 20]) -> String {
    let mut out = String::with_capacity(40);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn parse_sha1_hex(raw: &str) -> Result<[u8; 20], LuaError> {
    if raw.len() != 40 {
        return Err(LuaError::NoScript);
    }
    let mut out = [0u8; 20];
    for (index, chunk) in raw.as_bytes().chunks_exact(2).enumerate() {
        out[index] = (hex_nibble(chunk[0])? << 4) | hex_nibble(chunk[1])?;
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8, LuaError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(LuaError::NoScript),
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| value.as_millis() as u64)
}

const HEX: &[u8; 16] = b"0123456789abcdef";
