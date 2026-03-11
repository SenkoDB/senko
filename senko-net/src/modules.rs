use std::sync::{Arc, OnceLock};

use bytes::{Bytes, BytesMut};
use senko_core::{
    ModuleCommandContext, ModuleDescriptor, ModuleRegistry, ModuleResponse, ProbMergeValue,
    SenkoValue, ShardExtensions,
};
use senko_proto::{Frame, RespSerializer};
use senko_store::{SetOptions, Store};

use crate::connection::{error_bytes, error_message, frame_bytes};

static MODULE_REGISTRY: OnceLock<Arc<ModuleRegistry>> = OnceLock::new();

pub fn init(registry: Arc<ModuleRegistry>) {
    let _ = MODULE_REGISTRY.set(registry);
}

pub fn registry() -> &'static Arc<ModuleRegistry> {
    MODULE_REGISTRY.get_or_init(|| Arc::new(ModuleRegistry::new(Vec::new())))
}

pub fn descriptors() -> &'static [ModuleDescriptor] {
    registry().modules()
}

pub fn dispatch(
    shard_id: usize,
    command: &[u8],
    args: &[Frame<'_>],
    resp3: bool,
    extensions: &Arc<ShardExtensions>,
    store: &mut Store,
) -> Option<Result<Vec<u8>, Vec<u8>>> {
    let raw_args = match args.iter().map(frame_bytes).collect::<Result<Vec<_>, _>>() {
        Ok(raw_args) => raw_args,
        Err(error) => return Some(Err(error_bytes(&error))),
    };
    let mut ctx = NetModuleCommandContext {
        shard_id,
        extensions: Arc::clone(extensions),
        store,
    };
    match registry().execute(command, &mut ctx, &raw_args) {
        Some(Ok(response)) => Some(Ok(serialize(&response, resp3))),
        Some(Err(error)) => Some(Err(error_message(error.message()))),
        None => None,
    }
}

pub fn serialize(response: &ModuleResponse, resp3: bool) -> Vec<u8> {
    let mut out = BytesMut::new();
    write_response(&mut out, response, resp3);
    out.to_vec()
}

pub fn list_response() -> ModuleResponse {
    ModuleResponse::Array(Box::new(
        descriptors()
            .iter()
            .map(module_list_entry)
            .collect::<smallvec::SmallVec<[ModuleResponse; 16]>>(),
    ))
}

pub fn info_section() -> String {
    let mut out = String::new();
    for descriptor in descriptors() {
        out.push_str(&format!(
            "module:name={},ver={},path=builtin,args=\r\n",
            descriptor.name, descriptor.version
        ));
    }
    out
}

fn module_list_entry(descriptor: &ModuleDescriptor) -> ModuleResponse {
    ModuleResponse::Map(Box::new(smallvec::smallvec![
        bulk(b"name"),
        bulk(descriptor.name.as_bytes()),
        bulk(b"ver"),
        ModuleResponse::Integer(descriptor.version as i64),
        bulk(b"path"),
        bulk(b"builtin"),
        bulk(b"args"),
        ModuleResponse::Array(Box::default()),
    ]))
}

fn bulk(value: &[u8]) -> ModuleResponse {
    ModuleResponse::Bulk(Some(Bytes::copy_from_slice(value)))
}

fn write_response(out: &mut BytesMut, response: &ModuleResponse, resp3: bool) {
    match response {
        ModuleResponse::Simple(value) => RespSerializer::write_simple_string(out, value),
        ModuleResponse::Bulk(Some(value)) => RespSerializer::write_bulk_string(out, value),
        ModuleResponse::Bulk(None) => {
            if resp3 {
                RespSerializer::write_null(out);
            } else {
                RespSerializer::write_nil_bulk(out);
            }
        }
        ModuleResponse::Integer(value) => RespSerializer::write_integer(out, *value),
        ModuleResponse::Array(items) => {
            RespSerializer::write_array_header(out, items.len());
            for item in items.iter() {
                write_response(out, item, resp3);
            }
        }
        ModuleResponse::Map(items) => {
            if resp3 {
                RespSerializer::write_raw_map_header(out, items.len() / 2);
            } else {
                RespSerializer::write_array_header(out, items.len());
            }
            for item in items.iter() {
                write_response(out, item, resp3);
            }
        }
    }
}

struct NetModuleCommandContext {
    shard_id: usize,
    extensions: Arc<ShardExtensions>,
    store: *mut Store,
}

impl ModuleCommandContext for NetModuleCommandContext {
    fn shard_id(&self) -> usize {
        self.shard_id
    }

    fn shard_extensions(&self) -> &ShardExtensions {
        &self.extensions
    }

    fn get_value(&mut self, key: &[u8]) -> Option<SenkoValue> {
        // SAFETY: module dispatch holds exclusive access to the store for the duration
        // of the command and only this context dereferences the pointer.
        unsafe { (&mut *self.store).get_cloned(key) }
    }

    fn get_prob_merge_values(&mut self, key: &[u8]) -> Vec<ProbMergeValue> {
        let mut values = Vec::new();
        if let Some(local) = self.get_value(key) {
            match local {
                SenkoValue::CountMinSketch(sketch) => {
                    values.push(ProbMergeValue::CountMinSketch(sketch));
                }
                SenkoValue::TDigest(digest) => {
                    values.push(ProbMergeValue::TDigest(digest));
                }
                _ => {}
            }
        }
        values.extend(
            crate::commands::server::info::fetch_prob_merge_values_for_key(self.shard_id, key),
        );
        values
    }

    fn set_value(&mut self, key: &[u8], value: SenkoValue) {
        if let Ok(key) = compact_str::CompactString::from_utf8(key) {
            // SAFETY: same exclusive access guarantee as above.
            let _ = unsafe { (&mut *self.store).set(key, value, SetOptions::default()) };
        }
    }

    fn delete_key(&mut self, key: &[u8]) -> u64 {
        // SAFETY: same exclusive access guarantee as above.
        unsafe { (&mut *self.store).delete(key) as u64 }
    }
}
