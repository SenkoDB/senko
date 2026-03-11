use std::{
    any::{Any, TypeId},
    sync::{Arc, RwLock},
};

use ahash::RandomState;
use bytes::Bytes;
use compact_str::CompactString;
use hashbrown::HashMap;
use smallvec::SmallVec;

#[cfg(feature = "prob")]
use crate::ProbMergeValue;
use crate::SenkoValue;

pub type ModuleResult = Result<ModuleResponse, ModuleError>;
pub type ModuleCommandHandler =
    for<'a> fn(&mut dyn ModuleCommandContext, &[&'a [u8]]) -> ModuleResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleDescriptor {
    pub name: &'static str,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ModuleResponse {
    Simple(&'static [u8]),
    Bulk(Option<Bytes>),
    Integer(i64),
    Array(Box<SmallVec<[ModuleResponse; 16]>>),
    Map(Box<SmallVec<[ModuleResponse; 32]>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleError {
    message: CompactString,
}

impl ModuleError {
    pub fn new(message: impl Into<CompactString>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        self.message.as_str()
    }
}

impl std::fmt::Display for ModuleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for ModuleError {}

pub trait ModuleCommandContext {
    fn shard_id(&self) -> usize;
    fn shard_extensions(&self) -> &ShardExtensions;
    fn get_value(&mut self, key: &[u8]) -> Option<SenkoValue>;
    #[cfg(feature = "prob")]
    fn get_prob_merge_values(&mut self, key: &[u8]) -> Vec<ProbMergeValue> {
        match self.get_value(key) {
            Some(SenkoValue::CountMinSketch(sketch)) => {
                vec![ProbMergeValue::CountMinSketch(sketch)]
            }
            Some(SenkoValue::TDigest(digest)) => vec![ProbMergeValue::TDigest(digest)],
            _ => Vec::new(),
        }
    }
    fn set_value(&mut self, key: &[u8], value: SenkoValue);
    fn delete_key(&mut self, key: &[u8]) -> u64;
}

pub trait SenkoModule: Send + Sync {
    fn name(&self) -> &'static str;
    fn version(&self) -> u64;
    fn register_commands(&self, registry: &mut CommandRegistry);
    fn init_shard(&self, shard: &mut ShardState);
}

#[derive(Default)]
pub struct CommandRegistry {
    commands: Vec<ModuleCommand>,
}

impl CommandRegistry {
    pub fn register(&mut self, name: &'static str, handler: ModuleCommandHandler) {
        assert!(
            !self
                .commands
                .iter()
                .any(|command| command.name.eq_ignore_ascii_case(name)),
            "duplicate module command registration: {name}"
        );
        self.commands.push(ModuleCommand { name, handler });
    }
}

#[derive(Clone)]
struct ModuleCommand {
    name: &'static str,
    handler: ModuleCommandHandler,
}

#[derive(Clone)]
struct RegisteredModuleCommand {
    module: ModuleDescriptor,
    command: ModuleCommand,
}

pub struct ModuleRegistry {
    modules_impl: Vec<Arc<dyn SenkoModule>>,
    modules: Vec<ModuleDescriptor>,
    commands: Vec<RegisteredModuleCommand>,
}

impl ModuleRegistry {
    pub fn new(modules: Vec<Arc<dyn SenkoModule>>) -> Self {
        let mut descriptors = Vec::with_capacity(modules.len());
        let mut commands = Vec::new();
        for module in &modules {
            let descriptor = ModuleDescriptor {
                name: module.name(),
                version: module.version(),
            };
            let mut registry = CommandRegistry::default();
            module.register_commands(&mut registry);
            for command in registry.commands {
                assert!(
                    !commands.iter().any(|registered: &RegisteredModuleCommand| {
                        registered.command.name.eq_ignore_ascii_case(command.name)
                    }),
                    "duplicate module command registration: {}",
                    command.name
                );
                commands.push(RegisteredModuleCommand {
                    module: descriptor.clone(),
                    command,
                });
            }
            descriptors.push(descriptor);
        }
        Self {
            modules_impl: modules,
            modules: descriptors,
            commands,
        }
    }

    pub fn modules(&self) -> &[ModuleDescriptor] {
        &self.modules
    }

    pub fn execute(
        &self,
        command: &[u8],
        ctx: &mut dyn ModuleCommandContext,
        args: &[&[u8]],
    ) -> Option<ModuleResult> {
        self.commands
            .iter()
            .find(|registered| {
                registered
                    .command
                    .name
                    .as_bytes()
                    .eq_ignore_ascii_case(command)
            })
            .map(|registered| (registered.command.handler)(ctx, args))
    }

    pub fn owning_module(&self, command: &[u8]) -> Option<&ModuleDescriptor> {
        self.commands
            .iter()
            .find(|registered| {
                registered
                    .command
                    .name
                    .as_bytes()
                    .eq_ignore_ascii_case(command)
            })
            .map(|registered| &registered.module)
    }

    pub fn init_shard(&self, shard: &mut ShardState) {
        for module in &self.modules_impl {
            module.init_shard(shard);
        }
    }
}

#[derive(Default)]
pub struct ShardExtensions {
    entries: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>, RandomState>>,
}

impl ShardExtensions {
    pub fn set<T>(&self, value: Arc<T>)
    where
        T: Any + Send + Sync + 'static,
    {
        self.entries
            .write()
            .expect("shard extensions write lock poisoned")
            .insert(TypeId::of::<T>(), value);
    }

    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        self.entries
            .read()
            .expect("shard extensions read lock poisoned")
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|value| value.downcast::<T>().ok())
    }
}

pub struct ShardState {
    shard_id: usize,
    extensions: Arc<ShardExtensions>,
}

impl ShardState {
    pub fn new(shard_id: usize, extensions: Arc<ShardExtensions>) -> Self {
        Self {
            shard_id,
            extensions,
        }
    }

    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    pub fn extensions(&self) -> &Arc<ShardExtensions> {
        &self.extensions
    }

    pub fn set_extension<T>(&mut self, value: Arc<T>)
    where
        T: Any + Send + Sync + 'static,
    {
        self.extensions.set(value);
    }
}
