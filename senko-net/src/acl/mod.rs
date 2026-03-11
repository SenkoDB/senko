use std::{
    cell::RefCell,
    collections::VecDeque,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use ahash::RandomState;
use bytes::Bytes;
use compact_str::CompactString;
use compio::io::AsyncWrite;
use getrandom::fill as getrandom_fill;
use hashbrown::HashMap;
use roaring::RoaringBitmap;
use senko_core::{SenkoConfig, SenkoValue};
use senko_proto::Frame;
use senko_store::{Response, pattern::glob_match};
use sha2::{Digest, Sha256};
use smallvec::{SmallVec, smallvec};

use crate::{
    blocked::{BlockedKeyRegistry, UnblockReason},
    connection::{
        ClientConnectionMap, ConnectionFlags, ConnectionMeta, bulk_string, error_bytes,
        error_message, frame_bytes, serialize_response, simple_string,
    },
};

const DEFAULT_LOG_MAX: usize = 128;
const DEFAULT_USER: &str = "default";
const CATEGORY_READ: u32 = 1 << 0;
const CATEGORY_WRITE: u32 = 1 << 1;
const CATEGORY_SET: u32 = 1 << 2;
const CATEGORY_SORTEDSET: u32 = 1 << 3;
const CATEGORY_LIST: u32 = 1 << 4;
const CATEGORY_HASH: u32 = 1 << 5;
const CATEGORY_STRING: u32 = 1 << 6;
const CATEGORY_BITMAP: u32 = 1 << 7;
const CATEGORY_HYPERLOGLOG: u32 = 1 << 8;
const CATEGORY_GEO: u32 = 1 << 9;
const CATEGORY_STREAM: u32 = 1 << 10;
const CATEGORY_PUBSUB: u32 = 1 << 11;
const CATEGORY_ADMIN: u32 = 1 << 12;
const CATEGORY_FAST: u32 = 1 << 13;
const CATEGORY_SLOW: u32 = 1 << 14;
const CATEGORY_BLOCKING: u32 = 1 << 15;
const CATEGORY_DANGEROUS: u32 = 1 << 16;
const CATEGORY_CONNECTION: u32 = 1 << 17;
const CATEGORY_TRANSACTION: u32 = 1 << 18;
const CATEGORY_SCRIPTING: u32 = 1 << 19;
const CATEGORY_KEYSPACE: u32 = 1 << 20;

static ACL_REGISTRY: OnceLock<Arc<Mutex<Arc<AclState>>>> = OnceLock::new();
static COMMAND_REGISTRY: OnceLock<CommandRegistry> = OnceLock::new();

pub type PasswordHash = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyPatternMode {
    ReadWrite,
    ReadOnly,
    WriteOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclKeyPattern {
    pub pattern: CompactString,
    pub mode: KeyPatternMode,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CommandPermissions {
    pub allowed: RoaringBitmap,
    pub denied: RoaringBitmap,
}

impl CommandPermissions {
    pub fn check(&self, cmd_id: u16) -> bool {
        let cmd_id = u32::from(cmd_id);
        if self.denied.contains(cmd_id) {
            return false;
        }
        self.allowed.contains(cmd_id)
    }

    fn allow(&mut self, cmd_id: u16) {
        let id = u32::from(cmd_id);
        self.denied.remove(id);
        self.allowed.insert(id);
    }

    fn deny(&mut self, cmd_id: u16) {
        let id = u32::from(cmd_id);
        self.allowed.remove(id);
        self.denied.insert(id);
    }

    fn allow_many(&mut self, ids: impl IntoIterator<Item = u16>) {
        for id in ids {
            self.allow(id);
        }
    }

    fn deny_many(&mut self, ids: impl IntoIterator<Item = u16>) {
        for id in ids {
            self.deny(id);
        }
    }

    fn clear(&mut self) {
        self.allowed.clear();
        self.denied.clear();
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AclUser {
    pub username: CompactString,
    pub enabled: bool,
    pub nopass: bool,
    pub passwords: SmallVec<[PasswordHash; 2]>,
    pub key_patterns: Vec<AclKeyPattern>,
    pub channel_patterns: Vec<CompactString>,
    pub allowed_commands: CommandPermissions,
}

impl AclUser {
    fn default_user(config: &SenkoConfig) -> Self {
        let mut user = Self::new(DEFAULT_USER);
        user.enabled = true;
        user.key_patterns.push(AclKeyPattern {
            pattern: CompactString::const_new("*"),
            mode: KeyPatternMode::ReadWrite,
        });
        user.channel_patterns.push(CompactString::const_new("*"));
        user.allowed_commands
            .allow_many(command_registry().all_ids());
        if let Some(password) = &config.auth_password {
            user.nopass = false;
            user.passwords.push(hash_password(password.as_bytes()));
        } else {
            user.nopass = true;
        }
        user
    }

    fn new(username: &str) -> Self {
        Self {
            username: CompactString::from(username),
            enabled: false,
            nopass: true,
            passwords: SmallVec::new(),
            key_patterns: Vec::new(),
            channel_patterns: Vec::new(),
            allowed_commands: CommandPermissions::default(),
        }
    }

    fn reset(&mut self) {
        self.enabled = false;
        self.nopass = true;
        self.passwords.clear();
        self.key_patterns.clear();
        self.channel_patterns.clear();
        self.allowed_commands.clear();
        self.allowed_commands
            .deny_many(command_registry().all_ids().into_iter());
    }

    fn can_auth(&self, password: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }
        if self.nopass {
            return true;
        }
        let hash = hash_password(password);
        self.passwords.iter().any(|candidate| candidate == &hash)
    }

    fn allows_key(&self, key: &[u8], intent: AccessIntent) -> bool {
        if self.key_patterns.is_empty() {
            return false;
        }
        self.key_patterns.iter().any(|pattern| {
            glob_match(pattern.pattern.as_bytes(), key)
                && matches!(
                    (pattern.mode, intent),
                    (KeyPatternMode::ReadWrite, _)
                        | (KeyPatternMode::ReadOnly, AccessIntent::Read)
                        | (KeyPatternMode::WriteOnly, AccessIntent::Write)
                )
        })
    }

    fn allows_channel(&self, channel: &[u8]) -> bool {
        if self.channel_patterns.is_empty() {
            return false;
        }
        self.channel_patterns
            .iter()
            .any(|pattern| glob_match(pattern.as_bytes(), channel))
    }
}

#[derive(Debug, Clone)]
pub struct AclState {
    pub users: HashMap<CompactString, AclUser, RandomState>,
    pub log: VecDeque<AclLogEntry>,
    pub log_max: usize,
    next_entry_id: u64,
    aclfile: Option<PathBuf>,
}

impl AclState {
    fn new(config: &SenkoConfig) -> Self {
        let mut users = HashMap::with_hasher(RandomState::new());
        users.insert(
            CompactString::const_new(DEFAULT_USER),
            AclUser::default_user(config),
        );
        Self {
            users,
            log: VecDeque::new(),
            log_max: DEFAULT_LOG_MAX,
            next_entry_id: 1,
            aclfile: config.aclfile.clone(),
        }
    }

    fn push_log(&mut self, mut entry: AclLogEntry) {
        if let Some(existing) = self.log.iter_mut().find(|candidate| {
            candidate.reason == entry.reason
                && candidate.context == entry.context
                && candidate.object == entry.object
                && candidate.username == entry.username
                && candidate.client_info == entry.client_info
        }) {
            existing.count = existing.count.saturating_add(1);
            return;
        }
        entry.entry_id = self.next_entry_id;
        self.next_entry_id = self.next_entry_id.saturating_add(1);
        self.log.push_front(entry);
        while self.log.len() > self.log_max {
            let _ = self.log.pop_back();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclDenyReason {
    Auth,
    Command,
    Key,
    Channel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclContext {
    Toplevel,
    Multi,
    Lua,
}

#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub count: u64,
    pub reason: AclDenyReason,
    pub context: AclContext,
    pub object: CompactString,
    pub username: CompactString,
    pub age_seconds: f64,
    pub client_info: CompactString,
    pub entry_id: u64,
    first_seen_ms: u64,
}

#[derive(Debug)]
pub struct AclCommandOutcome {
    pub response: Vec<u8>,
    pub close_after_write: bool,
    pub suppress_response: bool,
    pub force_send_response: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccessIntent {
    Read,
    Write,
}

#[derive(Debug)]
struct CommandRegistry {
    specs: Vec<CommandSpec>,
    ids: HashMap<&'static str, u16, RandomState>,
    categories: HashMap<&'static str, u32, RandomState>,
}

#[derive(Debug, Clone, Copy)]
struct CommandSpec {
    id: u16,
    name: &'static str,
    categories: u32,
}

#[derive(Debug)]
struct AclTargets {
    effective_name: CompactString,
    command_id: Option<u16>,
    root_id: Option<u16>,
    keys: Vec<(CompactString, AccessIntent)>,
    channels: Vec<CompactString>,
}

#[derive(Debug)]
enum SetUserOp {
    On,
    Off,
    AddPassword(PasswordHash),
    RemovePassword(PasswordHash),
    NoPass,
    ResetPass,
    AddKeyPattern(AclKeyPattern),
    ResetKeys,
    AddChannelPattern(CompactString),
    ResetChannels,
    AllowCommand(u16),
    DenyCommand(u16),
    AllowCategory(u32),
    DenyCategory(u32),
    AllCommands,
    NoCommands,
    Reset,
}

pub fn init(config: &SenkoConfig) {
    let _ = command_registry();
    let state = Arc::new(AclState::new(config));
    let _ = ACL_REGISTRY.set(Arc::new(Mutex::new(state)));
}

pub fn current_state() -> Arc<AclState> {
    ACL_REGISTRY
        .get()
        .expect("acl state not initialized")
        .lock()
        .expect("acl state lock poisoned")
        .clone()
}

fn swap_state(next: AclState) {
    let cell = ACL_REGISTRY.get().expect("acl state not initialized");
    *cell.lock().expect("acl state lock poisoned") = Arc::new(next);
}

pub fn connection_starts_authenticated() -> bool {
    let Some(cell) = ACL_REGISTRY.get() else {
        return true;
    };
    cell.lock()
        .expect("acl state lock poisoned")
        .users
        .get(DEFAULT_USER)
        .is_some_and(|user| user.enabled && user.nopass)
}

pub fn default_username() -> CompactString {
    CompactString::const_new(DEFAULT_USER)
}

pub fn default_password_hash_prefix() -> Option<String> {
    current_state()
        .users
        .get(DEFAULT_USER)
        .and_then(|user| user.passwords.first())
        .map(|hash| hex(hash)[..8].to_owned())
}

pub fn set_default_user_password(password: Option<String>) {
    let snapshot = current_state();
    let mut next = (*snapshot).clone();
    let user = next
        .users
        .entry(CompactString::const_new(DEFAULT_USER))
        .or_insert_with(|| AclUser::default_user(&SenkoConfig::default()));
    user.enabled = true;
    user.allowed_commands
        .allow_many(command_registry().all_ids());
    if user.key_patterns.is_empty() {
        user.key_patterns.push(AclKeyPattern {
            pattern: CompactString::const_new("*"),
            mode: KeyPatternMode::ReadWrite,
        });
    }
    if user.channel_patterns.is_empty() {
        user.channel_patterns.push(CompactString::const_new("*"));
    }
    match password {
        Some(password) if !password.is_empty() => {
            user.nopass = false;
            user.passwords.clear();
            user.passwords.push(hash_password(password.as_bytes()));
        }
        _ => {
            user.nopass = true;
            user.passwords.clear();
        }
    }
    swap_state(next);
}

pub fn set_log_max_len(log_max: usize) {
    let snapshot = current_state();
    let mut next = (*snapshot).clone();
    next.log_max = log_max;
    while next.log.len() > next.log_max {
        let _ = next.log.pop_back();
    }
    swap_state(next);
}

pub fn reset_connection_auth(meta: &mut ConnectionMeta) {
    meta.flags.remove(ConnectionFlags::AUTHENTICATED);
    meta.username = default_username();
    if connection_starts_authenticated() {
        meta.flags.insert(ConnectionFlags::AUTHENTICATED);
    }
}

pub fn authenticate(
    meta: &mut ConnectionMeta,
    username: &[u8],
    password: &[u8],
) -> Result<(), Vec<u8>> {
    let username = std::str::from_utf8(username)
        .ok()
        .and_then(|name| CompactString::from_utf8(name.as_bytes()).ok())
        .ok_or_else(|| {
            error_message("WRONGPASS invalid username-password pair or user is disabled.")
        })?;
    let snapshot = current_state();
    let Some(user) = snapshot.users.get(username.as_str()) else {
        log_auth_failure(&username, meta);
        return Err(error_message(
            "WRONGPASS invalid username-password pair or user is disabled.",
        ));
    };
    if !user.can_auth(password) {
        log_auth_failure(&username, meta);
        return Err(error_message(
            "WRONGPASS invalid username-password pair or user is disabled.",
        ));
    }
    meta.flags.insert(ConnectionFlags::AUTHENTICATED);
    meta.username = username;
    Ok(())
}

pub fn check_permissions(
    meta: &ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    context: AclContext,
    qbuf_len: usize,
) -> Result<(), Vec<u8>> {
    let snapshot = current_state();
    let Some(user) = snapshot.users.get(meta.username.as_str()) else {
        return Err(command_denied(command, meta, context, qbuf_len));
    };
    if !user.enabled {
        return Err(command_denied(command, meta, context, qbuf_len));
    }
    let targets = extract_acl_targets(command, args).map_err(|message| error_message(message))?;
    if let Some(command_id) = targets.command_id {
        let command_allowed = if targets.effective_name.as_str()
            != std::str::from_utf8(command).unwrap_or_default()
        {
            user.allowed_commands.check(command_id)
                || targets
                    .root_id
                    .is_some_and(|root_id| user.allowed_commands.check(root_id))
        } else {
            user.allowed_commands.check(command_id)
        };
        if !command_allowed {
            log_denial(
                AclDenyReason::Command,
                context,
                targets.effective_name.clone(),
                meta,
                qbuf_len,
            );
            return Err(error_message(&format!(
                "NOPERM this user has no permissions to run the '{}' command",
                targets.effective_name
            )));
        }
    }
    for (key, intent) in &targets.keys {
        if !user.allows_key(key.as_bytes(), *intent) {
            log_denial(AclDenyReason::Key, context, key.clone(), meta, qbuf_len);
            return Err(error_message("NOPERM No permissions to access a key"));
        }
    }
    for channel in &targets.channels {
        if !user.allows_channel(channel.as_bytes()) {
            log_denial(
                AclDenyReason::Channel,
                context,
                channel.clone(),
                meta,
                qbuf_len,
            );
            return Err(error_message("NOPERM No permissions to access a channel"));
        }
    }
    Ok(())
}

pub fn execute(
    command: &[u8],
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    config: &SenkoConfig,
) -> Option<Result<AclCommandOutcome, Vec<u8>>> {
    if !eq_ascii(command, b"ACL") {
        return None;
    }
    Some(dispatch_acl(
        args,
        meta,
        client_connections,
        blocked,
        config,
    ))
}

fn dispatch_acl(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    config: &SenkoConfig,
) -> Result<AclCommandOutcome, Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl' command",
        ));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;
    if eq_ascii(subcommand, b"SETUSER") {
        return acl_setuser(rest);
    }
    if eq_ascii(subcommand, b"GETUSER") {
        return acl_getuser(rest, meta.resp_version == 3);
    }
    if eq_ascii(subcommand, b"DELUSER") {
        return acl_deluser(rest, meta, client_connections, blocked);
    }
    if eq_ascii(subcommand, b"LIST") {
        return acl_list(rest, meta.resp_version == 3);
    }
    if eq_ascii(subcommand, b"USERS") {
        return acl_users(rest, meta.resp_version == 3);
    }
    if eq_ascii(subcommand, b"WHOAMI") {
        return acl_whoami(rest, meta);
    }
    if eq_ascii(subcommand, b"CAT") {
        return acl_cat(rest, meta.resp_version == 3);
    }
    if eq_ascii(subcommand, b"GENPASS") {
        return acl_genpass(rest);
    }
    if eq_ascii(subcommand, b"DRYRUN") {
        return acl_dryrun(rest);
    }
    if eq_ascii(subcommand, b"LOG") {
        return acl_log(rest, meta.resp_version == 3);
    }
    if eq_ascii(subcommand, b"LOAD") {
        return acl_load(rest, config);
    }
    if eq_ascii(subcommand, b"SAVE") {
        return acl_save(rest, config);
    }
    Err(error_message(
        "ERR Unknown ACL subcommand or wrong number of arguments",
    ))
}

fn acl_setuser(args: &[Frame<'_>]) -> Result<AclCommandOutcome, Vec<u8>> {
    let Some((username, rules)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl|setuser' command",
        ));
    };
    let username = parse_compact(frame_bytes(username).map_err(|error| error_bytes(&error))?)?;
    let ops = parse_setuser_rules(rules)?;
    let snapshot = current_state();
    let mut next = (*snapshot).clone();
    let mut user = next
        .users
        .remove(username.as_str())
        .unwrap_or_else(|| AclUser::new(username.as_str()));
    for op in ops {
        apply_setuser_op(&mut user, op);
    }
    next.users.insert(username.clone(), user);
    swap_state(next);
    Ok(ok_outcome(simple_string(b"OK")))
}

fn acl_getuser(args: &[Frame<'_>], resp3: bool) -> Result<AclCommandOutcome, Vec<u8>> {
    let [username] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl|getuser' command",
        ));
    };
    let username = frame_bytes(username).map_err(|error| error_bytes(&error))?;
    let snapshot = current_state();
    let Some(user) = lookup_user(&snapshot, username) else {
        return Ok(ok_outcome(serialize_response(
            &Response::Value(None),
            resp3,
        )));
    };
    let flags = {
        let mut values = SmallVec::new();
        values.push(bulk_value(if user.enabled { b"on" } else { b"off" }));
        if user.nopass {
            values.push(bulk_value(b"nopass"));
        }
        Response::Array(Box::new(values))
    };
    let passwords = Response::Array(Box::new(
        user.passwords
            .iter()
            .map(|hash| bulk_response(format!("#{}", hex(hash)).into_bytes()))
            .collect(),
    ));
    let keys = Response::Array(Box::new(
        user.key_patterns
            .iter()
            .map(|pattern| bulk_response(render_key_pattern(pattern).into_bytes()))
            .collect(),
    ));
    let channels = Response::Array(Box::new(
        user.channel_patterns
            .iter()
            .map(|pattern| bulk_response(format!("&{pattern}").into_bytes()))
            .collect(),
    ));
    let response = Response::Map(Box::new(smallvec![
        bulk_value(b"flags"),
        flags,
        bulk_value(b"passwords"),
        passwords,
        bulk_value(b"commands"),
        bulk_response(render_command_rules(&user.allowed_commands).into_bytes()),
        bulk_value(b"keys"),
        keys,
        bulk_value(b"channels"),
        channels,
        bulk_value(b"selectors"),
        Response::Array(Box::new(SmallVec::new())),
    ]));
    Ok(ok_outcome(serialize_response(&response, resp3)))
}

fn acl_deluser(
    args: &[Frame<'_>],
    meta: &ConnectionMeta,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
) -> Result<AclCommandOutcome, Vec<u8>> {
    if args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl|deluser' command",
        ));
    }
    let snapshot = current_state();
    let mut next = (*snapshot).clone();
    let mut deleted = 0i64;
    let mut deleted_names = Vec::new();
    for arg in args {
        let username = parse_compact(frame_bytes(arg).map_err(|error| error_bytes(&error))?)?;
        if username == DEFAULT_USER {
            return Err(error_message(
                "ERR The special 'default' user can't be removed from the ACL",
            ));
        }
        if next.users.remove(username.as_str()).is_some() {
            deleted += 1;
            deleted_names.push(username);
        }
    }
    if deleted > 0 {
        swap_state(next);
        disconnect_users(&deleted_names, meta.id, client_connections, blocked);
    }
    Ok(ok_outcome(serialize_response(
        &Response::Integer(deleted),
        meta.resp_version == 3,
    )))
}

fn acl_list(args: &[Frame<'_>], resp3: bool) -> Result<AclCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl|list' command",
        ));
    }
    let snapshot = current_state();
    let mut names = snapshot.users.keys().cloned().collect::<Vec<_>>();
    names.sort();
    let values = names
        .into_iter()
        .filter_map(|username| {
            snapshot
                .users
                .get(username.as_str())
                .map(render_acl_list_line)
        })
        .map(|line| bulk_response(line.into_bytes()))
        .collect();
    Ok(ok_outcome(serialize_response(
        &Response::Array(Box::new(values)),
        resp3,
    )))
}

fn acl_users(args: &[Frame<'_>], resp3: bool) -> Result<AclCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl|users' command",
        ));
    }
    let snapshot = current_state();
    let mut names = snapshot.users.keys().cloned().collect::<Vec<_>>();
    names.sort();
    let values = names
        .into_iter()
        .map(|name| bulk_response(name.to_string().into_bytes()))
        .collect();
    Ok(ok_outcome(serialize_response(
        &Response::Array(Box::new(values)),
        resp3,
    )))
}

fn acl_whoami(args: &[Frame<'_>], meta: &ConnectionMeta) -> Result<AclCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl|whoami' command",
        ));
    }
    Ok(ok_outcome(bulk_string(meta.username.as_bytes())))
}

fn acl_cat(args: &[Frame<'_>], resp3: bool) -> Result<AclCommandOutcome, Vec<u8>> {
    let registry = command_registry();
    if args.is_empty() {
        let mut names = registry.categories.keys().copied().collect::<Vec<_>>();
        names.sort_unstable();
        let response = Response::Array(Box::new(
            names
                .into_iter()
                .map(|name| bulk_response(name.as_bytes().to_vec()))
                .collect(),
        ));
        return Ok(ok_outcome(serialize_response(&response, resp3)));
    }
    let [category] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl|cat' command",
        ));
    };
    let category = std::str::from_utf8(frame_bytes(category).map_err(|error| error_bytes(&error))?)
        .map_err(|_| error_message("ERR unknown category"))?
        .trim_start_matches('@')
        .to_ascii_lowercase();
    let Some(flag) = registry.categories.get(category.as_str()).copied() else {
        return Err(error_message("ERR unknown category"));
    };
    let response = Response::Array(Box::new(
        registry
            .specs
            .iter()
            .filter(|spec| (spec.categories & flag) != 0)
            .map(|spec| bulk_response(spec.name.as_bytes().to_vec()))
            .collect(),
    ));
    Ok(ok_outcome(serialize_response(&response, resp3)))
}

fn acl_genpass(args: &[Frame<'_>]) -> Result<AclCommandOutcome, Vec<u8>> {
    let bits = match args {
        [] => 256usize,
        [bits] => std::str::from_utf8(frame_bytes(bits).map_err(|error| error_bytes(&error))?)
            .ok()
            .and_then(|text| text.parse::<usize>().ok())
            .ok_or_else(|| error_message("ERR value is not an integer or out of range"))?,
        _ => {
            return Err(error_message(
                "ERR wrong number of arguments for 'acl|genpass' command",
            ));
        }
    };
    if !(1..=4096).contains(&bits) || bits % 4 != 0 {
        return Err(error_message(
            "ERR ACL GENPASS argument must be the number of bits, a multiple of 4 and between 1 and 4096",
        ));
    }
    let mut bytes = vec![0u8; bits / 8 + usize::from(bits % 8 != 0)];
    getrandom_fill(&mut bytes).map_err(|_| error_message("ERR failed to generate password"))?;
    let mut out = hex(&bytes);
    out.truncate(bits / 4);
    Ok(ok_outcome(bulk_string(out.as_bytes())))
}

fn acl_dryrun(args: &[Frame<'_>]) -> Result<AclCommandOutcome, Vec<u8>> {
    if args.len() < 2 {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl|dryrun' command",
        ));
    }
    let username = parse_compact(frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?)?;
    let command = frame_bytes(&args[1]).map_err(|error| error_bytes(&error))?;
    let fake_meta = ConnectionMeta::for_acl_dryrun(username);
    let snapshot = current_state();
    let Some(user) = snapshot.users.get(fake_meta.username.as_str()) else {
        return Err(error_message(
            "WRONGPASS invalid username-password pair or user is disabled.",
        ));
    };
    if !user.enabled {
        return Err(error_message(
            "WRONGPASS invalid username-password pair or user is disabled.",
        ));
    }
    check_permissions(&fake_meta, command, &args[2..], AclContext::Toplevel, 0)?;
    Ok(ok_outcome(simple_string(b"OK")))
}

fn acl_log(args: &[Frame<'_>], resp3: bool) -> Result<AclCommandOutcome, Vec<u8>> {
    let snapshot = current_state();
    if args.is_empty() {
        return Ok(ok_outcome(serialize_response(
            &render_acl_log(&snapshot, 10),
            resp3,
        )));
    }
    let [arg] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl|log' command",
        ));
    };
    let arg = frame_bytes(arg).map_err(|error| error_bytes(&error))?;
    if eq_ascii(arg, b"RESET") {
        let mut next = (*snapshot).clone();
        next.log.clear();
        swap_state(next);
        return Ok(ok_outcome(simple_string(b"OK")));
    }
    let count = std::str::from_utf8(arg)
        .ok()
        .and_then(|text| text.parse::<usize>().ok())
        .ok_or_else(|| error_message("ERR value is not an integer or out of range"))?;
    Ok(ok_outcome(serialize_response(
        &render_acl_log(&snapshot, count),
        resp3,
    )))
}

fn acl_load(args: &[Frame<'_>], config: &SenkoConfig) -> Result<AclCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl|load' command",
        ));
    }
    let path = config.aclfile.as_deref().ok_or_else(|| {
        error_message("ERR This Redis server is not configured to use an ACL file.")
    })?;
    let loaded = load_acl_file(path, config)?;
    swap_state(loaded);
    Ok(ok_outcome(simple_string(b"OK")))
}

fn acl_save(args: &[Frame<'_>], config: &SenkoConfig) -> Result<AclCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'acl|save' command",
        ));
    }
    let path = config.aclfile.as_deref().ok_or_else(|| {
        error_message("ERR This Redis server is not configured to use an ACL file.")
    })?;
    save_acl_file(path, &current_state()).map_err(|error| error_message(&error))?;
    Ok(ok_outcome(simple_string(b"OK")))
}

fn load_acl_file(path: &Path, config: &SenkoConfig) -> Result<AclState, Vec<u8>> {
    let contents = fs::read_to_string(path).map_err(|error| error_message(&error.to_string()))?;
    let mut state = AclState::new(config);
    state.users.clear();
    for (lineno, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let tokens = split_acl_line(line);
        if tokens.len() < 2 || tokens[0] != "user" {
            return Err(error_message(&format!(
                "ERR Error in ACL file line {}",
                lineno + 1
            )));
        }
        let username = CompactString::from(tokens[1].as_str());
        let ops = parse_setuser_rule_tokens(&tokens[2..]).map_err(|message| {
            error_message(&format!(
                "ERR Error in ACL file line {}: {message}",
                lineno + 1
            ))
        })?;
        let mut user = state
            .users
            .remove(username.as_str())
            .unwrap_or_else(|| AclUser::new(username.as_str()));
        for op in ops {
            apply_setuser_op(&mut user, op);
        }
        state.users.insert(username, user);
    }
    state.aclfile = Some(path.to_path_buf());
    Ok(state)
}

fn save_acl_file(path: &Path, state: &AclState) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    let mut names = state.users.keys().cloned().collect::<Vec<_>>();
    names.sort();
    let mut out = String::new();
    for name in names {
        if let Some(user) = state.users.get(name.as_str()) {
            out.push_str(&render_acl_list_line(user));
            out.push('\n');
        }
    }
    fs::write(&tmp, out).map_err(|error| error.to_string())?;
    fs::rename(&tmp, path).map_err(|error| error.to_string())
}

fn render_acl_log(state: &AclState, count: usize) -> Response {
    let now_ms = now_ms();
    Response::Array(Box::new(
        state
            .log
            .iter()
            .take(count)
            .map(|entry| {
                Response::Map(Box::new(smallvec![
                    bulk_value(b"count"),
                    Response::Integer(entry.count as i64),
                    bulk_value(b"reason"),
                    bulk_response(render_reason(entry.reason).into_bytes()),
                    bulk_value(b"context"),
                    bulk_response(render_context(entry.context).into_bytes()),
                    bulk_value(b"object"),
                    bulk_response(entry.object.to_string().into_bytes()),
                    bulk_value(b"username"),
                    bulk_response(entry.username.to_string().into_bytes()),
                    bulk_value(b"age_seconds"),
                    bulk_response(
                        format!(
                            "{:.2}",
                            (now_ms.saturating_sub(entry.first_seen_ms)) as f64 / 1000.0
                        )
                        .into_bytes()
                    ),
                    bulk_value(b"client-info"),
                    bulk_response(entry.client_info.to_string().into_bytes()),
                    bulk_value(b"entry-id"),
                    Response::Integer(entry.entry_id as i64),
                ]))
            })
            .collect(),
    ))
}

fn disconnect_users(
    usernames: &[CompactString],
    issuer_id: u64,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
) {
    let usernames = usernames
        .iter()
        .map(CompactString::as_str)
        .collect::<Vec<_>>();
    let handles = client_connections
        .borrow()
        .values()
        .filter_map(|handle| {
            let meta = handle.meta.lock().ok()?.clone();
            usernames
                .contains(&meta.username.as_str())
                .then_some((handle.clone(), meta))
        })
        .collect::<Vec<_>>();
    for (handle, snapshot) in handles {
        if snapshot.flags.contains(ConnectionFlags::BLOCKED) {
            let _ = blocked
                .borrow_mut()
                .force_unblock(snapshot.id, UnblockReason::Timeout);
        }
        handle
            .close_after_write
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if snapshot.id != issuer_id {
            let writer = handle.writer.clone();
            compio::runtime::spawn(async move {
                let mut writer = writer.lock().expect("writer poisoned");
                let _ = (&mut *writer).shutdown().await;
            })
            .detach();
        }
    }
}

fn render_acl_list_line(user: &AclUser) -> String {
    let mut out = format!(
        "user {} {}",
        user.username,
        if user.enabled { "on" } else { "off" }
    );
    if user.nopass {
        out.push_str(" nopass");
    }
    for hash in &user.passwords {
        out.push(' ');
        out.push('#');
        out.push_str(&hex(hash));
    }
    for pattern in &user.key_patterns {
        out.push(' ');
        out.push_str(&render_key_pattern(pattern));
    }
    for pattern in &user.channel_patterns {
        out.push_str(&format!(" &{pattern}"));
    }
    let commands = render_command_rules(&user.allowed_commands);
    if !commands.is_empty() {
        out.push(' ');
        out.push_str(&commands);
    }
    out
}

fn render_key_pattern(pattern: &AclKeyPattern) -> String {
    match pattern.mode {
        KeyPatternMode::ReadWrite => format!("~{}", pattern.pattern),
        KeyPatternMode::ReadOnly => format!("%R~{}", pattern.pattern),
        KeyPatternMode::WriteOnly => format!("%W~{}", pattern.pattern),
    }
}

fn render_command_rules(permissions: &CommandPermissions) -> String {
    let registry = command_registry();
    let all = registry.all_ids();
    let all_allowed = all.iter().all(|id| permissions.check(*id));
    if all_allowed && permissions.denied.is_empty() {
        return "+@all".to_owned();
    }
    if permissions.allowed.is_empty() {
        return "-@all".to_owned();
    }
    let mut parts = Vec::new();
    for spec in &registry.specs {
        let id = u32::from(spec.id);
        if permissions.allowed.contains(id) {
            parts.push(format!("+{}", spec.name));
        }
    }
    for spec in &registry.specs {
        let id = u32::from(spec.id);
        if permissions.denied.contains(id) {
            parts.push(format!("-{}", spec.name));
        }
    }
    parts.join(" ")
}

fn parse_setuser_rules(rules: &[Frame<'_>]) -> Result<Vec<SetUserOp>, Vec<u8>> {
    let mut tokens = Vec::with_capacity(rules.len());
    for rule in rules {
        tokens.push(
            std::str::from_utf8(frame_bytes(rule).map_err(|error| error_bytes(&error))?)
                .map_err(|_| error_message("ERR syntax error"))?
                .to_owned(),
        );
    }
    parse_setuser_rule_tokens(&tokens).map_err(|message| error_message(&message))
}

fn parse_setuser_rule_tokens(tokens: &[String]) -> Result<Vec<SetUserOp>, String> {
    let registry = command_registry();
    let mut ops = Vec::with_capacity(tokens.len());
    for token in tokens {
        if token == "on" {
            ops.push(SetUserOp::On);
            continue;
        }
        if token == "off" {
            ops.push(SetUserOp::Off);
            continue;
        }
        if token == "nopass" {
            ops.push(SetUserOp::NoPass);
            continue;
        }
        if token == "resetpass" {
            ops.push(SetUserOp::ResetPass);
            continue;
        }
        if token == "allkeys" || token == "~*" {
            ops.push(SetUserOp::AddKeyPattern(AclKeyPattern {
                pattern: CompactString::const_new("*"),
                mode: KeyPatternMode::ReadWrite,
            }));
            continue;
        }
        if token == "resetkeys" {
            ops.push(SetUserOp::ResetKeys);
            continue;
        }
        if token == "allchannels" || token == "&*" {
            ops.push(SetUserOp::AddChannelPattern(CompactString::const_new("*")));
            continue;
        }
        if token == "resetchannels" {
            ops.push(SetUserOp::ResetChannels);
            continue;
        }
        if token == "reset" {
            ops.push(SetUserOp::Reset);
            continue;
        }
        if token == "allcommands" || token == "+@all" {
            ops.push(SetUserOp::AllCommands);
            continue;
        }
        if token == "nocommands" || token == "-@all" {
            ops.push(SetUserOp::NoCommands);
            continue;
        }
        if let Some(raw) = token.strip_prefix('>') {
            ops.push(SetUserOp::AddPassword(hash_password(raw.as_bytes())));
            continue;
        }
        if let Some(raw) = token.strip_prefix('<') {
            ops.push(SetUserOp::RemovePassword(hash_password(raw.as_bytes())));
            continue;
        }
        if let Some(raw) = token.strip_prefix('#') {
            ops.push(SetUserOp::AddPassword(parse_hash(raw)?));
            continue;
        }
        if let Some(raw) = token.strip_prefix('!') {
            ops.push(SetUserOp::RemovePassword(parse_hash(raw)?));
            continue;
        }
        if let Some(pattern) = token.strip_prefix("%RW~") {
            ops.push(SetUserOp::AddKeyPattern(AclKeyPattern {
                pattern: CompactString::from(pattern),
                mode: KeyPatternMode::ReadWrite,
            }));
            continue;
        }
        if let Some(pattern) = token.strip_prefix("%R~") {
            ops.push(SetUserOp::AddKeyPattern(AclKeyPattern {
                pattern: CompactString::from(pattern),
                mode: KeyPatternMode::ReadOnly,
            }));
            continue;
        }
        if let Some(pattern) = token.strip_prefix("%W~") {
            ops.push(SetUserOp::AddKeyPattern(AclKeyPattern {
                pattern: CompactString::from(pattern),
                mode: KeyPatternMode::WriteOnly,
            }));
            continue;
        }
        if let Some(pattern) = token.strip_prefix('~') {
            ops.push(SetUserOp::AddKeyPattern(AclKeyPattern {
                pattern: CompactString::from(pattern),
                mode: KeyPatternMode::ReadWrite,
            }));
            continue;
        }
        if let Some(pattern) = token.strip_prefix('&') {
            ops.push(SetUserOp::AddChannelPattern(CompactString::from(pattern)));
            continue;
        }
        if let Some(category) = token.strip_prefix("+@") {
            let category = category.to_ascii_lowercase();
            let Some(flag) = registry.categories.get(category.as_str()).copied() else {
                return Err(format!(
                    "ERR Error in ACL SETUSER modifier '{token}': Unknown command or category"
                ));
            };
            ops.push(SetUserOp::AllowCategory(flag));
            continue;
        }
        if let Some(category) = token.strip_prefix("-@") {
            let category = category.to_ascii_lowercase();
            let Some(flag) = registry.categories.get(category.as_str()).copied() else {
                return Err(format!(
                    "ERR Error in ACL SETUSER modifier '{token}': Unknown command or category"
                ));
            };
            ops.push(SetUserOp::DenyCategory(flag));
            continue;
        }
        if let Some(name) = token.strip_prefix('+') {
            let Some(id) = registry.id_of(&name.to_ascii_lowercase()) else {
                return Err(format!(
                    "ERR Error in ACL SETUSER modifier '{token}': Unknown command or category"
                ));
            };
            ops.push(SetUserOp::AllowCommand(id));
            continue;
        }
        if let Some(name) = token.strip_prefix('-') {
            let Some(id) = registry.id_of(&name.to_ascii_lowercase()) else {
                return Err(format!(
                    "ERR Error in ACL SETUSER modifier '{token}': Unknown command or category"
                ));
            };
            ops.push(SetUserOp::DenyCommand(id));
            continue;
        }
        return Err(format!(
            "ERR Error in ACL SETUSER modifier '{token}': Syntax error"
        ));
    }
    Ok(ops)
}

fn apply_setuser_op(user: &mut AclUser, op: SetUserOp) {
    match op {
        SetUserOp::On => user.enabled = true,
        SetUserOp::Off => user.enabled = false,
        SetUserOp::AddPassword(hash) => {
            user.nopass = false;
            if !user.passwords.contains(&hash) {
                user.passwords.push(hash);
            }
        }
        SetUserOp::RemovePassword(hash) => user.passwords.retain(|candidate| candidate != &hash),
        SetUserOp::NoPass => {
            user.nopass = true;
            user.passwords.clear();
        }
        SetUserOp::ResetPass => {
            user.nopass = false;
            user.passwords.clear();
        }
        SetUserOp::AddKeyPattern(pattern) => user.key_patterns.push(pattern),
        SetUserOp::ResetKeys => user.key_patterns.clear(),
        SetUserOp::AddChannelPattern(pattern) => user.channel_patterns.push(pattern),
        SetUserOp::ResetChannels => user.channel_patterns.clear(),
        SetUserOp::AllowCommand(id) => user.allowed_commands.allow(id),
        SetUserOp::DenyCommand(id) => user.allowed_commands.deny(id),
        SetUserOp::AllowCategory(flag) => user
            .allowed_commands
            .allow_many(command_registry().ids_for_category(flag)),
        SetUserOp::DenyCategory(flag) => user
            .allowed_commands
            .deny_many(command_registry().ids_for_category(flag)),
        SetUserOp::AllCommands => user
            .allowed_commands
            .allow_many(command_registry().all_ids()),
        SetUserOp::NoCommands => user
            .allowed_commands
            .deny_many(command_registry().all_ids()),
        SetUserOp::Reset => user.reset(),
    }
}

fn extract_acl_targets(command: &[u8], args: &[Frame<'_>]) -> Result<AclTargets, &'static str> {
    let registry = command_registry();
    let root_name = std::str::from_utf8(command)
        .map_err(|_| "ERR syntax error")?
        .to_ascii_lowercase();
    let mut effective_name = root_name.clone();
    if matches!(root_name.as_str(), "client" | "acl" | "pubsub") {
        if let Some(first) = args.first() {
            let sub = std::str::from_utf8(frame_bytes(first).map_err(|_| "ERR syntax error")?)
                .map_err(|_| "ERR syntax error")?
                .to_ascii_lowercase();
            effective_name = format!("{root_name}|{sub}");
        }
    }
    let mut targets = AclTargets {
        effective_name: CompactString::from(effective_name.as_str()),
        command_id: registry.id_of(&effective_name),
        root_id: registry.id_of(&root_name),
        keys: Vec::new(),
        channels: Vec::new(),
    };
    let lower = root_name.as_str();
    match lower {
        "get" | "getdel" | "getex" | "getrange" | "getset" | "strlen" | "substr" | "dump"
        | "type" | "ttl" | "pttl" | "expiretime" | "pexpiretime" | "object" => {
            push_first_key(args, &mut targets.keys, AccessIntent::Read)?
        }
        "set" | "setex" | "psetex" | "setnx" | "setrange" | "append" | "decr" | "decrby"
        | "incr" | "incrby" | "incrbyfloat" | "delex" | "delifex" | "digest" | "expire"
        | "pexpire" | "expireat" | "pexpireat" | "persist" => {
            push_first_key(args, &mut targets.keys, AccessIntent::Write)?
        }
        "mget" | "exists" | "touch" | "del" | "unlink" => push_all_keys(
            args,
            &mut targets.keys,
            if matches!(lower, "del" | "unlink") {
                AccessIntent::Write
            } else {
                AccessIntent::Read
            },
        )?,
        "mset" | "msetex" | "msetnx" => push_every_other_key(args, &mut targets.keys)?,
        "copy" | "rename" | "renamenx" | "move" | "lmove" | "blmove" | "rpoplpush"
        | "brpoplpush" => push_two_keys(args, &mut targets.keys)?,
        "hget" | "hgetall" | "hkeys" | "hlen" | "hmget" | "hrandfield" | "hscan" | "httl"
        | "hpttl" | "hexists" | "hstrlen" => {
            push_first_key(args, &mut targets.keys, AccessIntent::Read)?
        }
        "hset" | "hsetnx" | "hdel" | "hmset" | "hexpire" | "hexpireat" | "hexpiretime"
        | "hpexpire" | "hpexpireat" | "hpexpiretime" | "hpersist" | "hgetdel" | "hgetex"
        | "hsetex" | "hincrby" | "hincrbyfloat" => {
            push_first_key(args, &mut targets.keys, AccessIntent::Write)?
        }
        "llen" | "lindex" | "lrange" | "lpos" => {
            push_first_key(args, &mut targets.keys, AccessIntent::Read)?
        }
        "lpush" | "lpushx" | "rpush" | "rpushx" | "lpop" | "rpop" | "lrem" | "lset" | "ltrim"
        | "linsert" => push_first_key(args, &mut targets.keys, AccessIntent::Write)?,
        "lmpop" | "blmpop" | "zmpop" => push_numkeys(args, 0, &mut targets.keys)?,
        "blpop" | "brpop" => push_all_but_last(args, &mut targets.keys, AccessIntent::Write)?,
        "sadd" | "srem" | "spop" | "smove" => {
            push_first_key(args, &mut targets.keys, AccessIntent::Write)?
        }
        "scard" | "smembers" | "smismember" | "sismember" | "srandmember" | "sscan" => {
            push_first_key(args, &mut targets.keys, AccessIntent::Read)?
        }
        "sdiff" | "sinter" | "sunion" => {
            push_all_keys(args, &mut targets.keys, AccessIntent::Read)?
        }
        "sdiffstore" | "sinterstore" | "sunionstore" => {
            push_store_dest_first(args, &mut targets.keys)?
        }
        "zadd" | "zincrby" | "zrem" | "zremrangebylex" | "zremrangebyrank" | "zremrangebyscore"
        | "zpopmax" | "zpopmin" | "zrangestore" => {
            push_first_key(args, &mut targets.keys, AccessIntent::Write)?
        }
        "zcard" | "zcount" | "zlexcount" | "zmscore" | "zrandmember" | "zrange" | "zrangebylex"
        | "zrangebyscore" | "zrank" | "zrevrange" | "zrevrangebylex" | "zrevrangebyscore"
        | "zrevrank" | "zscan" | "zscore" => {
            push_first_key(args, &mut targets.keys, AccessIntent::Read)?
        }
        "zdiff" | "zinter" | "zunion" => {
            push_all_but_first_numeric(args, &mut targets.keys, AccessIntent::Read)?
        }
        "zdiffstore" | "zinterstore" | "zunionstore" => push_zstore_keys(args, &mut targets.keys)?,
        "xack" | "xadd" | "xclaim" | "xautoclaim" | "xdel" | "xdelex" | "xgroup" | "xsetid"
        | "xtrim" => push_first_key(args, &mut targets.keys, AccessIntent::Write)?,
        "xinfo" | "xlen" | "xpending" | "xrange" | "xrevrange" => {
            push_first_key(args, &mut targets.keys, AccessIntent::Read)?
        }
        "watch" => push_all_keys(args, &mut targets.keys, AccessIntent::Read)?,
        "subscribe" | "unsubscribe" | "psubscribe" | "punsubscribe" | "ssubscribe"
        | "sunsubscribe" => push_all_channels(args, &mut targets.channels)?,
        "publish" | "spublish" => push_first_channel(args, &mut targets.channels)?,
        _ => {}
    }
    Ok(targets)
}

fn push_first_key(
    args: &[Frame<'_>],
    out: &mut Vec<(CompactString, AccessIntent)>,
    intent: AccessIntent,
) -> Result<(), &'static str> {
    if let Some(first) = args.first() {
        out.push((
            parse_compact(frame_bytes(first).map_err(|_| "ERR syntax error")?)?,
            intent,
        ));
    }
    Ok(())
}

fn push_all_keys(
    args: &[Frame<'_>],
    out: &mut Vec<(CompactString, AccessIntent)>,
    intent: AccessIntent,
) -> Result<(), &'static str> {
    for arg in args {
        out.push((
            parse_compact(frame_bytes(arg).map_err(|_| "ERR syntax error")?)?,
            intent,
        ));
    }
    Ok(())
}

fn push_every_other_key(
    args: &[Frame<'_>],
    out: &mut Vec<(CompactString, AccessIntent)>,
) -> Result<(), &'static str> {
    for index in (0..args.len()).step_by(2) {
        out.push((
            parse_compact(frame_bytes(&args[index]).map_err(|_| "ERR syntax error")?)?,
            AccessIntent::Write,
        ));
    }
    Ok(())
}

fn push_two_keys(
    args: &[Frame<'_>],
    out: &mut Vec<(CompactString, AccessIntent)>,
) -> Result<(), &'static str> {
    if let Some(first) = args.first() {
        out.push((
            parse_compact(frame_bytes(first).map_err(|_| "ERR syntax error")?)?,
            AccessIntent::Write,
        ));
    }
    if let Some(second) = args.get(1) {
        out.push((
            parse_compact(frame_bytes(second).map_err(|_| "ERR syntax error")?)?,
            AccessIntent::Write,
        ));
    }
    Ok(())
}

fn push_all_but_last(
    args: &[Frame<'_>],
    out: &mut Vec<(CompactString, AccessIntent)>,
    intent: AccessIntent,
) -> Result<(), &'static str> {
    for arg in &args[..args.len().saturating_sub(1)] {
        out.push((
            parse_compact(frame_bytes(arg).map_err(|_| "ERR syntax error")?)?,
            intent,
        ));
    }
    Ok(())
}

fn push_numkeys(
    args: &[Frame<'_>],
    index: usize,
    out: &mut Vec<(CompactString, AccessIntent)>,
) -> Result<(), &'static str> {
    let Some(numkeys) = args.get(index) else {
        return Ok(());
    };
    let numkeys = std::str::from_utf8(frame_bytes(numkeys).map_err(|_| "ERR syntax error")?)
        .ok()
        .and_then(|text| text.parse::<usize>().ok())
        .ok_or("ERR syntax error")?;
    for arg in args.iter().skip(index + 1).take(numkeys) {
        out.push((
            parse_compact(frame_bytes(arg).map_err(|_| "ERR syntax error")?)?,
            AccessIntent::Write,
        ));
    }
    Ok(())
}

fn push_store_dest_first(
    args: &[Frame<'_>],
    out: &mut Vec<(CompactString, AccessIntent)>,
) -> Result<(), &'static str> {
    if let Some(dest) = args.first() {
        out.push((
            parse_compact(frame_bytes(dest).map_err(|_| "ERR syntax error")?)?,
            AccessIntent::Write,
        ));
    }
    for arg in args.iter().skip(1) {
        out.push((
            parse_compact(frame_bytes(arg).map_err(|_| "ERR syntax error")?)?,
            AccessIntent::Read,
        ));
    }
    Ok(())
}

fn push_all_but_first_numeric(
    args: &[Frame<'_>],
    out: &mut Vec<(CompactString, AccessIntent)>,
    intent: AccessIntent,
) -> Result<(), &'static str> {
    let Some(numkeys) = args.first() else {
        return Ok(());
    };
    let numkeys = std::str::from_utf8(frame_bytes(numkeys).map_err(|_| "ERR syntax error")?)
        .ok()
        .and_then(|text| text.parse::<usize>().ok())
        .ok_or("ERR syntax error")?;
    for arg in args.iter().skip(1).take(numkeys) {
        out.push((
            parse_compact(frame_bytes(arg).map_err(|_| "ERR syntax error")?)?,
            intent,
        ));
    }
    Ok(())
}

fn push_zstore_keys(
    args: &[Frame<'_>],
    out: &mut Vec<(CompactString, AccessIntent)>,
) -> Result<(), &'static str> {
    if let Some(dest) = args.first() {
        out.push((
            parse_compact(frame_bytes(dest).map_err(|_| "ERR syntax error")?)?,
            AccessIntent::Write,
        ));
    }
    push_all_but_first_numeric(&args[1..], out, AccessIntent::Read)
}

fn push_all_channels(args: &[Frame<'_>], out: &mut Vec<CompactString>) -> Result<(), &'static str> {
    for arg in args {
        out.push(parse_compact(
            frame_bytes(arg).map_err(|_| "ERR syntax error")?,
        )?);
    }
    Ok(())
}

fn push_first_channel(
    args: &[Frame<'_>],
    out: &mut Vec<CompactString>,
) -> Result<(), &'static str> {
    if let Some(first) = args.first() {
        out.push(parse_compact(
            frame_bytes(first).map_err(|_| "ERR syntax error")?,
        )?);
    }
    Ok(())
}

fn lookup_user<'a>(state: &'a AclState, username: &[u8]) -> Option<&'a AclUser> {
    std::str::from_utf8(username)
        .ok()
        .and_then(|name| state.users.get(name))
}

fn parse_compact(bytes: &[u8]) -> Result<CompactString, &'static str> {
    CompactString::from_utf8(bytes).map_err(|_| "ERR syntax error")
}

fn parse_hash(hex_value: &str) -> Result<PasswordHash, String> {
    if hex_value.len() != 64 {
        return Err("ERR Error in ACL SETUSER modifier '#...': Invalid password hash".to_owned());
    }
    let mut out = [0u8; 32];
    for (index, chunk) in hex_value.as_bytes().chunks_exact(2).enumerate() {
        out[index] = (decode_nibble(chunk[0]).ok_or_else(|| "invalid".to_owned())? << 4)
            | decode_nibble(chunk[1]).ok_or_else(|| "invalid".to_owned())?;
    }
    Ok(out)
}

fn decode_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn hash_password(password: &[u8]) -> PasswordHash {
    let mut hasher = Sha256::new();
    hasher.update(password);
    hasher.finalize().into()
}

fn log_auth_failure(username: &CompactString, meta: &ConnectionMeta) {
    log_denial(
        AclDenyReason::Auth,
        AclContext::Toplevel,
        username.clone(),
        meta,
        0,
    );
}

fn command_denied(
    command: &[u8],
    meta: &ConnectionMeta,
    context: AclContext,
    qbuf_len: usize,
) -> Vec<u8> {
    let object = CompactString::from_utf8(command).unwrap_or_else(|_| CompactString::const_new(""));
    log_denial(
        AclDenyReason::Command,
        context,
        object.clone(),
        meta,
        qbuf_len,
    );
    error_message(&format!(
        "NOPERM this user has no permissions to run the '{}' command",
        object
    ))
}

fn log_denial(
    reason: AclDenyReason,
    context: AclContext,
    object: CompactString,
    meta: &ConnectionMeta,
    qbuf_len: usize,
) {
    let cell = ACL_REGISTRY.get().expect("acl state not initialized");
    let current = cell.lock().expect("acl state lock poisoned").clone();
    let mut next = (*current).clone();
    next.push_log(AclLogEntry {
        count: 1,
        reason,
        context,
        object,
        username: meta.username.clone(),
        age_seconds: 0.0,
        client_info: CompactString::from(render_client_info(meta, qbuf_len)),
        entry_id: 0,
        first_seen_ms: now_ms(),
    });
    *cell.lock().expect("acl state lock poisoned") = Arc::new(next);
}

fn render_client_info(meta: &ConnectionMeta, qbuf_len: usize) -> String {
    format!(
        "id={} addr={} laddr={} name={} user={} qbuf={} cmd={}",
        meta.id,
        meta.peer_addr,
        meta.local_addr,
        meta.name.as_deref().unwrap_or(""),
        meta.username,
        qbuf_len,
        meta.last_cmd.as_deref().unwrap_or("NULL"),
    )
}

fn split_acl_line(line: &str) -> Vec<String> {
    line.split_whitespace().map(str::to_owned).collect()
}

fn bulk_value(value: &[u8]) -> Response {
    Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(value))))
}

fn bulk_response(bytes: Vec<u8>) -> Response {
    Response::Value(Some(SenkoValue::from(Bytes::from(bytes))))
}

fn ok_outcome(response: Vec<u8>) -> AclCommandOutcome {
    AclCommandOutcome {
        response,
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn render_reason(reason: AclDenyReason) -> String {
    match reason {
        AclDenyReason::Auth => "auth",
        AclDenyReason::Command => "command",
        AclDenyReason::Key => "key",
        AclDenyReason::Channel => "channel",
    }
    .to_owned()
}

fn render_context(context: AclContext) -> String {
    match context {
        AclContext::Toplevel => "toplevel",
        AclContext::Multi => "multi",
        AclContext::Lua => "lua",
    }
    .to_owned()
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

impl CommandRegistry {
    fn id_of(&self, name: &str) -> Option<u16> {
        self.ids.get(name).copied()
    }

    fn all_ids(&self) -> Vec<u16> {
        self.specs.iter().map(|spec| spec.id).collect()
    }

    fn ids_for_category(&self, flag: u32) -> Vec<u16> {
        self.specs
            .iter()
            .filter(|spec| (spec.categories & flag) != 0)
            .map(|spec| spec.id)
            .collect()
    }
}

fn command_registry() -> &'static CommandRegistry {
    COMMAND_REGISTRY.get_or_init(build_command_registry)
}

fn build_command_registry() -> CommandRegistry {
    let mut specs = Vec::new();
    let mut ids = HashMap::with_hasher(RandomState::new());
    let mut next_id = 0u16;
    let mut add = |name: &'static str, categories: u32| {
        ids.insert(name, next_id);
        specs.push(CommandSpec {
            id: next_id,
            name,
            categories,
        });
        next_id = next_id.saturating_add(1);
    };

    for name in ["ping", "echo"].iter().copied() {
        add(name, CATEGORY_CONNECTION | CATEGORY_FAST);
    }
    for name in ["quit", "reset", "select", "auth", "hello"].iter().copied() {
        add(name, CATEGORY_CONNECTION | CATEGORY_SLOW);
    }
    add(
        "client",
        CATEGORY_CONNECTION | CATEGORY_ADMIN | CATEGORY_SLOW,
    );
    for name in [
        "client|id",
        "client|getname",
        "client|setname",
        "client|setinfo",
        "client|info",
        "client|list",
        "client|no-evict",
        "client|no-touch",
        "client|reply",
        "client|caching",
        "client|getredir",
        "client|trackinginfo",
        "client|help",
        "client|kill",
        "client|pause",
        "client|unpause",
        "client|unblock",
        "client|tracking",
    ] {
        add(name, CATEGORY_CONNECTION | CATEGORY_ADMIN | CATEGORY_SLOW);
    }
    add("acl", CATEGORY_ADMIN | CATEGORY_SLOW | CATEGORY_DANGEROUS);
    for name in [
        "acl|setuser",
        "acl|getuser",
        "acl|deluser",
        "acl|list",
        "acl|users",
        "acl|whoami",
        "acl|cat",
        "acl|genpass",
        "acl|dryrun",
        "acl|log",
        "acl|load",
        "acl|save",
    ] {
        add(name, CATEGORY_ADMIN | CATEGORY_SLOW | CATEGORY_DANGEROUS);
    }
    for name in ["multi", "exec", "discard", "watch", "unwatch"]
        .iter()
        .copied()
    {
        add(name, CATEGORY_TRANSACTION | CATEGORY_SLOW);
    }
    for name in [
        "info", "time", "dbsize", "role", "lastsave", "lolwut", "cluster", "migrate", "wait",
        "waitaof",
    ] {
        add(name, CATEGORY_ADMIN | CATEGORY_SLOW | CATEGORY_DANGEROUS);
    }
    for name in [
        "del",
        "unlink",
        "exists",
        "expire",
        "pexpire",
        "expireat",
        "pexpireat",
        "ttl",
        "pttl",
        "expiretime",
        "pexpiretime",
        "persist",
        "keys",
        "move",
        "object",
        "randomkey",
        "rename",
        "renamenx",
        "copy",
        "restore",
        "dump",
        "touch",
        "scan",
        "type",
    ] {
        let cats = if matches!(
            name,
            "del"
                | "unlink"
                | "expire"
                | "pexpire"
                | "expireat"
                | "pexpireat"
                | "persist"
                | "move"
                | "rename"
                | "renamenx"
                | "copy"
                | "restore"
        ) {
            CATEGORY_KEYSPACE | CATEGORY_WRITE | CATEGORY_SLOW
        } else {
            CATEGORY_KEYSPACE | CATEGORY_READ | CATEGORY_SLOW
        };
        add(name, cats);
    }
    for name in [
        "get", "getrange", "getdel", "getex", "getset", "strlen", "substr",
    ]
    .iter()
    .copied()
    {
        add(name, CATEGORY_STRING | CATEGORY_READ | CATEGORY_FAST);
    }
    for name in [
        "set",
        "setex",
        "psetex",
        "setrange",
        "setnx",
        "append",
        "incr",
        "incrby",
        "incrbyfloat",
        "decr",
        "decrby",
        "delex",
        "delifex",
        "digest",
        "mget",
        "mset",
        "msetex",
        "msetnx",
        "lcs",
    ]
    .iter()
    .copied()
    {
        add(
            name,
            CATEGORY_STRING
                | if matches!(name, "mget" | "lcs") {
                    CATEGORY_READ
                } else {
                    CATEGORY_WRITE
                }
                | if matches!(name, "lcs") {
                    CATEGORY_SLOW
                } else {
                    CATEGORY_FAST
                },
        );
    }
    for name in [
        "hget",
        "hgetall",
        "hkeys",
        "hlen",
        "hmget",
        "hexists",
        "hrandfield",
        "hscan",
        "httl",
        "hpttl",
        "hvals",
        "hstrlen",
    ]
    .iter()
    .copied()
    {
        add(name, CATEGORY_HASH | CATEGORY_READ | CATEGORY_FAST);
    }
    for name in [
        "hset",
        "hsetnx",
        "hdel",
        "hmset",
        "hexpire",
        "hexpireat",
        "hexpiretime",
        "hpexpire",
        "hpexpireat",
        "hpexpiretime",
        "hpersist",
        "hgetdel",
        "hgetex",
        "hsetex",
        "hincrby",
        "hincrbyfloat",
    ]
    .iter()
    .copied()
    {
        add(name, CATEGORY_HASH | CATEGORY_WRITE | CATEGORY_FAST);
    }
    for name in ["llen", "lindex", "lrange", "lpos"].iter().copied() {
        add(name, CATEGORY_LIST | CATEGORY_READ | CATEGORY_FAST);
    }
    for name in [
        "lpush",
        "lpushx",
        "rpush",
        "rpushx",
        "lpop",
        "rpop",
        "lrem",
        "lset",
        "ltrim",
        "linsert",
        "lmove",
        "lmpop",
        "blmove",
        "blpop",
        "brpop",
        "blmpop",
        "rpoplpush",
        "brpoplpush",
    ]
    .iter()
    .copied()
    {
        add(
            name,
            CATEGORY_LIST
                | CATEGORY_WRITE
                | if name.starts_with('b') {
                    CATEGORY_BLOCKING | CATEGORY_SLOW
                } else {
                    CATEGORY_FAST
                },
        );
    }
    for name in ["sadd", "srem", "spop", "smove"].iter().copied() {
        add(name, CATEGORY_SET | CATEGORY_WRITE | CATEGORY_FAST);
    }
    for name in [
        "scard",
        "sdiff",
        "sdiffstore",
        "sinter",
        "sintercard",
        "sinterstore",
        "sismember",
        "smismember",
        "smembers",
        "srandmember",
        "sscan",
        "sunion",
        "sunionstore",
        "sort",
        "sort_ro",
    ]
    .iter()
    .copied()
    {
        add(
            name,
            CATEGORY_SET
                | if name.ends_with("store") {
                    CATEGORY_WRITE
                } else {
                    CATEGORY_READ
                }
                | CATEGORY_SLOW,
        );
    }
    for name in [
        "zadd",
        "zincrby",
        "zrem",
        "zremrangebylex",
        "zremrangebyrank",
        "zremrangebyscore",
        "zrangestore",
        "zpopmax",
        "zpopmin",
        "zmpop",
    ]
    .iter()
    .copied()
    {
        add(name, CATEGORY_SORTEDSET | CATEGORY_WRITE | CATEGORY_FAST);
    }
    for name in [
        "zcard",
        "zcount",
        "zdiff",
        "zdiffstore",
        "zinter",
        "zintercard",
        "zinterstore",
        "zlexcount",
        "zmscore",
        "zrandmember",
        "zrange",
        "zrangebylex",
        "zrangebyscore",
        "zrank",
        "zrevrange",
        "zrevrangebylex",
        "zrevrangebyscore",
        "zrevrank",
        "zscan",
        "zscore",
        "zunion",
        "zunionstore",
    ]
    .iter()
    .copied()
    {
        add(
            name,
            CATEGORY_SORTEDSET
                | if name.ends_with("store") {
                    CATEGORY_WRITE
                } else {
                    CATEGORY_READ
                }
                | CATEGORY_SLOW,
        );
    }
    for name in [
        "xadd",
        "xack",
        "xackdel",
        "xautoclaim",
        "xclaim",
        "xdel",
        "xdelex",
        "xgroup",
        "xsetid",
        "xtrim",
    ]
    .iter()
    .copied()
    {
        add(name, CATEGORY_STREAM | CATEGORY_WRITE | CATEGORY_FAST);
    }
    for name in [
        "xinfo",
        "xlen",
        "xpending",
        "xrange",
        "xrevrange",
        "xread",
        "xreadgroup",
    ]
    .iter()
    .copied()
    {
        add(
            name,
            CATEGORY_STREAM
                | CATEGORY_READ
                | CATEGORY_SLOW
                | if name.starts_with("xread") {
                    CATEGORY_BLOCKING
                } else {
                    0
                },
        );
    }
    for name in [
        "subscribe",
        "unsubscribe",
        "psubscribe",
        "punsubscribe",
        "publish",
        "pubsub",
        "ssubscribe",
        "sunsubscribe",
        "spublish",
    ]
    .iter()
    .copied()
    {
        add(name, CATEGORY_PUBSUB | CATEGORY_SLOW);
    }
    for name in [
        "pubsub|channels",
        "pubsub|numsub",
        "pubsub|numpat",
        "pubsub|shardchannels",
        "pubsub|shardnumsub",
    ]
    .iter()
    .copied()
    {
        add(name, CATEGORY_PUBSUB | CATEGORY_SLOW);
    }

    let mut categories = HashMap::with_hasher(RandomState::new());
    for (name, flag) in [
        ("read", CATEGORY_READ),
        ("write", CATEGORY_WRITE),
        ("set", CATEGORY_SET),
        ("sortedset", CATEGORY_SORTEDSET),
        ("list", CATEGORY_LIST),
        ("hash", CATEGORY_HASH),
        ("string", CATEGORY_STRING),
        ("bitmap", CATEGORY_BITMAP),
        ("hyperloglog", CATEGORY_HYPERLOGLOG),
        ("geo", CATEGORY_GEO),
        ("stream", CATEGORY_STREAM),
        ("pubsub", CATEGORY_PUBSUB),
        ("admin", CATEGORY_ADMIN),
        ("fast", CATEGORY_FAST),
        ("slow", CATEGORY_SLOW),
        ("blocking", CATEGORY_BLOCKING),
        ("dangerous", CATEGORY_DANGEROUS),
        ("connection", CATEGORY_CONNECTION),
        ("transaction", CATEGORY_TRANSACTION),
        ("scripting", CATEGORY_SCRIPTING),
        ("keyspace", CATEGORY_KEYSPACE),
        ("all", u32::MAX),
    ] {
        categories.insert(name, flag);
    }

    CommandRegistry {
        specs,
        ids,
        categories,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{ConnectionFlags, ReplyMode};
    use std::{
        fs,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        rc::Rc,
        sync::{Mutex, MutexGuard, OnceLock},
    };

    fn meta(username: &str) -> ConnectionMeta {
        ConnectionMeta {
            id: 1,
            username: CompactString::from(username),
            name: None,
            db: 0,
            flags: ConnectionFlags::AUTHENTICATED,
            created_at: 0,
            last_cmd: None,
            last_cmd_at: 0,
            lib_name: None,
            lib_ver: None,
            peer_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1000),
            local_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            resp_version: 3,
            no_evict: false,
            no_touch: false,
            reply_mode: ReplyMode::Normal,
            watch_count: 0,
            multi_queue_len: -1,
            tracking_redirect: -1,
            tracking_optin: false,
            tracking_optout: false,
            tracking_bcast: false,
            tracking_noloop: false,
            tracking_prefixes: SmallVec::new(),
            tracking_caching: None,
            replica_listening_port: None,
            replica_ip_address: None,
            replica_psync2: false,
            replica_eof: false,
            replica_ack_offset: 0,
        }
    }

    fn init_test_acl_with(config: SenkoConfig) -> SenkoConfig {
        if ACL_REGISTRY.get().is_none() {
            init(&config);
        } else {
            swap_state(AclState::new(&config));
        }
        config
    }

    fn init_test_acl() -> SenkoConfig {
        init_test_acl_with(SenkoConfig::default())
    }

    fn acl_test_guard() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("acl test lock poisoned")
    }

    fn temp_acl_path(name: &str) -> PathBuf {
        let unique = format!("senko-acl-{name}-{}-{}.acl", std::process::id(), now_ms());
        std::env::temp_dir().join(unique)
    }

    #[test]
    fn setuser_getuser_and_list_roundtrip() {
        let _guard = acl_test_guard();
        let _ = init_test_acl();
        let frames = [
            Frame::BulkString(b"alice"),
            Frame::BulkString(b"on"),
            Frame::BulkString(b">secret"),
            Frame::BulkString(b"~cache:*"),
            Frame::BulkString(b"+get"),
        ];
        let _ = acl_setuser(&frames).unwrap();
        let state = current_state();
        let alice = state.users.get("alice").unwrap();
        assert!(alice.enabled);
        assert!(!alice.nopass);
        assert_eq!(alice.key_patterns.len(), 1);
        assert!(
            alice
                .allowed_commands
                .check(command_registry().id_of("get").unwrap())
        );
        let line = render_acl_list_line(alice);
        assert!(line.contains("user alice on"));
        assert!(line.contains("~cache:*"));
    }

    #[test]
    fn command_and_key_permissions_are_checked() {
        let _guard = acl_test_guard();
        let _ = init_test_acl();
        let frames = [
            Frame::BulkString(b"alice"),
            Frame::BulkString(b"on"),
            Frame::BulkString(b"-@all"),
            Frame::BulkString(b"+get"),
            Frame::BulkString(b"~cache:*"),
        ];
        let _ = acl_setuser(&frames).unwrap();
        let m = meta("alice");
        assert!(
            check_permissions(
                &m,
                b"GET",
                &[Frame::BulkString(b"cache:foo")],
                AclContext::Toplevel,
                0
            )
            .is_ok()
        );
        assert!(
            check_permissions(
                &m,
                b"GET",
                &[Frame::BulkString(b"other:foo")],
                AclContext::Toplevel,
                0
            )
            .is_err()
        );
        assert!(
            check_permissions(
                &m,
                b"SET",
                &[Frame::BulkString(b"cache:foo"), Frame::BulkString(b"v")],
                AclContext::Toplevel,
                0
            )
            .is_err()
        );
    }

    #[test]
    fn channel_permissions_are_checked() {
        let _guard = acl_test_guard();
        let _ = init_test_acl();
        let frames = [
            Frame::BulkString(b"alice"),
            Frame::BulkString(b"on"),
            Frame::BulkString(b"-@all"),
            Frame::BulkString(b"+subscribe"),
            Frame::BulkString(b"&news.*"),
        ];
        let _ = acl_setuser(&frames).unwrap();
        let m = meta("alice");
        assert!(
            check_permissions(
                &m,
                b"SUBSCRIBE",
                &[Frame::BulkString(b"news.sports")],
                AclContext::Toplevel,
                0
            )
            .is_ok()
        );
        assert!(
            check_permissions(
                &m,
                b"SUBSCRIBE",
                &[Frame::BulkString(b"other")],
                AclContext::Toplevel,
                0
            )
            .is_err()
        );
    }

    #[test]
    fn genpass_returns_requested_hex_length() {
        let _guard = acl_test_guard();
        let pass = acl_genpass(&[Frame::BulkString(b"128")]).unwrap();
        let text = String::from_utf8(pass.response).unwrap();
        assert!(text.starts_with('$'));
    }

    #[test]
    fn default_user_backward_compat_tracks_requirepass() {
        let _guard = acl_test_guard();
        let _ = init_test_acl();
        assert!(connection_starts_authenticated());
        let mut default_meta = meta("default");
        default_meta.flags.remove(ConnectionFlags::AUTHENTICATED);
        authenticate(&mut default_meta, b"default", b"anything").unwrap();
        assert_eq!(default_meta.username, "default");
        assert!(default_meta.flags.contains(ConnectionFlags::AUTHENTICATED));

        let mut config = SenkoConfig::default();
        config.auth_password = Some("secret".to_owned());
        let _ = init_test_acl_with(config);
        assert!(!connection_starts_authenticated());

        let mut denied = meta("default");
        denied.flags.remove(ConnectionFlags::AUTHENTICATED);
        let err = authenticate(&mut denied, b"default", b"wrong").unwrap_err();
        assert!(String::from_utf8_lossy(&err).contains("WRONGPASS"));

        let mut accepted = meta("default");
        accepted.flags.remove(ConnectionFlags::AUTHENTICATED);
        authenticate(&mut accepted, b"default", b"secret").unwrap();
        assert_eq!(accepted.username, "default");
        assert!(accepted.flags.contains(ConnectionFlags::AUTHENTICATED));
    }

    #[test]
    fn dryrun_logs_denials_and_log_reset_clears_entries() {
        let _guard = acl_test_guard();
        let _ = init_test_acl();
        let frames = [
            Frame::BulkString(b"alice"),
            Frame::BulkString(b"on"),
            Frame::BulkString(b"-@all"),
            Frame::BulkString(b"+get"),
            Frame::BulkString(b"~cache:*"),
        ];
        let _ = acl_setuser(&frames).unwrap();

        let ok = acl_dryrun(&[
            Frame::BulkString(b"alice"),
            Frame::BulkString(b"get"),
            Frame::BulkString(b"cache:foo"),
        ])
        .unwrap();
        assert_eq!(ok.response, b"+OK\r\n");

        let err = acl_dryrun(&[
            Frame::BulkString(b"alice"),
            Frame::BulkString(b"set"),
            Frame::BulkString(b"cache:foo"),
            Frame::BulkString(b"value"),
        ])
        .unwrap_err();
        assert!(String::from_utf8_lossy(&err).contains("NOPERM"));

        let state = current_state();
        assert_eq!(state.log.len(), 1);
        assert_eq!(state.log[0].reason, AclDenyReason::Command);
        assert_eq!(state.log[0].username, "alice");

        let _ = acl_log(&[Frame::BulkString(b"RESET")], true).unwrap();
        assert!(current_state().log.is_empty());
    }

    #[test]
    fn acl_save_load_roundtrip_and_invalid_load_is_atomic() {
        let _guard = acl_test_guard();
        let path = temp_acl_path("roundtrip");
        let mut config = SenkoConfig::default();
        config.aclfile = Some(path.clone());
        let config = init_test_acl_with(config);
        let frames = [
            Frame::BulkString(b"alice"),
            Frame::BulkString(b"on"),
            Frame::BulkString(b">secret"),
            Frame::BulkString(b"~cache:*"),
            Frame::BulkString(b"+get"),
        ];
        let _ = acl_setuser(&frames).unwrap();

        acl_save(&[], &config).unwrap();
        let saved = fs::read_to_string(&path).unwrap();
        assert!(saved.contains("user alice on"));
        assert!(saved.contains("~cache:*"));

        fs::write(
            &path,
            "user default on nopass ~* &* +@all\nuser bob on nopass ~cache:* +get\n",
        )
        .unwrap();
        acl_load(&[], &config).unwrap();
        let loaded = current_state();
        assert!(loaded.users.contains_key("bob"));
        assert!(!loaded.users.contains_key("alice"));

        fs::write(&path, "not-an-acl-line\n").unwrap();
        let err = acl_load(&[], &config).unwrap_err();
        assert!(String::from_utf8_lossy(&err).contains("ERR Error in ACL file line 1"));
        let after_error = current_state();
        assert!(after_error.users.contains_key("bob"));
        assert!(!after_error.users.contains_key("alice"));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn acl_deluser_rejects_default_and_removes_regular_users() {
        let _guard = acl_test_guard();
        let _ = init_test_acl();
        let frames = [
            Frame::BulkString(b"alice"),
            Frame::BulkString(b"on"),
            Frame::BulkString(b"+get"),
            Frame::BulkString(b"~cache:*"),
        ];
        let _ = acl_setuser(&frames).unwrap();

        let err = acl_deluser(
            &[Frame::BulkString(b"default")],
            &meta("default"),
            &Rc::new(RefCell::new(ClientConnectionMap::default())),
            &Rc::new(RefCell::new(BlockedKeyRegistry::default())),
        )
        .unwrap_err();
        assert!(String::from_utf8_lossy(&err).contains("can't be removed"));

        let _ = acl_deluser(
            &[Frame::BulkString(b"alice")],
            &meta("default"),
            &Rc::new(RefCell::new(ClientConnectionMap::default())),
            &Rc::new(RefCell::new(BlockedKeyRegistry::default())),
        )
        .unwrap();
        assert!(!current_state().users.contains_key("alice"));
    }

    #[test]
    fn command_rules_render_default_deny_canonically() {
        let _guard = acl_test_guard();
        let _ = init_test_acl();
        let frames = [Frame::BulkString(b"alice"), Frame::BulkString(b"on")];
        let _ = acl_setuser(&frames).unwrap();
        let state = current_state();
        let alice = state.users.get("alice").unwrap();
        let line = render_acl_list_line(alice);
        assert!(line.contains("nopass"));
        assert!(line.contains("-@all"));
    }
}
