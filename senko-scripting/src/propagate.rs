use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropagationEntry {
    pub db_id: u8,
    pub cmd: Vec<Bytes>,
    pub flags: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptPropagation {
    pub pending: Vec<PropagationEntry>,
    pub repl_flags: u8,
}

impl Default for ScriptPropagation {
    fn default() -> Self {
        Self {
            pending: Vec::new(),
            repl_flags: 3,
        }
    }
}

impl ScriptPropagation {
    pub const REPL_NONE: u8 = 0;
    pub const REPL_AOF: u8 = 1;
    pub const REPL_REPLICA: u8 = 2;
    pub const REPL_ALL: u8 = 3;

    pub fn push(&mut self, db_id: u8, command: &[Bytes]) {
        self.pending.push(PropagationEntry {
            db_id,
            cmd: command.to_vec(),
            flags: self.repl_flags,
        });
    }

    pub fn clear(&mut self) {
        self.pending.clear();
        self.repl_flags = Self::REPL_ALL;
    }

    pub fn set_flags(&mut self, flags: u8) {
        self.repl_flags = flags;
    }
}
