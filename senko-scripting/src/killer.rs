use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Default)]
pub struct ScriptKiller {
    kill_requested: AtomicBool,
    aborted: AtomicBool,
}

impl ScriptKiller {
    pub fn request_kill(&self) {
        self.kill_requested.store(true, Ordering::Release);
    }

    pub fn is_kill_requested(&self) -> bool {
        self.kill_requested.load(Ordering::Acquire)
    }

    pub fn mark_aborted(&self) {
        self.aborted.store(true, Ordering::Release);
    }

    pub fn was_aborted(&self) -> bool {
        self.aborted.load(Ordering::Acquire)
    }

    pub fn reset(&self) {
        self.kill_requested.store(false, Ordering::Release);
        self.aborted.store(false, Ordering::Release);
    }
}
