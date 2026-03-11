use std::collections::VecDeque;

use compact_str::CompactString;

use crate::current_unix_ms;

#[derive(Debug, Clone)]
pub struct Notification {
    pub event: CompactString,
    pub payload: CompactString,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct RunningScript {
    pub pid: u32,
    pub script: CompactString,
    pub args: Vec<CompactString>,
    pub start_time: u64,
}

#[derive(Default)]
pub struct Notifier {
    events: VecDeque<Notification>,
    scripts: Vec<RunningScript>,
}

impl Notifier {
    pub fn emit(&mut self, event: impl Into<CompactString>, payload: impl Into<CompactString>) {
        if self.events.len() >= 1_024 {
            let _ = self.events.pop_front();
        }
        self.events.push_back(Notification {
            event: event.into(),
            payload: payload.into(),
            timestamp: current_unix_ms(),
        });
    }

    pub fn recent(&self) -> impl Iterator<Item = &Notification> {
        self.events.iter()
    }

    pub fn register_script(
        &mut self,
        pid: u32,
        script: impl Into<CompactString>,
        args: Vec<CompactString>,
    ) {
        self.scripts.push(RunningScript {
            pid,
            script: script.into(),
            args,
            start_time: current_unix_ms(),
        });
    }

    pub fn pending_scripts(&self) -> &[RunningScript] {
        &self.scripts
    }

    pub fn clear_finished_scripts(&mut self) {
        self.scripts.retain(|script| script.pid != 0);
    }
}
