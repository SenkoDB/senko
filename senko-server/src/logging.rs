use std::{
    fmt::Write as _,
    fs::OpenOptions,
    io::{self, IsTerminal, Write},
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use senko_core::{SenkoConfig, SenkoError, SenkoResult, config::LogLevel};
use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{Field, Visit},
    level_filters::LevelFilter,
    span::{Attributes, Id, Record},
};

#[derive(Clone)]
struct SharedWriter {
    inner: Arc<Mutex<Box<dyn Write + Send>>>,
    color: bool,
}

impl SharedWriter {
    fn stdout() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Box::new(io::stdout()))),
            color: io::stdout().is_terminal(),
        }
    }

    fn file(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Box::new(file))),
            color: false,
        })
    }

    fn write_line(&self, line: &str) {
        let Ok(mut writer) = self.inner.lock() else {
            return;
        };
        let _ = writer.write_all(line.as_bytes());
        let _ = writer.flush();
    }
}

pub fn init(config: &SenkoConfig) -> SenkoResult<()> {
    let writer = match config.general.logfile.as_deref() {
        Some(path) => SharedWriter::file(path).map_err(SenkoError::Io)?,
        None => SharedWriter::stdout(),
    };
    let logger = SenkoLogger {
        writer,
        max_level: level_filter(config.general.loglevel),
    };
    tracing::subscriber::set_global_default(logger)
        .map_err(|_| SenkoError::ProtocolMessage("failed to install global logger".into()))?;
    if config.general.syslog_enabled {
        tracing::warn!(
            ident = %config.general.syslog_ident,
            facility = %config.general.syslog_facility,
            "syslog is configured but not implemented; using the configured tracing sink"
        );
    }
    Ok(())
}

struct SenkoLogger {
    writer: SharedWriter,
    max_level: LevelFilter,
}

impl SenkoLogger {
    fn enabled_level(&self, level: &Level) -> bool {
        match self.max_level {
            LevelFilter::OFF => false,
            LevelFilter::ERROR => matches!(*level, Level::ERROR),
            LevelFilter::WARN => matches!(*level, Level::ERROR | Level::WARN),
            LevelFilter::INFO => matches!(*level, Level::ERROR | Level::WARN | Level::INFO),
            LevelFilter::DEBUG => {
                matches!(
                    *level,
                    Level::ERROR | Level::WARN | Level::INFO | Level::DEBUG
                )
            }
            LevelFilter::TRACE => true,
        }
    }

    fn log(&self, metadata: &Metadata<'_>, fields: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let seconds = now.as_secs();
        let millis = now.subsec_millis();
        let mut line = String::with_capacity(fields.len() + 96);
        let level = if self.writer.color {
            color_level_tag(metadata.level())
        } else {
            level_tag(metadata.level())
        };
        let _ = write!(
            line,
            "[{seconds}.{millis:03}] {} {}",
            level,
            metadata.target()
        );
        if !fields.is_empty() {
            line.push(' ');
            line.push_str(fields);
        }
        line.push('\n');
        self.writer.write_line(&line);
    }
}

impl Subscriber for SenkoLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.enabled_level(metadata.level())
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let metadata = event.metadata();
        if !self.enabled(metadata) {
            return;
        }
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        self.log(metadata, &visitor.finish());
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}

    fn register_callsite(
        &self,
        metadata: &'static Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if self.enabled(metadata) {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(self.max_level)
    }
}

#[derive(Default)]
struct FieldVisitor {
    message: Option<String>,
    fields: Vec<(String, String)>,
}

impl FieldVisitor {
    fn record_value(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = Some(value);
        } else {
            self.fields.push((field.name().to_owned(), value));
        }
    }

    fn finish(self) -> String {
        let mut out = String::new();
        if let Some(message) = self.message {
            out.push_str(&message);
        }
        for (key, value) in self.fields {
            if !out.is_empty() {
                out.push(' ');
            }
            let _ = write!(out, "{key}={value}");
        }
        out
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record_value(field, format!("{value:?}"));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_value(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_value(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field, value.to_string());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_value(field, value.to_owned());
    }
}

fn level_filter(level: LogLevel) -> LevelFilter {
    match level {
        LogLevel::Debug => LevelFilter::DEBUG,
        LogLevel::Verbose => LevelFilter::TRACE,
        LogLevel::Notice => LevelFilter::INFO,
        LogLevel::Warning => LevelFilter::WARN,
        LogLevel::Nothing => LevelFilter::OFF,
    }
}

fn level_tag(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARN",
        Level::INFO => "INFO",
        Level::DEBUG => "DEBUG",
        Level::TRACE => "TRACE",
    }
}

fn color_level_tag(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "\x1b[31mERROR\x1b[0m",
        Level::WARN => "\x1b[33mWARN\x1b[0m",
        Level::INFO => "\x1b[32mINFO\x1b[0m",
        Level::DEBUG => "\x1b[34mDEBUG\x1b[0m",
        Level::TRACE => "\x1b[90mTRACE\x1b[0m",
    }
}
