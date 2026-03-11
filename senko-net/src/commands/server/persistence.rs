use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use compio::runtime::spawn;
use senko_core::SenkoConfig;
use senko_proto::Frame;

use crate::{
    commands::server::{
        diagnostics,
        info::{self, ServerCommandOutcome},
    },
    connection::{error_bytes, error_message, frame_bytes, simple_string},
};

static SHUTDOWN_PENDING: AtomicBool = AtomicBool::new(false);

pub async fn execute(
    command: &[u8],
    args: &[Frame<'_>],
    _resp3: bool,
    config: &SenkoConfig,
) -> Option<Result<ServerCommandOutcome, Vec<u8>>> {
    if eq_ascii(command, b"SAVE") {
        return Some(handle_save(args, config).await);
    }
    if eq_ascii(command, b"BGSAVE") {
        return Some(handle_bgsave(args, config).await);
    }
    if eq_ascii(command, b"BGREWRITEAOF") {
        return Some(handle_bgrewriteaof(args));
    }
    if eq_ascii(command, b"FLUSHDB") {
        return Some(handle_flush(args, false).await);
    }
    if eq_ascii(command, b"FLUSHALL") {
        return Some(handle_flush(args, true).await);
    }
    if eq_ascii(command, b"SWAPDB") {
        return Some(handle_swapdb(args));
    }
    if eq_ascii(command, b"SHUTDOWN") {
        return Some(handle_shutdown(args, config).await);
    }
    None
}

async fn handle_save(
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<ServerCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'save' command",
        ));
    }
    save_once(config).await?;
    Ok(ok_outcome())
}

async fn handle_bgsave(
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<ServerCommandOutcome, Vec<u8>> {
    let scheduled = match args {
        [] => false,
        [flag]
            if eq_ascii(
                frame_bytes(flag).map_err(|error| error_bytes(&error))?,
                b"SCHEDULE",
            ) =>
        {
            true
        }
        _ => {
            return Err(error_message("ERR syntax error"));
        }
    };
    if info::bgsave_in_progress() {
        if scheduled {
            info::schedule_bgsave();
            return Ok(simple_outcome(b"Background saving started"));
        }
        return Err(error_message("ERR Background save already in progress"));
    }
    start_bgsave(config.clone());
    Ok(simple_outcome(b"Background saving started"))
}

fn handle_bgrewriteaof(args: &[Frame<'_>]) -> Result<ServerCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'bgrewriteaof' command",
        ));
    }
    info::set_aof_last_bgrewrite_status_ok();
    Ok(simple_outcome(
        b"Background append only file rewriting started",
    ))
}

async fn handle_flush(args: &[Frame<'_>], _all: bool) -> Result<ServerCommandOutcome, Vec<u8>> {
    let async_flush = match args {
        [] => false,
        [mode]
            if eq_ascii(
                frame_bytes(mode).map_err(|error| error_bytes(&error))?,
                b"ASYNC",
            ) =>
        {
            true
        }
        [mode]
            if eq_ascii(
                frame_bytes(mode).map_err(|error| error_bytes(&error))?,
                b"SYNC",
            ) =>
        {
            false
        }
        _ => return Err(error_message("ERR syntax error")),
    };
    if async_flush {
        info::flush_all_shards_async();
    } else {
        info::flush_all_shards_sync().await?;
    }
    Ok(ok_outcome())
}

fn handle_swapdb(args: &[Frame<'_>]) -> Result<ServerCommandOutcome, Vec<u8>> {
    if args.len() != 2 {
        return Err(error_message(
            "ERR wrong number of arguments for 'swapdb' command",
        ));
    }
    let left = parse_db_index(&args[0])?;
    let right = parse_db_index(&args[1])?;
    if left == 0 && right == 0 {
        return Ok(ok_outcome());
    }
    Err(error_message("ERR invalid DB index"))
}

async fn handle_shutdown(
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<ServerCommandOutcome, Vec<u8>> {
    let mut save = None;
    let mut abort = false;
    for arg in args {
        let token = frame_bytes(arg).map_err(|error| error_bytes(&error))?;
        if eq_ascii(token, b"SAVE") {
            save = Some(true);
            continue;
        }
        if eq_ascii(token, b"NOSAVE") {
            save = Some(false);
            continue;
        }
        if eq_ascii(token, b"NOW") || eq_ascii(token, b"FORCE") {
            continue;
        }
        if eq_ascii(token, b"ABORT") {
            abort = true;
            continue;
        }
        return Err(error_message("ERR syntax error"));
    }
    if abort {
        if SHUTDOWN_PENDING.swap(false, Ordering::SeqCst) {
            return Ok(ok_outcome());
        }
        return Err(error_message("ERR No shutdown in progress"));
    }

    let should_save = save.unwrap_or(!config.save.trim().is_empty());
    SHUTDOWN_PENDING.store(true, Ordering::SeqCst);
    if should_save {
        let _ = save_once(config).await;
    }
    spawn(async move {
        compio::time::sleep(Duration::from_millis(50)).await;
        std::process::exit(0);
    })
    .detach();
    Ok(ServerCommandOutcome {
        response: Vec::new(),
        close_after_write: true,
        suppress_response: true,
        force_send_response: false,
    })
}

async fn save_once(config: &SenkoConfig) -> Result<(), Vec<u8>> {
    let started = Instant::now();
    info::set_bgsave_in_progress(true);
    match info::save_rdb_snapshot(config).await {
        Ok(()) => {
            info::record_save_success(started.elapsed());
            diagnostics::record_bgsave_latency(started.elapsed());
            Ok(())
        }
        Err(error) => {
            info::record_save_failure();
            Err(error_message(&error))
        }
    }
}

fn start_bgsave(config: SenkoConfig) {
    info::set_bgsave_in_progress(true);
    spawn(async move {
        let started = Instant::now();
        let result = info::save_rdb_snapshot(&config).await;
        match result {
            Ok(()) => {
                info::record_save_success(started.elapsed());
                diagnostics::record_bgsave_latency(started.elapsed());
            }
            Err(_) => info::record_save_failure(),
        }
        if info::take_scheduled_bgsave() {
            start_bgsave(config);
        }
    })
    .detach();
}

fn parse_db_index(frame: &Frame<'_>) -> Result<u64, Vec<u8>> {
    let bytes = frame_bytes(frame).map_err(|error| error_bytes(&error))?;
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| error_message("ERR value is not an integer or out of range"))
}

fn ok_outcome() -> ServerCommandOutcome {
    ServerCommandOutcome {
        response: simple_string(b"OK"),
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

fn simple_outcome(message: &'static [u8]) -> ServerCommandOutcome {
    ServerCommandOutcome {
        response: simple_string(message),
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}
