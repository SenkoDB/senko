mod cli;
mod convert;

use std::{collections::HashSet, env, sync::Arc, thread};

use clap::Parser;
use cli::{Cli, Commands, check_config, load_effective_config};
use compio::driver::{DriverType, ProactorBuilder};
use compio::runtime::RuntimeBuilder;
use convert::write_converted_config;
use core_affinity::CoreId;
use mimalloc::MiMalloc;
use senko_core::{
    ModuleRegistry, SenkoModule, SenkoConfig, SenkoError, SenkoResult,
    render_default_config_toml,
};
use senko_net::{PreparedListener, prepare_listeners, run_shard};
use senko_sentinel::{
    cli::{SentinelCliAction, parse_process_args},
    config::SentinelConfig,
    run as run_sentinel,
};

#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;

fn desired_driver_type() -> DriverType {
    #[cfg(windows)]
    {
        DriverType::IOCP
    }
    #[cfg(target_os = "linux")]
    {
        DriverType::IoUring
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        DriverType::Poll
    }
}

fn shard_core(core_ids: &[CoreId], shard_index: usize) -> Option<CoreId> {
    core_ids.get(shard_index % core_ids.len().max(1)).copied()
}

fn runtime_builder_for(core_id: Option<CoreId>) -> RuntimeBuilder {
    let mut proactor = ProactorBuilder::new();
    proactor.driver_type(desired_driver_type());
    proactor.thread_pool_limit(1);

    let mut builder = RuntimeBuilder::new();
    builder.with_proactor(proactor);
    if let Some(core_id) = core_id {
        let mut affinity = HashSet::new();
        affinity.insert(core_id.id);
        builder.thread_affinity(affinity);
    }
    builder
}

fn spawn_shards(
    config: SenkoConfig,
    listeners: Vec<PreparedListener>,
    module_registry: Arc<ModuleRegistry>,
) -> Vec<thread::JoinHandle<SenkoResult<()>>> {
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    listeners
        .into_iter()
        .enumerate()
        .map(|(shard_index, listener)| {
            let config = config.clone();
            let core_id = shard_core(&core_ids, shard_index);
            let module_registry = Arc::clone(&module_registry);
            thread::Builder::new()
                .name(format!("senko-shard-{shard_index}"))
                .spawn(move || {
                    if let Some(core_id) = core_id {
                        let _ = core_affinity::set_for_current(core_id);
                    }
                    let runtime = runtime_builder_for(core_id).build()?;
                    runtime.block_on(run_shard(shard_index, config, listener, module_registry))
                })
                .expect("failed to spawn shard thread")
        })
        .collect()
}

fn run_server_mode(config: SenkoConfig) -> SenkoResult<()> {
    let listeners = prepare_listeners(&config)?;
    let workers = spawn_shards(config, listeners, built_in_modules());
    for worker in workers {
        worker
            .join()
            .map_err(|_| SenkoError::InvalidConfig("shard thread panicked"))??;
    }
    Ok(())
}

fn built_in_modules() -> Arc<ModuleRegistry> {
    let modules: Vec<Arc<dyn SenkoModule>> = Vec::new();
    #[cfg(any(feature = "module-json", feature = "json"))]
    let modules = {
        let mut modules = modules;
        modules.push(Arc::new(senko_json::JsonModule));
        modules
    };
    #[cfg(feature = "module-search")]
    let modules = {
        let mut modules = modules;
        modules.push(Arc::new(senko_search::SearchModule::new()));
        modules
    };
    #[cfg(feature = "module-ts")]
    let modules = {
        let mut modules = modules;
        modules.push(Arc::new(senko_ts::TsModule::new()));
        modules
    };
    #[cfg(feature = "module-prob")]
    let modules = {
        let mut modules = modules;
        modules.push(Arc::new(senko_prob::ProbModule));
        modules
    };
    #[cfg(feature = "module-vector")]
    let modules = {
        let mut modules = modules;
        modules.push(Arc::new(senko_vector::VectorModule::new()));
        modules
    };
    Arc::new(ModuleRegistry::new(modules))
}

fn run_sentinel_mode(config: SentinelConfig) -> SenkoResult<()> {
    let runtime = runtime_builder_for(None).build()?;
    runtime.block_on(run_sentinel(config))
}

fn run_app() -> SenkoResult<()> {
    if let Some(action) = parse_process_args(env::args_os())
        .map_err(|message| SenkoError::ProtocolMessage(message.into()))?
    {
        match action {
            SentinelCliAction::Run(config) => return run_sentinel_mode(config),
            SentinelCliAction::Print(output) => {
                if !output.is_empty() {
                    print!("{output}");
                }
                return Ok(());
            }
        }
    }

    let cli = Cli::parse();
    match cli.command {
        Some(Commands::CheckConfig { file }) => {
            check_config(&file).map_err(map_config_error)?;
            return Ok(());
        }
        Some(Commands::DefaultConfig) => {
            print!(
                "{}",
                render_default_config_toml().map_err(map_config_error)?
            );
            return Ok(());
        }
        Some(Commands::Version) => {
            println!("{}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Some(Commands::ConvertConfig { input, output }) => {
            let rendered =
                write_converted_config(&input, output.as_deref()).map_err(map_config_error)?;
            if output.is_none() {
                print!("{rendered}");
            }
            return Ok(());
        }
        Some(Commands::Start) | None => {}
    }

    let config = load_effective_config(&cli).map_err(map_config_error)?;
    run_server_mode(config)
}

fn map_config_error(error: senko_core::ConfigError) -> SenkoError {
    match error {
        senko_core::ConfigError::IoError(error) => SenkoError::Io(error),
        other => SenkoError::ProtocolMessage(other.to_string().into()),
    }
}

fn main() -> SenkoResult<()> {
    run_app()
}
