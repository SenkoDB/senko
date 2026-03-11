use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use crate::{
    conf_writer::write_sentinel_conf,
    config::{ConfigError, SentinelConfig, load_sentinel_config, render_default_config_toml},
};

pub enum SentinelCliAction {
    Run(SentinelConfig),
    Print(String),
}

pub fn parse_process_args<I>(args: I) -> Result<Option<SentinelCliAction>, String>
where
    I: IntoIterator<Item = OsString>,
{
    let args = args
        .into_iter()
        .map(|value| value.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    if args.len() <= 1 {
        return Ok(None);
    }
    if args[1] == "--sentinel" {
        return parse_sentinel_namespace(&args[2..]).map(Some);
    }
    let direct = PathBuf::from(&args[1]);
    if detect_direct_sentinel_path(&direct) {
        return parse_direct_mode(&direct, &args[2..]).map(Some);
    }
    Ok(None)
}

pub fn detect_direct_sentinel_path(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|ext| ext.to_str()) else {
        return false;
    };
    if !matches!(ext, "conf" | "toml") || !path.is_file() {
        return false;
    }
    let Ok(content) = fs::read_to_string(path) else {
        return false;
    };
    if ext == "toml" {
        return content.contains("[[masters]]");
    }
    content.to_ascii_lowercase().contains("sentinel monitor")
}

fn parse_direct_mode(path: &Path, args: &[String]) -> Result<SentinelCliAction, String> {
    if args.is_empty() {
        return load_sentinel_config(path)
            .map(SentinelCliAction::Run)
            .map_err(display_error);
    }
    parse_command(args)
}

fn parse_sentinel_namespace(args: &[String]) -> Result<SentinelCliAction, String> {
    if args.is_empty() {
        return Err("missing sentinel config path or subcommand after --sentinel".to_owned());
    }
    let first = PathBuf::from(&args[0]);
    if first.exists() {
        if args.len() == 1 {
            return load_sentinel_config(&first)
                .map(SentinelCliAction::Run)
                .map_err(display_error);
        }
        return parse_command(&args[1..]);
    }
    parse_command(args)
}

fn parse_command(args: &[String]) -> Result<SentinelCliAction, String> {
    match args.first().map(String::as_str) {
        Some("check-config") => {
            let path = path_arg(args, 1, "check-config <file>")?;
            let config = load_sentinel_config(&path).map_err(display_error)?;
            Ok(SentinelCliAction::Print(format!(
                "valid sentinel config: {}\n",
                config
                    .config_file
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| path.display().to_string())
            )))
        }
        Some("default-config") => Ok(SentinelCliAction::Print(render_default_config_toml())),
        Some("convert") => {
            let input = path_arg(args, 1, "convert <input.conf> [-o output.toml]")?;
            let config = load_sentinel_config(&input).map_err(display_error)?;
            let output = config.normalized_toml().map_err(display_error)?;
            if let Some(path) = output_path(args)? {
                fs::write(path, &output).map_err(|error| error.to_string())?;
                Ok(SentinelCliAction::Print(String::new()))
            } else {
                Ok(SentinelCliAction::Print(output))
            }
        }
        Some("convert-to-conf") => {
            let input = path_arg(args, 1, "convert-to-conf <input.toml> [-o output.conf]")?;
            let config = load_sentinel_config(&input).map_err(display_error)?;
            let output = write_sentinel_conf(&config);
            if let Some(path) = output_path(args)? {
                fs::write(path, &output).map_err(|error| error.to_string())?;
                Ok(SentinelCliAction::Print(String::new()))
            } else {
                Ok(SentinelCliAction::Print(output))
            }
        }
        Some("show-config") => {
            let path = path_arg(args, 1, "show-config <file>")?;
            let config = load_sentinel_config(&path).map_err(display_error)?;
            Ok(SentinelCliAction::Print(
                config.normalized_toml().map_err(display_error)?,
            ))
        }
        Some(other) => Err(format!("unknown sentinel subcommand: {other}")),
        None => Err("missing sentinel subcommand".to_owned()),
    }
}

fn output_path(args: &[String]) -> Result<Option<PathBuf>, String> {
    if args.len() >= 4 && matches!(args[2].as_str(), "-o" | "--output") {
        return Ok(Some(PathBuf::from(&args[3])));
    }
    Ok(None)
}

fn path_arg(args: &[String], index: usize, usage: &str) -> Result<PathBuf, String> {
    args.get(index)
        .map(PathBuf::from)
        .ok_or_else(|| format!("usage: {usage}"))
}

fn display_error(error: ConfigError) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_file(ext: &str, content: &str) -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("senko-sentinel-cli-{ts}.{ext}"));
        fs::write(&path, content).expect("write");
        path
    }

    #[test]
    fn detects_direct_sentinel_toml() {
        let path = unique_file(
            "toml",
            r#"[[masters]]
name="m"
host="127.0.0.1"
port=6379
quorum=2"#,
        );
        assert!(detect_direct_sentinel_path(&path));
    }

    #[test]
    fn parses_default_config_subcommand() {
        let args = vec![
            "senkodb".into(),
            "--sentinel".into(),
            "default-config".into(),
        ];
        let action = parse_process_args(args).expect("parse").expect("action");
        match action {
            SentinelCliAction::Print(output) => assert!(output.contains("[[masters]]")),
            SentinelCliAction::Run(_) => panic!("expected print"),
        }
    }
}
