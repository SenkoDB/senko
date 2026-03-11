use std::{
    fs,
    fs::OpenOptions,
    io,
    path::{Path, PathBuf},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use rand::{RngCore, SeedableRng, rngs::SmallRng};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunId {
    raw: [u8; 20],
    hex: [u8; 40],
}

#[derive(Debug)]
pub enum RunIdError {
    InvalidLength,
    InvalidHex,
}

impl RunId {
    pub fn generate() -> Self {
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_owned());
        let pid = std::process::id();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut hasher = blake3::Hasher::new_keyed(b"senko-sentinel-runid-seed-32byte");
        hasher.update(hostname.as_bytes());
        hasher.update(&pid.to_le_bytes());
        hasher.update(&now.to_le_bytes());
        let mut raw = [0u8; 20];
        raw.copy_from_slice(&hasher.finalize().as_bytes()[..20]);
        let mut rng = SmallRng::from_entropy();
        let mut extra = [0u8; 20];
        rng.fill_bytes(&mut extra);
        for (byte, extra) in raw.iter_mut().zip(extra) {
            *byte ^= extra;
        }
        Self {
            raw,
            hex: encode_hex(raw),
        }
    }

    pub fn as_hex(&self) -> &str {
        std::str::from_utf8(&self.hex).expect("runid hex is ascii")
    }

    pub fn from_hex(input: &str) -> Result<Self, RunIdError> {
        if input.len() != 40 {
            return Err(RunIdError::InvalidLength);
        }
        let mut raw = [0u8; 20];
        for (index, chunk) in input.as_bytes().chunks_exact(2).enumerate() {
            raw[index] = decode_hex_byte(chunk[0], chunk[1])?;
        }
        Ok(Self {
            raw,
            hex: encode_hex(raw),
        })
    }
}

pub fn load_or_generate_runid(path: &Path) -> Result<RunId, io::Error> {
    fs::create_dir_all(path.parent().unwrap_or_else(|| Path::new(".")))?;
    let lock = path.with_extension("runid.lock");
    loop {
        if let Ok(existing) = fs::read_to_string(path)
            && let Ok(runid) = RunId::from_hex(existing.lines().next().unwrap_or("").trim())
        {
            return Ok(runid);
        }

        match OpenOptions::new().write(true).create_new(true).open(&lock) {
            Ok(_guard) => {
                if let Ok(existing) = fs::read_to_string(path)
                    && let Ok(runid) = RunId::from_hex(existing.lines().next().unwrap_or("").trim())
                {
                    let _ = fs::remove_file(&lock);
                    return Ok(runid);
                }
                let runid = RunId::generate();
                let result = write_runid_atomic(path, &runid);
                let _ = fs::remove_file(&lock);
                result?;
                return Ok(runid);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                thread::yield_now();
            }
            Err(error) => return Err(error),
        }
    }
}

fn write_runid_atomic(path: &Path, runid: &RunId) -> Result<(), io::Error> {
    let tmp = unique_tmp(path);
    fs::write(&tmp, format!("{}\n", runid.as_hex()))?;
    fs::File::open(&tmp)?.sync_data()?;
    match fs::rename(&tmp, path) {
        Ok(()) => Ok(()),
        Err(_error) if path.exists() => {
            let _ = fs::remove_file(&tmp);
            let _ = fs::read_to_string(path)?;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn unique_tmp(path: &Path) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.with_extension(format!("runid.tmp.{now}"))
}

fn encode_hex(raw: [u8; 20]) -> [u8; 40] {
    let mut hex = [0u8; 40];
    for (index, byte) in raw.iter().copied().enumerate() {
        hex[index * 2] = nybble_to_hex(byte >> 4);
        hex[index * 2 + 1] = nybble_to_hex(byte & 0x0f);
    }
    hex
}

fn nybble_to_hex(value: u8) -> u8 {
    match value {
        0..=9 => b'0' + value,
        10..=15 => b'a' + (value - 10),
        _ => unreachable!("nybble"),
    }
}

fn decode_hex_byte(high: u8, low: u8) -> Result<u8, RunIdError> {
    Ok((decode_nybble(high)? << 4) | decode_nybble(low)?)
}

fn decode_nybble(value: u8) -> Result<u8, RunIdError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(RunIdError::InvalidHex),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, Mutex},
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn unique_file() -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("senko-runid-{ts}"))
    }

    #[test]
    fn generate_write_and_load_round_trip() {
        let path = unique_file();
        let runid = load_or_generate_runid(&path).expect("generate");
        let loaded = load_or_generate_runid(&path).expect("load");
        assert_eq!(runid, loaded);
    }

    #[test]
    fn invalid_file_is_regenerated() {
        let path = unique_file();
        fs::write(&path, "bad").expect("write");
        let runid = load_or_generate_runid(&path).expect("regenerate");
        assert_eq!(runid.as_hex().len(), 40);
    }

    #[test]
    fn concurrent_load_or_generate_converges() {
        let path = unique_file();
        let values = Arc::new(Mutex::new(Vec::new()));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let path = path.clone();
            let values = values.clone();
            threads.push(thread::spawn(move || {
                let runid = load_or_generate_runid(&path).expect("load");
                values.lock().expect("lock").push(runid.as_hex().to_owned());
            }));
        }
        for handle in threads {
            handle.join().expect("join");
        }
        let values = values.lock().expect("lock");
        assert!(values.iter().all(|value| value == &values[0]));
    }
}
