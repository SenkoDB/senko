use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    let build_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    println!("cargo:rustc-env=SENKO_BUILD_ID={build_id:016x}");
}
