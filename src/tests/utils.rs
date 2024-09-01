use anyhow::{Context, Result};

use valkey_module::{ValkeyValue, ValkeyError};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

/// Ensure child process is killed both on normal exit and when panicking due to a failed test.
pub struct ChildGuard {
    pub name: &'static str,
    pub child: std::process::Child,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Err(e) = self.child.kill() {
            println!("Could not kill {}: {}", self.name, e);
        }
        if let Err(e) = self.child.wait() {
            println!("Could not wait for {}: {}", self.name, e);
        }
    }
}

pub fn start_redis_server_with_module(module_name: &str, port: u16) -> Result<ChildGuard> {
    let extension = if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };

    let profile = if cfg!(not(debug_assertions)) {
        "release"
    } else {
        "debug"
    };
    let module_path: PathBuf = [
        std::env::current_dir()?,
        PathBuf::from(format!(
            "target/{}/lib{}.{}",
            profile, module_name, extension
        )),
    ]
        .iter()
        .collect();

    let module_path = format!("{}", module_path.display());
    assert!(fs::metadata(&module_path)
        .with_context(|| format!("Loading valkey module: {}", module_path.as_str()))?
        .is_file());
    println!("Loading valkey module: {}", module_path.as_str());
    let args = &[
        "--port",
        &port.to_string(),
        "--loadmodule",
        module_path.as_str()
    ];

    let redis_server = Command::new("redis-server")
        .args(args)
        .spawn()
        .map(|c| ChildGuard {
            name: "redis-server",
            child: c,
        }).with_context(|| format!("Error in raising redis-server => {}", module_path.as_str()))?;

    Ok(redis_server)
}

pub fn stop_valkey_server(child: &mut ChildGuard) {
    child.child.kill().expect("Ohh no!");
}

pub fn error_cannot_find_iset_key(key_name: &str) -> String {
    format!("An error was signalled by the server: Interval Set '{key_name}' does not exist!", key_name = key_name)
}