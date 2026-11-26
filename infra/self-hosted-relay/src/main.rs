use std::{env, error::Error, ffi::OsStr, fs, path::PathBuf};

use self_hosted_relay::{Config, serve};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args_os().skip(1);
    let path = match (args.next(), args.next(), args.next()) {
        (Some(flag), Some(path), None) if flag == "--config" => PathBuf::from(path),
        (None, None, None) => env::var_os("RELAY_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|| "relay.toml".into()),
        (Some(flag), None, None) if flag == OsStr::new("--help") || flag == OsStr::new("-h") => {
            println!(
                "Usage: self-hosted-relay [--config PATH]\nDefault: RELAY_CONFIG or relay.toml"
            );
            return Ok(());
        }
        _ => return Err("usage: self-hosted-relay [--config PATH]".into()),
    };
    let config = Config::parse(&fs::read_to_string(path)?)?;
    let listener = TcpListener::bind(config.listen_addr).await?;
    eprintln!("Relay listening on {}", listener.local_addr()?);
    Ok(serve(listener, config).await?)
}
