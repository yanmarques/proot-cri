use std::process::exit;

use tokio::signal::unix::{signal, SignalKind};

///
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let exitcode: i32 = std::env::args()
        .nth(1)
        .and_then(|e| Some(e.parse::<i32>().expect("exit code must be a number")))
        .unwrap_or(0);

    let mut stream = signal(SignalKind::terminate())?;
    stream.recv().await;
    println!("received SIGTERM");
    exit(exitcode);
}
