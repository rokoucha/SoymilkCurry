use std::{env, path::Path, sync::Arc};

use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::{config::Config, tuner::AppState};

mod api;
mod config;
mod tuner;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "config.yml".to_owned());
    let config = Config::load(Path::new(&config_path)).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(2);
    });
    let listen = config.listen;
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .unwrap_or_else(|error| {
            eprintln!("failed to listen on {listen}: {error}");
            std::process::exit(2);
        });
    info!(%listen, config = %config_path, "server started");

    if let Err(error) = axum::serve(listener, api::router(Arc::new(AppState::new(config))))
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        error!(%error, "server stopped with an error");
        std::process::exit(1);
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
