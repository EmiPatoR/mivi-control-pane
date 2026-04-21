use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing::info;

mod config;
mod error;
mod grpc;
mod holoscan;
mod nats;
mod session;

use config::Config;
use grpc::{ControlPaneServer, ControlPaneService};
use holoscan::HoloscanAdapter;
use nats::NatsPublisher;
use session::SessionRegistry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env if present (development convenience)
    let _ = dotenvy::dotenv();

    // Load config eagerly — fail fast on bad values
    let cfg = Config::load().map_err(|e| anyhow::anyhow!("config error: {e}"))?;

    // Tracing
    let filter = tracing_subscriber::EnvFilter::try_new(&cfg.log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!(
        grpc_port = cfg.grpc_port,
        holoscan_host = %cfg.holoscan_host,
        holoscan_health_port = cfg.holoscan_health_port,
        holoscan_command_port = cfg.holoscan_command_port,
        nats_url = %cfg.nats_url,
        "mivi-control-pane starting"
    );

    // NATS publisher
    let publisher = Arc::new(
        NatsPublisher::new(&cfg.nats_url)
            .await
            .map_err(|e| anyhow::anyhow!("NATS connect failed: {e}"))?,
    );

    // Holoscan adapter
    let holoscan = Arc::new(
        HoloscanAdapter::new(
            &cfg.holoscan_host,
            cfg.holoscan_health_port,
            cfg.holoscan_command_port,
            cfg.command_timeout,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Holoscan adapter init failed: {e}"))?,
    );

    // Session registry
    let registry = Arc::new(SessionRegistry::new());

    // Background health monitor
    {
        let holoscan_bg = Arc::clone(&holoscan);
        let interval = cfg.health_check_interval;
        tokio::spawn(async move {
            holoscan_bg.run_health_monitor(interval).await;
        });
    }

    // gRPC service
    let service = ControlPaneService {
        registry,
        holoscan,
        publisher,
        start_exam_timeout: cfg.start_exam_timeout,
    };

    let addr: SocketAddr = format!("0.0.0.0:{}", cfg.grpc_port).parse()?;
    info!(addr = %addr, "gRPC server listening");

    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/control_pane_descriptor.bin"
        )))
        .build_v1()
        .expect("gRPC reflection build failed");

    Server::builder()
        .add_service(reflection)
        .add_service(ControlPaneServer::new(service))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    info!("mivi-control-pane stopped");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("ctrl-c handler failed");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler failed")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received");
}
