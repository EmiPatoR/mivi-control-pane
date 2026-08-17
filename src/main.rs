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

    // Terminal-session pruning: without it the registry grew for the life of
    // the process and a re-used exam_id was ALREADY_EXISTS forever. Hourly
    // grace keeps just-stopped exams queryable.
    {
        let registry_bg = Arc::clone(&registry);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(600));
            loop {
                tick.tick().await;
                let pruned = registry_bg.prune_terminal(chrono::Duration::hours(1)).await;
                if pruned > 0 {
                    tracing::info!(pruned, "pruned terminal sessions from registry");
                }
            }
        });
    }

    // Background health monitor
    {
        let holoscan_bg = Arc::clone(&holoscan);
        let interval = cfg.health_check_interval;
        tokio::spawn(async move {
            holoscan_bg.run_health_monitor(interval).await;
        });
    }

    // Heartbeat publisher: emits mivi.controlpane.heartbeat + mivi.holoscan.heartbeat every interval.
    // Lets the backend detect crashes in < 1 s without polling.
    {
        let pub_bg = Arc::clone(&publisher);
        let holoscan_bg = Arc::clone(&holoscan);
        let interval = cfg.heartbeat_interval;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                pub_bg.publish(nats::PipelineEvent::ControlPaneHeartbeat).await;
                pub_bg.publish(nats::PipelineEvent::HoloscanHeartbeat {
                    healthy: holoscan_bg.is_healthy(),
                    reason: holoscan_bg.last_health_reason(),
                    rtt_ms: holoscan_bg.last_rtt_ms(),
                }).await;
            }
        });
    }

    // gRPC service
    let service = ControlPaneService {
        registry,
        holoscan,
        publisher,
        start_exam_timeout: cfg.start_exam_timeout,
        holoscan_status: Arc::new(Default::default()),
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
