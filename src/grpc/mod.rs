pub mod handler;

// Re-export the tonic-generated module.
pub mod proto {
    tonic::include_proto!("mivi.controlpane.v1");
}

pub use handler::ControlPaneService;
pub use proto::control_pane_server::ControlPaneServer;
