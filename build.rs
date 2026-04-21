fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use vendored protoc unless PROTOC is already set in the environment
    // (e.g. Docker image sets PROTOC=/usr/bin/protoc to use the system binary).
    if std::env::var("PROTOC").is_err() {
        let protoc = protoc_bin_vendored::protoc_bin_path()?;
        std::env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?)
                .join("control_pane_descriptor.bin"),
        )
        .compile_protos(&["proto/control_pane.proto"], &["proto/"])?;

    Ok(())
}
