fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("./src/pb")
        .compile_protos(&["./proto/api.proto"], &["./proto"])?;
    Ok(())
}
