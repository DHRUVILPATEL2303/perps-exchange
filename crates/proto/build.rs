fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/market.proto",
                "proto/account.proto",
                "proto/trading.proto",
                "proto/risk.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
