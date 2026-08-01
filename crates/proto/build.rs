

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(
            &[
                "proto/market.proto",
                "proto/account.proto",
                "proto/trading.proto",
                "proto/risk.proto",
                "proto/chart.proto"
            ],
            &["proto"],
        )?;

    Ok(())
}
