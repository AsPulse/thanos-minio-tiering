use clap::Parser;
use dotenvy::dotenv;

pub mod config;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let _ = dotenv();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    tracing::info!("thanos-minio-tiering v{}", env!("CARGO_PKG_VERSION"));

    let start_time = std::time::Instant::now();

    match thanos_minio_tiering().await {
        Ok(_) => {
            tracing::info!("done! {}ms", start_time.elapsed().as_millis());
        }
        Err(e) => {
            tracing::error!("error: {:?}", e);
        }
    }
}

async fn thanos_minio_tiering() -> Result<(), Box<dyn std::error::Error>> {
    let args = config::AppArgs::parse();

    Ok(())
}
