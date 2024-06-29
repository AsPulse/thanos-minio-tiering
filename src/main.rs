use clap::Parser;
use dotenvy::dotenv;
use thiserror::Error;
use tracing::instrument;
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

use crate::features::delete_empty_blocks::delete_empty_blocks;

use self::error::SpanErr;
use self::features::delete_empty_blocks::DeleteEmptyBlocksError;
use self::minio::{MinioInstance, MinioInstanceInitializationError};

pub mod config;
pub mod error;
pub mod features;
pub mod minio;

#[derive(Error, Debug)]
enum ClientError {
    #[error("at least one feature must be enabled.")]
    NoFeatureEnabled,
    #[error("failed to initialize source MinIO instance; {0}")]
    SourceConfigError(#[source] MinioInstanceInitializationError),
    #[error(transparent)]
    DeleteEmptyBlocksError(#[from] DeleteEmptyBlocksError),
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let _ = dotenv();
    tracing_subscriber::Registry::default()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
        )
        .with(ErrorLayer::default())
        .try_init()
        .expect("failed to initialize tracing subscriber");

    tracing::info!("thanos-minio-tiering v{}", env!("CARGO_PKG_VERSION"));

    let start_time = std::time::Instant::now();

    match thanos_minio_tiering().await {
        Ok(_) => {
            tracing::info!("done! {}ms", start_time.elapsed().as_millis());
        }
        Err(e) => {
            tracing::error!("{}", e.error);
            eprintln!("{}", color_spantrace::colorize(&e.span));
        }
    }
}

#[instrument]
async fn thanos_minio_tiering() -> Result<(), SpanErr<ClientError>> {
    let args = config::AppArgs::parse();

    if args.dry_run {
        tracing::warn!("running in dry-run mode");
    }

    let source = MinioInstance::new(args.source_minio_config)
        .await
        .map_err(|e| e.map(ClientError::SourceConfigError))?;
    tracing::info!("source MinIO instance initialized.");

    if [args.delete_empty_blocks].iter().all(|&x| !x) {
        Err(ClientError::NoFeatureEnabled)?;
    }

    if args.delete_empty_blocks {
        delete_empty_blocks(&source, args.dry_run)
            .await
            .map_err(|e| e.map(ClientError::DeleteEmptyBlocksError))?;
    }

    Ok(())
}
