use std::path::PathBuf;

use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
#[clap(
    name = "thanos-minio-tiering",
    version = env!("CARGO_PKG_VERSION"),
    author = "AsPulse",
    about = "A tool to tier data from MinIO to Thanos, and other chores."
)]
pub(crate) struct AppArgs {
    #[clap(default_value = "false", long)]
    pub dry_run: bool,

    #[clap(long)]
    pub source_minio_config: PathBuf,

    #[clap(default_value = "false", long)]
    pub delete_empty_blocks: bool,

    #[clap(default_value = "false", long)]
    pub delete_all_version: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct ThanosConfig {
    pub(crate) config: MinioConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct MinioConfig {
    pub(crate) endpoint: String,
    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    pub(crate) bucket: String,
    pub(crate) insecure: bool,
}
