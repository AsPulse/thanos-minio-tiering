use clap::Parser;

#[derive(Parser, Debug)]
#[clap(
    name = "thanos-minio-tiering",
    version = env!("CARGO_PKG_VERSION"),
    author = "AsPulse",
    about = "A tool to tier data from MinIO to Thanos, delete version of no-metadata block."
)]
pub(crate) struct AppArgs {
    #[clap(default_value = "false", long)]
    pub dry_run: bool,
}
