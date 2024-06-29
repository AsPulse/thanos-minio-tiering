use std::path::PathBuf;

use minio_rsc::client::Bucket;
use minio_rsc::provider::StaticProvider;
use minio_rsc::Minio;
use thiserror::Error;
use tracing::instrument;

use crate::config;
use crate::error::SpanErr;

#[derive(Error, Debug)]
pub enum MinioInstanceInitializationError {
    #[error("cannot read config file {0}. {1}")]
    IoError(PathBuf, std::io::Error),

    #[error("cannot parse config file {0}. {1}")]
    ParseError(PathBuf, serde_yaml::Error),

    #[error("cannot initialize MinIO instance. {0}")]
    MinioError(#[source] minio_rsc::error::ValueError),

    #[error(transparent)]
    MinioNetworkError(#[from] minio_rsc::error::Error),

    #[error("bucket {0} does not exist.")]
    BucketDoesNotExist(String),
}

pub struct MinioInstance {
    pub minio: Minio,
    pub bucket_name: String,
    pub bucket: Bucket,
}

type Error = MinioInstanceInitializationError;
impl MinioInstance {
    #[instrument(name = "minio_instance/new")]
    pub async fn new(config_path: PathBuf) -> Result<Self, SpanErr<Error>> {
        let config_file = tokio::fs::read(&config_path)
            .await
            .map_err(|e| Error::IoError(config_path.clone(), e))?;

        let config = serde_yaml::from_slice::<config::ThanosConfig>(&config_file)
            .map_err(|e| Error::ParseError(config_path.clone(), e))?
            .config;

        let provider = StaticProvider::new(config.access_key, config.secret_key, None);

        let minio = Minio::builder()
            .endpoint(config.endpoint)
            .provider(provider)
            .secure(!config.insecure)
            .build()
            .map_err(Error::MinioError)?;

        let bucket_name = config.bucket;
        let bucket = minio.bucket(bucket_name.clone());

        if bucket.exists().await.map_err(Error::MinioNetworkError)? {
            Ok(Self {
                minio,
                bucket,
                bucket_name,
            })
        } else {
            Err(Error::BucketDoesNotExist(bucket_name).into())
        }
    }
}
