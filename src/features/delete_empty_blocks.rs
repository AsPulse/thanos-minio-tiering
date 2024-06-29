use std::collections::{BTreeMap, BTreeSet};

use crate::error::SpanErr;
use crate::minio::MinioInstance;
use minio_rsc::client::{KeyArgs, ListObjectVersionsArgs};
use thiserror::Error;
use tracing::{info, instrument, warn};

#[derive(Debug, Error)]
pub enum DeleteEmptyBlocksError {
    #[error(transparent)]
    MinioNetworkError(#[from] minio_rsc::error::Error),
}

type Error = DeleteEmptyBlocksError;

#[instrument(name = "delete_empty_blocks", level = "trace")]
pub(crate) async fn delete_empty_blocks(
    instance: &MinioInstance,
    dry_run: bool,
) -> Result<(), SpanErr<Error>> {
    info!("listing object versions...");
    let mut blocks = BTreeSet::<String>::new();
    let mut delete = BTreeMap::<String, Vec<(String, String)>>::new();
    let mut next_id = None;

    loop {
        let Some(id) = list_object_versions(next_id, instance, &mut blocks, &mut delete).await?
        else {
            break;
        };
        next_id = Some(id);
    }

    if dry_run {
        info!("deleting empty blocks (dry-run)...");
    } else {
        info!("deleting empty blocks...");
    }

    drop(blocks);
    for (_, files) in delete {
        for (key, version) in files {
            info!(
                "deleting {}({}){}",
                key,
                version,
                if dry_run { " (dry-run)" } else { "" }
            );
            if !dry_run {
                delete_block(instance, key, version).await?;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    Ok(())
}

#[instrument(name = "delete_block", skip(instance), level = "trace")]
async fn delete_block(
    instance: &MinioInstance,
    key: String,
    version_id: String,
) -> Result<(), SpanErr<Error>> {
    let key = KeyArgs::new(key).version_id(Some(version_id));
    instance
        .bucket
        .remove_object(key)
        .await
        .map_err(Error::MinioNetworkError)?;
    Ok(())
}

#[instrument(
    name = "list_object_versions",
    skip(instance, blocks, delete),
    level = "trace"
)]
async fn list_object_versions(
    next_id: Option<(String, String)>,
    instance: &MinioInstance,
    blocks: &mut BTreeSet<String>,
    delete: &mut BTreeMap<String, Vec<(String, String)>>,
) -> Result<Option<(String, String)>, SpanErr<Error>> {
    let fetched = instance
        .minio
        .list_object_versions(
            instance.bucket.clone(),
            ListObjectVersionsArgs {
                key_marker: next_id.clone().map(|(k, _)| k),
                version_id_marker: next_id.clone().map(|(_, v)| v),
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .map_err(Error::MinioNetworkError)?;

    for object in fetched.versions {
        let Some(key) = object.key.split('/').next() else {
            warn!(
                "object: file {} may not be generated by Thanos. Skipping...",
                object.key
            );
            continue;
        };
        blocks.insert(key.to_string());
    }

    for marker in fetched.delete_markers {
        let Some(key) = marker.key.split('/').next() else {
            warn!(
                "object: file {} may not be generated by Thanos. Skipping...",
                marker.key
            );
            continue;
        };
        let files = delete.entry(key.to_string()).or_default();
        // TODO: check if the version_id is None
        files.push((marker.key, marker.version_id.unwrap()));
    }

    info!(
        "found {} blocks, {} blocks will be removed...",
        blocks.len(),
        delete.len(),
    );
    if fetched.is_truncated {
        Ok(Some((
            fetched.next_key_marker,
            fetched.next_version_id_marker,
        )))
    } else {
        Ok(None)
    }
}
