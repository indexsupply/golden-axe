use std::{
    path::Path,
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy::primitives::U64;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use aws_smithy_types::byte_stream::Length;
use eyre::{eyre, Context, ContextCompat, OptionExt, Result};
use futures::future::join_all;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::Semaphore,
};

#[tracing::instrument(skip_all, fields(id))]
pub async fn restore(
    pg_url: &str,
    chain_id: u64,
    dir: &str,
    bucket: &str,
    key: Option<String>,
) -> eyre::Result<()> {
    let config: tokio_postgres::Config = pg_url.parse().expect("unable to parse pg_url");
    let db_name = config.get_dbname().expect("unable to parse dbname");
    let s3 = aws_sdk_s3::Client::new(&aws_config::load_defaults(BehaviorVersion::latest()).await);
    let id = if let Some(key) = key {
        from_filename(chain_id, &key).ok_or(eyre!("unable to find backup id for: {}", key))?
    } else {
        remote_backups(chain_id, &s3, bucket)
            .await?
            .into_iter()
            .last()
            .ok_or(eyre!("no backups in remote"))?
    };
    tracing::Span::current().record("id", id);
    let resp = s3
        .get_object()
        .bucket(bucket)
        .key(to_filename(chain_id, id))
        .send()
        .await?;
    let mut file = File::create(Path::new(dir).join(to_filename(chain_id, id))).await?;
    let mut body = resp.body;
    while let Some(data) = body.next().await {
        file.write_all(&(data?)).await?;
    }
    tracing::info!("creating database: {}", db_name);
    std::process::Command::new("createdb")
        .arg(db_name)
        .stdout(Stdio::piped())
        .spawn()?
        .wait_with_output()?
        .status
        .success()
        .then(|| tracing::info!("created db"))
        .ok_or_eyre("unable to createdb")?;
    tracing::info!("restoring database: {}", db_name);
    std::process::Command::new("pg_restore")
        .arg("-j")
        .arg("4")
        .arg("-d")
        .arg(db_name)
        .arg(Path::new(dir).join(to_filename(chain_id, id)))
        .stdout(Stdio::piped())
        .spawn()?
        .wait_with_output()?
        .status
        .success()
        .then(|| tracing::info!("restored db"))
        .ok_or_eyre("unable to pg_restore")?;
    Ok(())
}

#[tracing::instrument(skip_all)]
pub async fn backup(
    pg_url: &str,
    dir: &str,
    bucket: &str,
    window: humantime::Duration,
) -> eyre::Result<()> {
    let s3 = aws_sdk_s3::Client::new(&aws_config::load_defaults(BehaviorVersion::latest()).await);
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("tls builder");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let (pg, pg_conn) = tokio_postgres::connect(pg_url, connector).await?;
    tokio::spawn(async move {
        if let Err(e) = pg_conn.await {
            panic!("database writer error: {}", e)
        }
    });
    pg.query("select pg_advisory_lock(2)", &[]).await?;
    let chain_id: u64 = pg
        .query_one("select chain_id from config", &[])
        .await?
        .get::<usize, U64>(0)
        .to();

    let local = local_backups(chain_id, dir)
        .await
        .wrap_err("loading local backups")?;
    match local.into_iter().last() {
        Some(last) if now() - last > window.as_secs() => {
            tracing::info!("local backup needed. last: {}", since(last));
            pgdump(chain_id, dir, pg_url)?;
        }
        Some(last) => {
            tracing::info!("local backup up to date. last: {}", since(last))
        }
        None => {
            tracing::info!("no local backups");
            pgdump(chain_id, dir, pg_url)?;
        }
    };
    let last_local = local_backups(chain_id, dir)
        .await
        .wrap_err("loading last local backup")?
        .into_iter()
        .last()
        .expect("missing local backup");
    let remote = remote_backups(chain_id, &s3, bucket)
        .await
        .wrap_err("loading remote backups")?;
    match remote.into_iter().last() {
        Some(last_remote) if last_local > last_remote => {
            tracing::info!("remote is behind local");
            multipart_upload(
                chain_id,
                &s3,
                bucket,
                &Path::new(dir).join(to_filename(chain_id, last_local)),
            )
            .await
            .wrap_err("uplading backup")?;
        }
        Some(last) => {
            tracing::info!("remote backup is up to date. last: {}", since(last))
        }
        None => {
            tracing::info!("no remote backups");
            multipart_upload(
                chain_id,
                &s3,
                bucket,
                &Path::new(dir).join(to_filename(chain_id, last_local)),
            )
            .await
            .wrap_err("uploading backup")?;
        }
    }
    local_cleanup(chain_id, dir).await
}

#[tracing::instrument(skip_all, fields(id))]
fn pgdump(cid: u64, dir: &str, database_url: &str) -> Result<u64> {
    let id = now();
    tracing::Span::current().record("id", id);
    tracing::info!("starting pg_dump");
    std::process::Command::new("pg_dump")
        .arg(database_url)
        .arg("-F")
        .arg("c")
        .arg("-f")
        .arg(Path::new(dir).join(to_filename(cid, id)))
        .stdout(Stdio::piped())
        .spawn()?
        .wait_with_output()?
        .status
        .success()
        .then(|| tracing::info!("dumped db"))
        .ok_or_eyre("unable to pg_dump")?;
    Ok(id)
}

fn since(id: u64) -> String {
    humantime::format_duration(Duration::from_secs(now() - id)).to_string()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn to_filename(cid: u64, id: u64) -> String {
    format!("ga-backup-{}-{}", cid, id)
}

fn from_filename(cid: u64, name: &str) -> Option<u64> {
    name.strip_prefix(&format!("ga-backup-{}-", cid))?
        .parse::<u64>()
        .ok()
}

#[tracing::instrument(skip_all)]
async fn local_backups(cid: u64, dir: &str) -> Result<Vec<u64>> {
    let mut res = Vec::new();
    let mut paths = fs::read_dir(dir).await?;
    while let Some(entry) = paths.next_entry().await? {
        if let Some(file) = entry.path().file_stem().unwrap().to_str() {
            if let Some(id) = from_filename(cid, file) {
                res.push(id)
            }
        }
    }
    res.sort();
    Ok(res)
}

#[tracing::instrument(skip_all, fields(deleted))]
async fn local_cleanup(cid: u64, dir: &str) -> Result<()> {
    let backups = local_backups(cid, dir).await?;
    let num_backups = backups.len();
    if num_backups > 1 {
        assert!(backups.windows(2).all(|b| b[0] <= b[1]));
        for id in backups.into_iter().take(num_backups - 1) {
            let path = Path::new(dir).join(to_filename(cid, id));
            tracing::info!(id, "deleting");
            fs::remove_file(path).await?;
        }
    }
    tracing::Span::current().record("deleted", num_backups - 1);
    Ok(())
}

#[tracing::instrument(skip_all, fields(bucket))]
async fn remote_backups(cid: u64, client: &Client, bucket: &str) -> Result<Vec<u64>> {
    let mut res = Vec::new();
    let mut response = client
        .list_objects_v2()
        .bucket(bucket)
        .into_paginator()
        .send();
    loop {
        let part = response.next().await;
        match part {
            None => break,
            Some(part) => {
                for object in part.wrap_err("unable to read part")?.contents() {
                    let key = object.key().expect("missing key");
                    if let Some(id) = from_filename(cid, key) {
                        res.push(id)
                    }
                }
            }
        };
    }
    res.sort();
    Ok(res)
}

#[tracing::instrument(skip_all, fields(id))]
pub async fn upload_backup(
    cid: u64,
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    file: &Path,
) -> Result<()> {
    let id = file
        .file_name()
        .expect("unable to get file name from path")
        .to_str()
        .expect("unable to convert file name to str");
    tracing::Span::current().record("id", from_filename(cid, id));
    client
        .put_object()
        .bucket(bucket_name)
        .key(id)
        .body(
            ByteStream::from_path(file)
                .await
                .expect("unable to open file for streaming"),
        )
        .send()
        .await
        .wrap_err("unable to upload backup")?;
    Ok(())
}

#[tracing::instrument(skip_all, fields(key))]
pub async fn multipart_upload(
    cid: u64,
    client: &aws_sdk_s3::Client,
    bucket: &str,
    file: &Path,
) -> Result<()> {
    let key = file
        .file_name()
        .expect("unable to get file name from path")
        .to_str()
        .expect("unable to convert file name to str");
    tracing::Span::current().record("key", from_filename(cid, key));

    let file_size = tokio::fs::metadata(file).await?.len();
    if file_size == 0 {
        return Err(eyre!("0 bytes in file {:?}", file));
    }

    // S3 Upload Limits:
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    //
    // Maximum object size: 5 TiB
    // Maximum number of parts per upload: 10,000
    // Part numbers	1 to 10,000 (inclusive)
    // Part size: 5 MiB to 5 GiB. There is no minimum size limit on the last part of your multipart upload.
    // Maximum number of parts returned for a list parts request: 1000
    // Maximum number of multipart uploads returned in a list multipart uploads request: 1000
    const PART_SIZE: u64 = 5 * 1024 * 1024 * 1024; //5GiB
    let parts = file_size.div_ceil(PART_SIZE);
    tracing::info!(file_size, parts, bucket, key);

    let upload = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    let upload_id = upload
        .upload_id()
        .wrap_err("creating multipart upload id")?;

    // run at most 10 upload parts. I'm not sure that this number is ideal
    // but it seems good to have some limit here.
    let semaphore = Arc::new(Semaphore::new(10));
    let mut tasks: Vec<_> = vec![];
    for i in 0..parts {
        let length = if i + 1 == parts {
            PART_SIZE.min(file_size % PART_SIZE)
        } else {
            PART_SIZE
        };
        let stream = ByteStream::read_from()
            .path(file)
            .offset(i * PART_SIZE)
            .length(Length::Exact(length))
            .build()
            .await?;
        let semaphore = semaphore.clone();
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let part_number = (i as i32) + 1; //part_numher is 1-indexed
        let upload_id = upload_id.to_string();
        tasks.push(tokio::task::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            upload_part(&client, &bucket, &key, &upload_id, part_number, stream).await
        }));
    }
    let results: Vec<CompletedPart> = join_all(tasks)
        .await
        .into_iter()
        .map(|part| part?)
        .collect::<Result<Vec<_>, _>>()?;
    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(results))
        .build();
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await?;
    Ok(())
}

#[tracing::instrument(skip(client, stream))]
async fn upload_part(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    stream: ByteStream,
) -> Result<CompletedPart> {
    let res = client
        .upload_part()
        .key(key)
        .bucket(bucket)
        .upload_id(upload_id)
        .body(stream)
        .part_number(part_number)
        .send()
        .await?;
    Ok(CompletedPart::builder()
        .e_tag(res.e_tag.unwrap_or_default())
        .part_number(part_number)
        .build())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_filename() {
        assert!(from_filename(0, "").is_none());
        assert!(from_filename(0, "ga-backup").is_none());
        assert!(from_filename(0, "ga-backup-foo").is_none());
        assert_eq!(
            1721432878,
            from_filename(85432, "ga-backup-85432-1721432878").expect("unable to parse time")
        );
    }
}
