use std::{
    fs,
    io::{self, BufReader, Read},
    iter::Peekable,
    marker::PhantomData,
    time::{SystemTime, UNIX_EPOCH},
};

use thiserror::Error;

use ext_sort::{
    BinaryHeapMerger, ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder, RmpExternalChunk,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

pub struct Storage {
    root: String,
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error(transparent)]
    FileError(#[from] std::io::Error),
    #[error(transparent)]
    EncodeError(#[from] rmp_serde::encode::Error),
    #[error(transparent)]
    SortError(
        #[from]
        ext_sort::SortError<
            rmp_serde::encode::Error,
            rmp_serde::decode::Error,
            rmp_serde::decode::Error,
        >,
    ),
}

pub type ReduceSortedIter = Peekable<
    BinaryHeapMerger<
        IntermediateData,
        rmp_serde::decode::Error,
        fn(&IntermediateData, &IntermediateData) -> std::cmp::Ordering,
        RmpExternalChunk<IntermediateData>,
    >,
>;

#[derive(Debug, Deserialize, Serialize)]
pub struct IntermediateData {
    pub key: String,
    pub value: Vec<u8>,
}

/// A key-value pair of output
#[derive(Deserialize, Serialize)]
pub struct OutputData(pub String, pub Vec<u8>);

impl Storage {
    pub fn new(root: &str) -> Self {
        Self {
            root: root.to_owned(),
        }
    }

    pub async fn clear(&self) -> Result<(), StorageError> {
        if tokio::fs::try_exists(&self.root).await? {
            tokio::fs::remove_dir_all(&self.root).await?;
        }

        Ok(())
    }

    pub async fn reset(&self) -> Result<(), StorageError> {
        self.clear().await?;
        tokio::fs::create_dir_all(&self.root).await?;
        tokio::fs::create_dir_all("./data").await?;
        Ok(())
    }

    pub async fn append_map_out(
        &self,
        index: usize,
        partition: String,
        data: IntermediateData,
    ) -> Result<(), StorageError> {
        let encoded = rmp_serde::to_vec(&data)?;

        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(format!("{}/int-{}-{}", self.root, partition, index))
            .await?;

        file.write_all(&encoded).await?;

        Ok(())
    }

    pub async fn write_map_out(
        &self,
        index: usize,
        partition: &str,
        records: &[IntermediateData],
    ) -> Result<(), StorageError> {
        let mut buf = Vec::new();
        for record in records {
            buf.extend_from_slice(&rmp_serde::to_vec(record)?);
        }

        let final_path = format!("{}/int-{}-{}", self.root, partition, index);
        let tmp_path = format!("{}.tmp-{}", final_path, unique_suffix());

        let mut tmp_file = tokio::fs::File::create(&tmp_path).await?;
        tmp_file.write_all(&buf).await?;
        tmp_file.sync_all().await?;
        drop(tmp_file);

        tokio::fs::rename(&tmp_path, &final_path).await?;

        Ok(())
    }

    pub async fn get_map_out(
        &self,
        index: usize,
        partition: String,
    ) -> Result<Vec<u8>, StorageError> {
        let data = tokio::fs::read(format!("{}/int-{}-{}", self.root, partition, index)).await?;
        Ok(data)
    }

    pub async fn append_reduce_in(
        &self,
        partition: String,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let file_path = format!("{}/sor-{}", self.root, partition);
        let mut f = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)
            .await?;
        f.write_all(&data).await?;

        Ok(())
    }

    pub async fn sync_reduce_in(&self, partition: &String) -> Result<(), StorageError> {
        let file_path = format!("{}/sor-{}", self.root, partition);
        let f = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&file_path)
            .await?;
        f.sync_all().await?;
        Ok(())
    }

    pub async fn reduce_in_len(&self, partition: &String) -> Result<u64, StorageError> {
        let file_path = format!("{}/sor-{}", self.root, partition);
        let metadata = tokio::fs::metadata(&file_path).await?;
        Ok(metadata.len())
    }

    pub async fn clear_reduce_in(&self, partition: &String) -> Result<(), StorageError> {
        let file_path = format!("{}/sor-{}", self.root, partition);
        let Ok(true) = tokio::fs::try_exists(&file_path).await else {
            return Ok(());
        };

        tokio::fs::remove_file(file_path).await?;

        Ok(())
    }

    pub async fn get_reduce_external_sort_iter(
        &self,
        partition: String,
    ) -> Result<ReduceSortedIter, StorageError> {
        let root = self.root.clone();

        tokio::task::spawn_blocking(move || {
            let sort_file = std::fs::File::open(format!("{}/sor-{}", root, partition))?;
            let file_size = sort_file.metadata()?.len();
            let reader = BufReader::new(sort_file).take(file_size);
            let iter: RmpIter<IntermediateData> = RmpIter {
                reader,
                _marker: PhantomData,
            };

            let sorter: ExternalSorter<
                IntermediateData,
                rmp_serde::decode::Error,
                LimitedBufferBuilder,
            > = ExternalSorterBuilder::new()
                .with_tmp_dir(std::path::Path::new("./data"))
                .with_buffer(LimitedBufferBuilder::new(100_000, false))
                .build()?;

            Ok(sorter
                .sort_by(
                    iter,
                    cmp_intermediate
                        as fn(&IntermediateData, &IntermediateData) -> std::cmp::Ordering,
                )?
                .peekable())
        })
        .await
        .expect("reduce sort setup task panicked")
    }

    pub async fn append_reduce_out(
        &self,
        partition: String,
        data: OutputData,
    ) -> Result<(), StorageError> {
        let output = rmp_serde::to_vec(&data)?;

        let output_file_path = format!("{}/out-{}", self.root, partition);
        let mut output_file = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(output_file_path)
            .await?;

        output_file.write_all(&output).await?;

        Ok(())
    }

    pub async fn sync_reduce_out(&self, partition: &String) -> Result<(), StorageError> {
        let file_path = format!("{}/out-{}", self.root, partition);
        let f = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&file_path)
            .await?;
        f.sync_all().await?;
        Ok(())
    }

    pub async fn clear_reduce_out(&self, partition: &String) -> Result<(), StorageError> {
        let file_path = format!("{}/out-{}", self.root, partition);
        let Ok(true) = tokio::fs::try_exists(&file_path).await else {
            return Ok(());
        };

        tokio::fs::remove_file(file_path).await?;

        Ok(())
    }

    pub async fn get_reduce_out(&self, partition: String) -> Result<Vec<u8>, StorageError> {
        let data = tokio::fs::read(format!("{}/out-{}", self.root, partition)).await?;
        Ok(data)
    }
}

pub async fn advance_reduce_sorted(
    mut iter: ReduceSortedIter,
) -> (
    ReduceSortedIter,
    Option<Result<IntermediateData, rmp_serde::decode::Error>>,
    bool,
) {
    tokio::task::spawn_blocking(move || {
        let item = iter.next();
        let peek_key_differs = match (&item, iter.peek()) {
            (Some(Ok(current)), Some(Ok(next))) => next.key != current.key,
            _ => false,
        };

        (iter, item, peek_key_differs)
    })
    .await
    .expect("reduce sort advance task panicked")
}

struct RmpIter<T> {
    reader: io::Take<BufReader<fs::File>>,
    _marker: PhantomData<T>,
}

impl<T: serde::de::DeserializeOwned> Iterator for RmpIter<T> {
    type Item = Result<T, rmp_serde::decode::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.limit() == 0 {
            return None;
        }
        Some(rmp_serde::decode::from_read(&mut self.reader))
    }
}

fn cmp_intermediate(a: &IntermediateData, b: &IntermediateData) -> std::cmp::Ordering {
    a.key.cmp(&b.key)
}

/// A value unique enough to keep concurrent [Storage::write_map_out] temp files
/// (e.g. from a requeued, re-executed map job) from colliding with each other.
fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos()
}
