use std::{
    fs::{self, OpenOptions},
    io::{self, BufReader, Read, Write},
    iter::Peekable,
    marker::PhantomData,
};

use thiserror::Error;

use ext_sort::{
    BinaryHeapMerger, ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder, RmpExternalChunk,
};
use serde::{Deserialize, Serialize};

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

#[derive(Deserialize, Serialize)]
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

    pub fn clear(&self) -> Result<(), StorageError> {
        if std::fs::exists(&self.root)? {
            std::fs::remove_dir_all(&self.root)?;
        }

        Ok(())
    }

    pub fn reset(&self) -> Result<(), StorageError> {
        self.clear()?;
        std::fs::create_dir_all("./data")?;
        Ok(())
    }

    pub fn append_map_out(
        &self,
        index: usize,
        partition: String,
        data: IntermediateData,
    ) -> Result<(), StorageError> {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(format!("{}/int-{}-{}", self.root, partition, index))?;

        let encoded = rmp_serde::to_vec(&data)?;

        file.write(&encoded)?;

        Ok(())
    }

    pub fn get_map_out(&self, index: usize, partition: String) -> Result<Vec<u8>, StorageError> {
        let data = std::fs::read(format!("{}/int-{}-{}", self.root, partition, index))?;
        Ok(data)
    }

    pub fn append_reduce_in(&self, partition: String, data: Vec<u8>) -> Result<(), StorageError> {
        let file_path = format!("{}/sor-{}", self.root, partition);
        let mut f = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)?;
        f.write(&data)?;

        Ok(())
    }

    pub fn get_reduce_external_sort_iter(
        &self,
        partition: String,
    ) -> Result<ReduceSortedIter, StorageError> {
        let sort_file = std::fs::File::open(&format!("{}/sor-{}", self.root, partition))?;
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
                cmp_intermediate as fn(&IntermediateData, &IntermediateData) -> std::cmp::Ordering,
            )?
            .peekable())
    }

    pub fn append_reduce_out(
        &self,
        partition: String,
        data: OutputData,
    ) -> Result<(), StorageError> {
        let output_file_path = format!("{}/out-{}", self.root, partition);
        let mut output_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(output_file_path)?;

        let output = rmp_serde::to_vec(&data)?;

        output_file.write(&output)?;

        Ok(())
    }

    pub fn get_reduce_out(&self, partition: String) -> Result<Vec<u8>, StorageError> {
        let data = std::fs::read(format!("{}/out-{}", self.root, partition))?;
        Ok(data)
    }
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
