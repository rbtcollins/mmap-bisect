use std::{fs::File, slice::from_raw_parts};

use eyre::Result;
use mmap_rs::{Mmap, MmapOptions};

/// Not worrying about network byte order, this test just uses native order
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Entry(u32);

impl From<&Entry> for u32 {
    fn from(e: &Entry) -> u32 {
        e.0
    }
}

impl From<Entry> for u32 {
    fn from(e: Entry) -> u32 {
        e.0
    }
}

impl From<u32> for Entry {
    fn from(e: u32) -> Entry {
        Entry(e)
    }
}

/// A set of Entries accessed via mmap.
/// Note: - this is not async, IO will block; use a thread pool to access concurrently if needed.
pub struct SST {
    // Drop map before file
    map: Mmap,
    #[allow(dead_code)]
    file: File,
}

impl SST {
    pub fn new(file: File) -> Result<Self> {
        let file_size: usize = file.metadata()?.len().try_into()?;
        assert!(
            file_size % 4 == 0,
            "file size is not a multiple of 4, cannot be a u32 array",
        );
        let map = unsafe { MmapOptions::new(file_size).unwrap().with_file(&file, 0) }.map()?;
        assert_eq!(file_size, map.len());
        Ok(Self { file, map })
    }

    fn as_slice(&self) -> &[Entry] {
        let slice: &[Entry] =
            unsafe { from_raw_parts(self.map.as_ptr() as *const Entry, self.map.len() / 4) };
        slice
    }

    pub fn find(&self, key: u32) -> Option<Entry> {
        self.as_slice()
            .binary_search_by_key(&key, |e| e.0)
            .ok()
            .map(|idx| self.as_slice()[idx])
    }
}
