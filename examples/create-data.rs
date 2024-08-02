use std::{fs::OpenOptions, io::Write};
use std::{path::PathBuf, slice::from_raw_parts, u32::MAX};

use eyre::Result;
use mmap_rs::MmapOptions;
use rand::Rng;
use rayon::iter::{
    IndexedParallelIterator as _, IntoParallelRefIterator as _, IntoParallelRefMutIterator,
    ParallelIterator as _,
};

use clap::Parser;

use mmap_btree::Entry;

/// Create a data file for benchmarking with. The output file will be called
/// output.sst. The file may have duplicate entries, but will be sorted.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Directory for working space, and the output will be in this dir as output.sst.
    #[arg(short, long, value_name = "DIR")]
    workdir: PathBuf,

    /// How many entries to write (each entry is u32 in size).
    #[arg(short, long, default_value = "600000000")]
    size: usize,

    /// Validate the file after writing
    #[arg(short, long)]
    validate: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    // make the directory if it doesn't exist
    std::fs::create_dir_all(&cli.workdir)?;
    // Scope to free once written
    let f = {
        // generate 1/nth data in every core
        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|i| format!("create-data-worker {i}"))
            .build()
            .unwrap();
        // one nth per thread
        let needed_entries_per_segment = cli.size / pool.current_num_threads();
        // spread the data equally per segment : segment n gets values from a range of width 0..2^32/n.
        let stride = MAX / pool.current_num_threads() as u32;
        let mut output: Vec<Vec<Entry>> = vec![];
        for t in 0..pool.current_num_threads() {
            let min = t as u32 * stride;
            // little ugly, but convenient
            output.push(vec![min.into()]);
        }
        output.par_iter_mut().for_each(|output| {
            let min: u32 = output.last().unwrap().into();
            let max = min + stride;
            let range = min..max;
            let mut segment = Vec::with_capacity(needed_entries_per_segment);
            // setup a random number generator
            let mut rnd = rand::thread_rng();
            for _ in 0..needed_entries_per_segment {
                segment.push(rnd.gen_range(range.clone()).into());
            }
            segment.sort();
            // save the segment in our output slot
            *output = segment;
        });
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(cli.workdir.join("output.sst"))?;
        for s in output.iter() {
            let ptr = s.as_ptr() as *const u8;
            let slice = unsafe { from_raw_parts(ptr, s.len() * 4) };
            // write_all_vectored is unstable
            f.write_all(slice)?;
        }
        f
    };
    let file_size = f.metadata().unwrap().len();
    if cli.validate {
        eprintln!("validating...");
        assert!(
            file_size % 4 == 0,
            "file size is not a multiple of 4, cannot be a u32 array",
        );
        let map = unsafe {
            MmapOptions::new(file_size as usize)
                .unwrap()
                .with_file(&f, 0)
        }
        .map()?;
        let slice: &[Entry] =
            unsafe { from_raw_parts(map.as_ptr() as *const Entry, file_size as usize / 4) };
        slice
            .par_iter()
            .enumerate()
            .try_fold(
                || 0.into(),
                |prev, (pos, current)| {
                    if prev > *current {
                        Err(eyre::eyre!("out of order at entry {pos}"))
                    } else {
                        Ok(*current)
                    }
                },
            )
            .try_reduce(
                || 0.into(),
                |prev, current| {
                    if prev > current {
                        Err(eyre::eyre!("out of order in reduce"))
                    } else {
                        Ok(current)
                    }
                },
            )?;
    }
    Ok(())
}
