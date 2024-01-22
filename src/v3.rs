use std::{
    cmp::min, collections::HashMap, fmt::Display, fs::File, io::BufRead, path::Path, sync::mpsc,
};

use anyhow::Result;

use crate::mmap::MMappedFile;

const MAX_THREADS: usize = 8;
const PAGE_SIZE: usize = 4096;

pub(crate) fn run<P>(filename: P) -> Result<()>
where
    P: AsRef<Path>,
{
    let file = unsafe { MMappedFile::new(File::open(filename)?) }?;
    let data = file.as_slice();

    let num_threads = min(MAX_THREADS, (data.len() + PAGE_SIZE - 1) / PAGE_SIZE);

    let block_size = data.len() / num_threads;
    // Round up to nearest multiple of PAGE_SIZE.
    let block_size = ((block_size + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;

    let (results_tx, results_rx) = mpsc::channel();

    for i in 0..num_threads {
        let results_tx = results_tx.clone();
        std::thread::spawn(move || -> Result<()> {
            let block_start = block_size * i;
            let block_end = min(data.len(), block_start + block_size);

            // Walk backwards to find the start of the record potentially
            // straddling the boundary with the previous block.
            let mut record_start = block_start;
            while record_start > 0 && data[record_start - 1] != b'\n' {
                record_start -= 1;
            }
            let record_start = record_start;

            let data = &data[record_start..block_end];
            let lines = data.lines();

            let mut station_stats = StationStats::new();
            for line in lines {
                let line = line?;
                match Measurement::try_from(line.as_str()) {
                    Ok(measurement) => station_stats.record(measurement),
                    // If we failed to parse the current line as a Measurement,
                    // then we assume that it was truncated at the block boundary,
                    // and has therefore already been handled as part of the next block.
                    Err(_) => break,
                }
            }

            results_tx.send(station_stats).unwrap();
            Ok(())
        });
    }
    drop(results_tx);

    let mut station_stats = results_rx.recv().unwrap();
    while let Ok(new_station_stats) = results_rx.recv() {
        station_stats.merge(new_station_stats);
    }

    println!("{}", station_stats);

    Ok(())
}

#[derive(Debug)]
struct Measurement<'a> {
    station_name: &'a str,
    value: f32,
}

impl<'a> TryFrom<&'a str> for Measurement<'a> {
    type Error = ();

    // Returns Err if s does not match the format "abcdef;[-][0]0.0"
    fn try_from(s: &'a str) -> std::result::Result<Self, ()> {
        if s.len() < 4 || s.as_bytes()[s.len() - 2] != b'.' {
            return Err(());
        }

        let Some((station_name, value)) = s.split_once(';') else {
            return Err(());
        };

        Ok(Measurement {
            station_name,
            value: value.parse().unwrap(), // Panic since this is unexpected.
        })
    }
}

struct Stats {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,
}

struct StationStats {
    stats: HashMap<String, Stats>,
}

impl StationStats {
    fn new() -> Self {
        Self {
            stats: HashMap::new(),
        }
    }

    fn record(&mut self, measurement: Measurement) {
        let station_name = measurement.station_name;
        let value = measurement.value;

        if let Some(stats) = self.stats.get_mut(station_name) {
            if value < stats.min {
                stats.min = value
            } else if value > stats.max {
                stats.max = value
            }
            stats.sum += value;
            stats.count += 1;
        } else {
            self.stats.insert(
                station_name.to_owned(),
                Stats {
                    min: value,
                    max: value,
                    sum: value,
                    count: 1,
                },
            );
        }
    }

    fn merge(&mut self, other: StationStats) {
        for (station_name, other_stats) in other.stats.into_iter() {
            if let Some(stats) = self.stats.get_mut(&station_name) {
                if other_stats.min < stats.min {
                    stats.min = other_stats.min;
                }
                if other_stats.max > stats.max {
                    stats.max = other_stats.max;
                }
                stats.sum += other_stats.sum;
                stats.count += other_stats.count;
            } else {
                self.stats.insert(station_name, other_stats);
            }
        }
    }
}

impl Display for StationStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut stations: Vec<_> = self.stats.keys().collect();
        stations.sort();
        let num_stations = stations.len();

        write!(f, "{{")?;
        for (idx, station) in stations.into_iter().enumerate() {
            let stats = self.stats.get(station).unwrap();
            let mean = stats.sum / (stats.count as f32);
            write!(
                f,
                "{}={:.1}/{:.1}/{:.1}",
                station, stats.min, mean, stats.max
            )?;

            if idx != num_stations - 1 {
                write!(f, ", ")?
            }
        }
        write!(f, "}}")?;

        Ok(())
    }
}
