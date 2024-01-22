use std::{cmp::min, fmt::Display, fs::File, io::BufRead, path::Path, sync::mpsc};

use anyhow::Result;
use fxhash::FxHashMap as HashMap;

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

            let mut station_stats = StationStats::new();
            foreach_measurement(data, |m| station_stats.record(m));

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

fn foreach_measurement<F>(mut data: &[u8], mut visit: F)
where
    F: FnMut(Measurement),
{
    while !data.is_empty() {
        let Some(station_name_len) = data.iter().position(|c| *c == b';') else {
            break;
        };
        let station_name = unsafe { std::str::from_utf8_unchecked(&data[0..station_name_len]) };
        data.consume(station_name_len + 1);

        let Some(value_len) = data.iter().position(|c| *c == b'\n') else {
            break;
        };
        let value_str = unsafe { std::str::from_utf8_unchecked(&data[0..value_len]) };
        data.consume(value_len + 1);

        visit(Measurement {
            station_name,
            value: value_str.parse().unwrap(),
        });
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
            stats: HashMap::default(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_measurements_end_newline() {
        let data: &[u8] = r#"Dushanbe;1.7
Honiara;34.9
Taipei;3.3
Suwałki;4.2
Lahore;15.3
Philadelphia;24.4
Kingston;29.0
Hamburg;-18.1
Damascus;5.4
Rabat;16.6
"#
        .as_bytes();

        foreach_measurement(data, |m| println!("{:?}", m));
    }

    #[test]
    fn read_measurements_truncated() {
        let data: &[u8] = r#"Dushanbe;1.7
Honiara;34.9
Taipei;3.3
Suwałki;4.2
Lahore;15.3
Philadelphia;24.4
Kingston;29.0
Hamburg;-18.1
Damascus;5.4
Rabat;1"#
            .as_bytes();
        foreach_measurement(data, |m| println!("{:?}", m));
    }

    #[test]
    fn read_measurements_no_newline() {
        let data: &[u8] = r#"Dushanbe;1.7
Honiara;34.9
Taipei;3.3
Suwałki;4.2
Lahore;15.3
Philadelphia;24.4
Kingston;29.0
Hamburg;-18.1
Damascus;5.4
Rabat;16.6"#
            .as_bytes();
        foreach_measurement(data, |m| println!("{:?}", m));
    }
}
