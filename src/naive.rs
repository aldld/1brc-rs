use std::{
    collections::HashMap,
    fmt::Display,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use anyhow::{anyhow, Error, Result};

pub(crate) fn run<P>(filename: P) -> Result<()>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    let lines = BufReader::new(file).lines();

    let mut station_stats = StationStats::new();
    for line in lines {
        let line = line?;
        let measurement: Measurement = line.as_str().try_into()?;
        station_stats.record(measurement);
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
    type Error = Error;

    fn try_from(s: &'a str) -> Result<Self> {
        let Some((station_name, value)) = s.split_once(';') else {
            return Err(anyhow!("invalid measurement {}", s));
        };

        Ok(Measurement {
            station_name,
            value: value.parse()?,
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
