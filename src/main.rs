use std::env;

use anyhow::{anyhow, bail, Result};

mod mmap;

mod naive;
mod v2;
mod v3;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let version = args
        .get(1)
        .ok_or_else(|| anyhow!("version not specified"))?;
    let filename = args
        .get(2)
        .ok_or_else(|| anyhow!("input file not specified"))?;

    if version == "naive" {
        naive::run(filename)?;
    } else if version == "v2" {
        v2::run(filename)?;
    } else if version == "v3" {
        v3::run(filename)?;
    } else {
        bail!("invalid version {}", version)
    }

    Ok(())
}
