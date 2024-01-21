use std::env;

use anyhow::{anyhow, Result};

mod naive;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let filename = args
        .get(1)
        .ok_or_else(|| anyhow!("input file not specified"))?;

    naive::run(filename)?;

    Ok(())
}
