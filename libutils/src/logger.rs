pub use simplelog::*;

use anyhow::Result;

use std::fs::File;

pub fn initialize_logging(log_path: Option<&str>) -> Result<()> {
    let config = ConfigBuilder::new()
        .set_thread_level(LevelFilter::Debug)
        .set_thread_mode(ThreadLogMode::Names)
        .set_target_level(LevelFilter::Trace)
        .build();
    if let Some(log_path) = log_path {
        CombinedLogger::init(vec![
            WriteLogger::new(LevelFilter::Info, config.clone(), File::create(log_path)?),
            TermLogger::new(LevelFilter::Info, config.clone(), TerminalMode::Mixed),
        ])?;
    } else {
        TermLogger::init(LevelFilter::Info, config, TerminalMode::Mixed)?;
    }
    Ok(())
}
