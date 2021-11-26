use std::time::{SystemTime, UNIX_EPOCH};

pub type Timestamp = u128;

pub fn time_since_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
