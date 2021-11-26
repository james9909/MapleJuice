use crate::heartbeat::HeartbeatStyle;
use crate::member::MemberMap;

use std::fmt;

use libutils::serde::{Deserialize, Serialize};
use libutils::time::Timestamp;

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Join(Timestamp),
    JoinAck(Timestamp, MemberMap),
    Gossip(Timestamp, MemberMap),
    Ping(Timestamp),
    Leave(Timestamp),
    SetStyle(HeartbeatStyle),
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Join(_) => "Join",
            Self::JoinAck(_, _) => "JoinAck",
            Self::Gossip(_, _) => "Gossip",
            Self::Ping(_) => "Ping",
            Self::Leave(_) => "Leave",
            Self::SetStyle(_) => "SetStyle",
        };
        write!(f, "{}", name)
    }
}
