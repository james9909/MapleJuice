use libheartbeat::member::MemberId;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Put(PathBuf, bool),
    Delete(PathBuf),
    Ls(PathBuf),
    Store(),
    Get(PathBuf),
    Join(MemberId),
    Leave(MemberId),
    PullFromNode(SocketAddr, PathBuf),
    MatchPrefix(String),
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Put(_, _) => "Put",
            Self::Delete(_) => "Delete",
            Self::Ls(_) => "Ls",
            Self::Store() => "Store",
            Self::Get(_) => "Get",
            Self::Join(_) => "Join",
            Self::Leave(_) => "Leave",
            Self::PullFromNode(_, _) => "PullFromNode",
            Self::MatchPrefix(_) => "MatchPrefix",
        };
        write!(f, "{}", name)
    }
}
