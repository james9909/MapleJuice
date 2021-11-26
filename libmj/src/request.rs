use std::net::SocketAddr;
use std::path::PathBuf;

use libheartbeat::member::MemberId;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PartitionStyle {
    Range,
    Hash,
}

/// MasterRequest encompasses all requests sent to the master node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MasterRequest {
    Join(MemberId),
    Leave(MemberId),
    Maple(MasterMapleRequest),
    Juice(MasterJuiceRequest),
    Complete(SocketAddr),
}

/// MasterMapleRequest contains all the information needed by the master to start
/// a Maple task.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MasterMapleRequest {
    pub exe: PathBuf,
    pub num_maples: u16,
    pub prefix: String,
    pub src_dir: PathBuf,
}

/// MasterJuiceRequest contains all the information needed by the master to start
/// a Juice task.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MasterJuiceRequest {
    pub exe: PathBuf,
    pub num_juices: u16,
    pub prefix: String,
    pub dest: PathBuf,
    pub delete_input: bool,
    pub partition_style: PartitionStyle,
}

/// WorkerRequest encompasses all requests sent to worker nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkerRequest {
    Maple(WorkerMapleRequest),
    Juice(WorkerJuiceRequest),
}

/// WorkerMapleRequest contains all information required for a worker node to
/// start a Maple task. The list of files should be a subset of the entire input
/// corpus.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerMapleRequest {
    pub exe: PathBuf,
    pub prefix: String,
    pub src_dir: PathBuf,
    pub files: Vec<PathBuf>,
}

/// WorkerJuiceRequest contains all information required for a worker node to
/// start a Juice task. The list of files should be a subset of the entire key
/// set.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerJuiceRequest {
    pub exe: PathBuf,
    pub prefix: String,
    pub dest: PathBuf,
    pub delete_input: bool,
    pub files: Vec<PathBuf>,
}
