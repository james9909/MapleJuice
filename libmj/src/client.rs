use std::net::SocketAddr;
use std::path::Path;

use crate::request::{MasterJuiceRequest, MasterMapleRequest, MasterRequest, PartitionStyle};

use libutils::anyhow::Result;
use libutils::bincode;
use libutils::net::write_with_len_tokio;
use libutils::tokio::net::TcpStream;

pub struct MjClient {
    master: SocketAddr,
}

impl MjClient {
    pub fn new(master: SocketAddr) -> Self {
        Self { master }
    }

    pub async fn submit_maple<P: AsRef<Path>>(
        &self,
        exe: P,
        num_workers: u16,
        intermediate_prefix: &str,
        src_dir: P,
    ) -> Result<()> {
        let mut stream = TcpStream::connect(self.master).await?;
        let request = MasterMapleRequest {
            exe: exe.as_ref().to_path_buf(),
            num_maples: num_workers,
            prefix: intermediate_prefix.to_string(),
            src_dir: src_dir.as_ref().to_path_buf(),
        };
        let message = bincode::serialize(&MasterRequest::Maple(request))?;
        write_with_len_tokio(&mut stream, &message).await
    }

    pub async fn submit_juice<P: AsRef<Path>>(
        &self,
        exe: P,
        num_workers: u16,
        intermediate_prefix: &str,
        dest: P,
        delete_input: bool,
        partition_style: PartitionStyle,
    ) -> Result<()> {
        let mut stream = TcpStream::connect(self.master).await?;
        let request = MasterJuiceRequest {
            exe: exe.as_ref().to_path_buf(),
            num_juices: num_workers,
            prefix: intermediate_prefix.to_string(),
            dest: dest.as_ref().to_path_buf(),
            delete_input,
            partition_style,
        };
        let message = bincode::serialize(&MasterRequest::Juice(request))?;
        write_with_len_tokio(&mut stream, &message).await
    }
}
