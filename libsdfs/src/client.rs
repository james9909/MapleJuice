use std::io::SeekFrom;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::message::Message;

use libutils::anyhow::{anyhow, Result};
use libutils::bincode;
use libutils::net::{
    read_file_from_stream, read_with_len_tokio, write_file_to_stream, write_with_len_tokio,
};
use libutils::tokio::fs::File;
use libutils::tokio::io::AsyncSeekExt;
use libutils::tokio::net::TcpStream;

async fn read_from_replica(addr: &SocketAddr, file: &mut File, message: &[u8]) -> Result<()> {
    match TcpStream::connect(addr).await {
        Ok(mut replica_conn) => {
            write_with_len_tokio(&mut replica_conn, &message).await?;
            // Exists sanity check
            let exists: bool =
                bincode::deserialize(&read_with_len_tokio(&mut replica_conn).await?)?;
            if exists {
                read_file_from_stream(file, &mut replica_conn).await
            } else {
                Err(anyhow!("file not found"))
            }
        }
        Err(e) => Err(e.into()),
    }
}

pub struct SdfsClient {
    master: SocketAddr,
}

impl SdfsClient {
    pub fn new(master: SocketAddr) -> Self {
        Self { master }
    }

    pub async fn put<P: AsRef<Path>>(
        &self,
        localfilename: P,
        sdfsfilename: P,
        append: bool,
    ) -> Result<()> {
        let mut stream = TcpStream::connect(self.master).await?;
        let put_msg =
            bincode::serialize(&Message::Put(sdfsfilename.as_ref().to_path_buf(), append))?;
        write_with_len_tokio(&mut stream, &put_msg).await?;

        let replicas: Vec<SocketAddr> =
            bincode::deserialize(&read_with_len_tokio(&mut stream).await?)?;
        let mut file = File::open(&localfilename).await?;
        for replica in replicas {
            file.seek(SeekFrom::Start(0)).await?;
            let mut replica_conn = TcpStream::connect(replica).await?;
            write_with_len_tokio(&mut replica_conn, &put_msg).await?;
            write_file_to_stream(&mut replica_conn, &mut file).await?;
        }

        // Send FIN
        write_with_len_tokio(&mut stream, &bincode::serialize(&true)?).await?;
        Ok(())
    }

    pub async fn delete<P: AsRef<Path>>(&self, sdfsfilename: P) -> Result<()> {
        let mut stream = TcpStream::connect(self.master).await?;
        let message = bincode::serialize(&Message::Delete(sdfsfilename.as_ref().to_path_buf()))?;
        write_with_len_tokio(&mut stream, &message).await?;

        // Send FIN
        write_with_len_tokio(&mut stream, &bincode::serialize(&true)?).await?;
        Ok(())
    }

    pub async fn list<P: AsRef<Path>>(&self, sdfsfilename: P) -> Result<Vec<SocketAddr>> {
        let mut stream = TcpStream::connect(self.master).await?;
        let message = bincode::serialize(&Message::Ls(sdfsfilename.as_ref().to_path_buf()))?;
        write_with_len_tokio(&mut stream, &message).await?;

        let res = read_with_len_tokio(&mut stream).await?;
        Ok(bincode::deserialize(&res).expect("failed to deserialize response"))
    }

    pub async fn match_prefix(&self, prefix: &str) -> Result<Vec<PathBuf>> {
        let mut stream = TcpStream::connect(self.master).await?;
        let message = bincode::serialize(&Message::MatchPrefix(prefix.to_string()))?;
        write_with_len_tokio(&mut stream, &message).await?;

        let res = read_with_len_tokio(&mut stream).await?;
        Ok(bincode::deserialize(&res).expect("failed to deserialize response"))
    }

    pub async fn get<P: AsRef<Path>>(&self, sdfsfilename: P, localfilename: P) -> Result<bool> {
        let mut stream = TcpStream::connect(self.master).await?;
        let message = bincode::serialize(&Message::Get(sdfsfilename.as_ref().to_path_buf()))?;
        write_with_len_tokio(&mut stream, &message).await?;

        let exists: bool = bincode::deserialize(&read_with_len_tokio(&mut stream).await?)?;
        if !exists {
            return Ok(false);
        }
        let replicas: Vec<SocketAddr> =
            bincode::deserialize(&read_with_len_tokio(&mut stream).await?)?;
        let mut file = File::create(localfilename).await?;
        let mut saved = false;
        for replica in replicas {
            if read_from_replica(&replica, &mut file, &message)
                .await
                .is_ok()
            {
                saved = true;
                break;
            }
        }

        // Send FIN
        write_with_len_tokio(&mut stream, &bincode::serialize(&true)?).await?;
        Ok(saved)
    }
}
