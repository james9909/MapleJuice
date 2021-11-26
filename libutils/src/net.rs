use anyhow::Result;
use dns_lookup::{get_hostname, lookup_host};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::io::{BufReader, BufWriter};

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr};
use std::net::{SocketAddr, ToSocketAddrs};

const CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4MB

pub fn get_socket_addr(ip: &str) -> Result<SocketAddr> {
    Ok(ip.to_socket_addrs()?.next().unwrap())
}

pub fn get_ip() -> Result<IpAddr> {
    get_ip_from_hostname(&get_hostname()?)
}

pub fn get_ip_from_hostname(hostname: &str) -> Result<IpAddr> {
    let ips = lookup_host(&hostname)?;
    let mut ip = ips
        .first()
        .ok_or(anyhow!("could not get ip address"))?
        .clone();
    if ip == IpAddr::V4(Ipv4Addr::new(127, 0, 1, 1)) {
        // Consistent localhost formatting
        ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    }
    Ok(ip)
}

pub async fn write_with_len_tokio<W>(s: &mut W, message: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let len = message.len().to_be_bytes();
    s.write_all(&len).await?;
    s.write_all(message).await?;
    Ok(())
}

pub fn write_with_len<W>(s: &mut W, message: &[u8]) -> Result<()>
where
    W: Write + Unpin,
{
    let len = message.len().to_be_bytes();
    s.write_all(&len)?;
    s.write_all(message)?;
    Ok(())
}

pub async fn read_with_len_tokio<R>(s: &mut R) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0; 8];
    s.read_exact(&mut len).await?;

    let len = u64::from_be_bytes(len);
    let mut ret = vec![0; len as usize];
    s.read_exact(&mut ret).await?;
    Ok(ret)
}

pub fn read_with_len<R>(s: &mut R) -> Result<Vec<u8>>
where
    R: Read + Unpin,
{
    let mut len = [0; 8];
    s.read_exact(&mut len)?;

    let len = u64::from_be_bytes(len);
    let mut ret = vec![0; len as usize];
    s.read_exact(&mut ret)?;
    Ok(ret)
}

pub async fn read_file_from_stream<R>(file: &mut File, input: &mut R) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0; 8];
    input.read_exact(&mut len_buf).await?;
    let len = u64::from_be_bytes(len_buf);

    let mut file = BufWriter::with_capacity(CHUNK_SIZE, file);
    let mut remaining = len;
    let mut buf = vec![0; CHUNK_SIZE];
    while remaining > 0 {
        let n = input.read(&mut buf).await?;
        file.write_all(&buf[..n]).await?;
        remaining -= n as u64;
    }
    file.flush().await?;
    Ok(())
}

pub async fn write_file_to_stream<W>(output: &mut W, file: &mut File) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let len = file.metadata().await?.len();
    output.write_all(&len.to_be_bytes()).await?;

    let mut file = BufReader::with_capacity(CHUNK_SIZE, file);
    let mut buf = vec![0; CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        output.write_all(&buf[..n]).await?;
    }
    Ok(())
}
