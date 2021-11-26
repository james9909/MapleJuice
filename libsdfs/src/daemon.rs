use crate::fs::FileSystem;
use crate::index::FileIndex;
use crate::message::Message;

use std::collections::HashSet;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use libutils::tokio;
use libutils::tokio::net::{TcpListener, TcpStream};
use libutils::tokio::sync::{Mutex, RwLock};

use libutils::anyhow::{anyhow, Result};
use libutils::bincode;
use libutils::log::*;
use libutils::net::{
    get_ip, read_file_from_stream, read_with_len_tokio, write_file_to_stream, write_with_len_tokio,
};

use libheartbeat::member::MemberId;

/// `Master`s will contain the global file index, while `Follower`s will contain
/// the address of the master.
enum State {
    Master(RwLock<FileIndex>),
    Follower(SocketAddr),
}

/// Shared inner state within a SDFS node
struct Inner {
    fs: RwLock<FileSystem>,
    nodes: Mutex<HashSet<MemberId>>,
    state: Mutex<State>,
}

pub struct SdfsDaemon {
    listener: TcpListener,
    inner: Arc<Inner>,
}

async fn pull_from_node(inner: &Arc<Inner>, node: SocketAddr, file_name: PathBuf) -> Result<()> {
    let mut conn =
        tokio::time::timeout(std::time::Duration::from_secs(1), TcpStream::connect(node)).await??;

    // We were able to connect
    let message =
        bincode::serialize(&Message::Get(file_name.clone())).expect("failed to serialize message");

    write_with_len_tokio(&mut conn, &message)
        .await
        .expect("failed to write to stream");

    let response = read_with_len_tokio(&mut conn).await?;
    let exists: bool = bincode::deserialize(&response).expect("failed to deserialize response");
    if exists {
        // Read file from replica
        let mut fs = inner.fs.write().await;
        let mut output = fs.insert(&file_name, false).await;
        read_file_from_stream(&mut output, &mut conn).await?;
        Ok(())
    } else {
        Err(anyhow!("file doesn't exist on replica"))
    }
}

async fn handle_connection(inner: &Arc<Inner>, mut socket: TcpStream) -> Result<()> {
    let buf = read_with_len_tokio(&mut socket).await?;
    let message: Message = bincode::deserialize(&buf).expect("failed to deserialize message");

    let mut state = inner.state.lock().await;
    match message {
        Message::Join(member) => {
            let mut nodes = inner.nodes.lock().await;
            match *state {
                State::Master(ref mut index) => {
                    // We are a master, so multicast the join
                    let mut index = index.write().await;
                    let join_msg = bincode::serialize(&Message::Join(member))
                        .expect("failed to serialize message");
                    for node in nodes.iter() {
                        if let Ok(mut stream) = TcpStream::connect(node.0).await {
                            if let Err(e) = write_with_len_tokio(&mut stream, &join_msg).await {
                                warn!("Failed to notify join to {}: {}", node.0, e);
                            }
                        }
                    }
                    nodes.insert(member);
                    index.add_node(member.0);
                }
                State::Follower(_) => {
                    // We are a follower, so just update our nodes list
                    nodes.insert(member);
                }
            }
        }
        Message::Put(file_name, append) => match *state {
            State::Master(ref mut index) => {
                let mut index = index.write().await;
                let replicas = index.put_file(&file_name);
                write_with_len_tokio(
                    &mut socket,
                    &bincode::serialize(&replicas).expect("failed to serialize message"),
                )
                .await
                .expect("failed to write to client");

                // Wait for client to respond
                read_with_len_tokio(&mut socket).await?;
            }
            State::Follower(_) => {
                let mut fs = inner.fs.write().await;
                let mut output = fs.insert(&file_name, append).await;
                read_file_from_stream(&mut output, &mut socket).await?;
            }
        },
        Message::Get(file_name) => match *state {
            State::Master(ref index) => {
                let index = index.read().await;
                let replicas = index.nodes_for_file(&file_name);
                if replicas.is_empty() {
                    write_with_len_tokio(&mut socket, &bincode::serialize(&false)?).await?;
                } else {
                    write_with_len_tokio(&mut socket, &bincode::serialize(&true)?).await?;
                    write_with_len_tokio(&mut socket, &bincode::serialize(&replicas)?).await?;
                }

                // Wait for client to respond
                read_with_len_tokio(&mut socket).await?;
            }
            State::Follower(_) => {
                let fs = inner.fs.read().await;
                let file = fs.get_file(&file_name).await;
                if let Ok(mut file) = file {
                    // File exists
                    write_with_len_tokio(&mut socket, &bincode::serialize(&true)?).await?;
                    write_file_to_stream(&mut socket, &mut file).await?;
                } else {
                    write_with_len_tokio(&mut socket, &bincode::serialize(&false)?).await?;
                }
            }
        },
        Message::Delete(file_name) => match *state {
            State::Master(ref mut index) => {
                let mut index = index.write().await;
                let replicas = index.delete_file(&file_name);
                let delete_msg = bincode::serialize(&Message::Delete(file_name))?;
                for replica in replicas {
                    if let Ok(mut stream) = TcpStream::connect(replica).await {
                        match write_with_len_tokio(&mut stream, &delete_msg).await {
                            Ok(_) => {
                                info!("Deleted from replica: {}", replica);
                            }
                            Err(e) => {
                                warn!("Failed to delete from {}: {}", replica, e);
                            }
                        }
                    }
                }

                // Wait for client to respond
                read_with_len_tokio(&mut socket).await?;
            }
            State::Follower(_) => {
                let mut fs = inner.fs.write().await;
                fs.delete(&file_name).await;
            }
        },
        Message::Ls(file_name) => match *state {
            State::Master(ref mut index) => {
                let index = index.read().await;
                let replicas = index.nodes_for_file(&file_name);

                let message = bincode::serialize(&replicas).expect("failed to serialize replicas");
                write_with_len_tokio(&mut socket, &message)
                    .await
                    .expect("failed to write to stream");
            }
            State::Follower(_) => {}
        },
        Message::Store() => {
            // Implementation is the same for both master and follower
            let fs = inner.fs.read().await;
            let files = fs.list_files().await;

            let message = bincode::serialize(&files).expect("failed to serialize files");
            write_with_len_tokio(&mut socket, &message)
                .await
                .expect("failed to write to stream");
        }
        Message::Leave(member) => match *state {
            State::Master(ref mut index) => {
                // Handle replica leave/failure
                let mut index = index.write().await;
                let mut nodes = inner.nodes.lock().await;
                nodes.remove(&member);

                index.remove_node(&member.0);

                // Replicate as necessary
                index.replicate().await?;
            }
            State::Follower(master) => {
                let mut nodes = inner.nodes.lock().await;
                nodes.remove(&member);

                if master != member.0 {
                    // Only promote a new leader if the master fails
                    return Ok(());
                }
                // Choose a new master based on join time
                let new_master = nodes.iter().min_by(|a, b| a.1.cmp(&b.1)).cloned();
                if let Some(nm) = new_master {
                    // A new master has been selected
                    nodes.remove(&nm);

                    let self_addr = SocketAddr::new(get_ip()?, socket.local_addr().unwrap().port());
                    if nm.0 == self_addr {
                        // Promote self as master
                        info!("Promoting to master");

                        // Rebuild the index by querying all followers
                        let mut index = FileIndex::new();
                        let message = bincode::serialize(&Message::Store())
                            .expect("failed to serialize message");
                        for (node, _) in nodes.iter() {
                            index.add_node(*node);
                            let follower_conn = TcpStream::connect(node).await;
                            if let Ok(mut follower_conn) = follower_conn {
                                write_with_len_tokio(&mut follower_conn, &message)
                                    .await
                                    .expect("failed to write to follower");
                                let res = read_with_len_tokio(&mut follower_conn)
                                    .await
                                    .expect("failed to read from follower");
                                let files: Vec<String> = bincode::deserialize(&res)
                                    .expect("failed to deserialize response");
                                for file in files {
                                    index.put_file_for_node(&node, &file);
                                }
                            }
                        }

                        // Replicate as necessary
                        index.replicate().await?;

                        let mut fs = inner.fs.write().await;
                        fs.clear();

                        *state = State::Master(RwLock::new(index));
                    } else {
                        // Promote the chosen node to master
                        info!("New master chosen: {}", nm.0);
                        *state = State::Follower(nm.0);
                    }
                }
            }
        },
        Message::PullFromNode(source, file_name) => {
            match *state {
                State::Master(_) => {
                    // This shouldn't happen
                }
                State::Follower(_) => {
                    let response = pull_from_node(inner, source, file_name).await.is_ok();
                    let message =
                        bincode::serialize(&response).expect("failed to serialize message");
                    write_with_len_tokio(&mut socket, &message).await?;
                }
            }
        }
        Message::MatchPrefix(prefix) => {
            if let State::Master(ref index) = *state {
                let matches = index.read().await.match_prefix(&prefix);
                let message = bincode::serialize(&matches).expect("failed to serialize matches");
                write_with_len_tokio(&mut socket, &message)
                    .await
                    .expect("failed to write to stream");
            };
        }
    }
    Ok(())
}

impl SdfsDaemon {
    pub fn new(listener: TcpListener, master: Option<SocketAddr>, base_path: OsString) -> Self {
        let state = match master {
            Some(m) => State::Follower(m),
            None => State::Master(RwLock::new(FileIndex::new())),
        };
        Self {
            listener,
            inner: Arc::new(Inner {
                fs: RwLock::new(FileSystem::new(base_path)),
                nodes: Mutex::new(HashSet::new()),
                state: Mutex::new(state),
            }),
        }
    }

    pub async fn run(self) {
        let listener = self.listener;
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let state = Arc::clone(&self.inner);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(&state, socket).await {
                    error!("An error occurred when handling connection: {}", e);
                }
            });
        }
    }
}
