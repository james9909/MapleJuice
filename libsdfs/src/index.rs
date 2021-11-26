use crate::message::Message;

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use libutils::anyhow::Result;
use libutils::bincode;
use libutils::log::*;
use libutils::net::{read_with_len_tokio, write_with_len_tokio};
use libutils::tokio::net::TcpStream;

const NUM_REPLICAS: usize = 4;

pub struct FileIndex {
    files_to_nodes: BTreeMap<PathBuf, HashSet<SocketAddr>>,
    nodes_to_files: BTreeMap<SocketAddr, HashSet<PathBuf>>,
}

impl FileIndex {
    pub fn new() -> Self {
        Self {
            files_to_nodes: BTreeMap::new(),
            nodes_to_files: BTreeMap::new(),
        }
    }

    pub fn add_node(&mut self, node: SocketAddr) {
        self.nodes_to_files.insert(node, HashSet::new());
    }

    pub fn remove_node(&mut self, node: &SocketAddr) {
        let files = self.nodes_to_files.remove(node);
        if let Some(files) = files {
            for file in files {
                let file_entry = self.files_to_nodes.entry(file);
                if let Entry::Occupied(mut e) = file_entry {
                    e.get_mut().remove(node);
                    if e.get().is_empty() {
                        // If the file has no replicas, remove it from the index entirely
                        e.remove();
                    }
                }
            }
        }
    }

    pub fn put_file_for_node<P: AsRef<Path>>(&mut self, node: &SocketAddr, file_name: P) {
        self.nodes_to_files
            .entry(node.clone())
            .or_default()
            .insert(file_name.as_ref().to_path_buf());
        self.files_to_nodes
            .entry(file_name.as_ref().to_path_buf())
            .or_default()
            .insert(node.clone());
    }

    pub fn put_file<P: AsRef<Path>>(&mut self, file_name: P) -> Vec<SocketAddr> {
        let entry = self.files_to_nodes.entry(file_name.as_ref().to_path_buf());
        match entry {
            Entry::Occupied(e) => (*e.get()).iter().cloned().collect(),
            Entry::Vacant(e) => {
                let mut all_nodes = Vec::from_iter(self.nodes_to_files.iter_mut());
                all_nodes.sort_by(|a, b| a.1.len().cmp(&b.1.len()));
                let mut replicas = HashSet::with_capacity(NUM_REPLICAS);

                for (&replica, ref mut replicated_files) in all_nodes.iter_mut().take(NUM_REPLICAS)
                {
                    replicas.insert(replica);
                    replicated_files.insert(file_name.as_ref().to_path_buf());
                }
                let ret = replicas.iter().cloned().collect();
                e.insert(replicas);
                ret
            }
        }
    }

    pub fn delete_file<P: AsRef<Path>>(&mut self, file_name: P) -> Vec<SocketAddr> {
        let entry = self.files_to_nodes.entry(file_name.as_ref().to_path_buf());
        match entry {
            Entry::Occupied(e) => {
                let removed: Vec<SocketAddr> = e.remove().iter().cloned().collect();
                for replica in &removed {
                    self.nodes_to_files
                        .get_mut(&replica)
                        .unwrap()
                        .remove(file_name.as_ref());
                }
                removed
            }
            Entry::Vacant(_) => Vec::new(),
        }
    }

    pub fn nodes_for_file<P: AsRef<Path>>(&self, file_name: P) -> Vec<SocketAddr> {
        self.files_to_nodes
            .get(file_name.as_ref())
            .map(|nodes| Vec::from_iter(nodes.iter().cloned()))
            .unwrap_or_else(|| Vec::new())
    }

    /// Determine the steps needed to maintain the replication factor from index's current state
    fn replication_steps(&self) -> Vec<(PathBuf, Vec<SocketAddr>, Vec<SocketAddr>)> {
        let mut steps = Vec::new();
        for (file, replicas) in &self.files_to_nodes {
            if replicas.len() < NUM_REPLICAS {
                // We need to add more replicas to this file
                let mut all_nodes = Vec::from_iter(self.nodes_to_files.iter());
                all_nodes.sort_by(|a, b| a.1.len().cmp(&b.1.len()));
                let new_replicas = all_nodes
                    .iter()
                    .filter(|(_, replicated_files)| !replicated_files.contains(file))
                    .take(NUM_REPLICAS - replicas.len())
                    .map(|v| v.0)
                    .cloned()
                    .collect::<Vec<SocketAddr>>();
                let sources = self
                    .files_to_nodes
                    .get(file)
                    .map(|x| x.iter().cloned().collect())
                    .unwrap_or_default();

                if !new_replicas.is_empty() {
                    // Only add a step if there are machines we can replicate to
                    steps.push((file.to_path_buf(), new_replicas, sources))
                }
            }
        }
        steps
    }

    /// Perform the actions from `replication_steps`
    pub async fn replicate(&mut self) -> Result<()> {
        for (file_name, replicas, sources) in self.replication_steps() {
            for replica in replicas {
                let mut replica_conn = TcpStream::connect(replica)
                    .await
                    .expect("failed to connect to replica");
                for source in &sources {
                    let message = bincode::serialize(&Message::PullFromNode(
                        source.clone(),
                        file_name.clone(),
                    ))
                    .expect("failed to serialize message");
                    if write_with_len_tokio(&mut replica_conn, &message)
                        .await
                        .is_ok()
                    {
                        let replica_response = read_with_len_tokio(&mut replica_conn).await?;
                        let ok: bool = bincode::deserialize(&replica_response).unwrap();
                        if ok {
                            self.put_file_for_node(&replica, &file_name);
                            debug!(
                                "Replicated {} to {}",
                                file_name.as_path().display(),
                                replica
                            );
                            break;
                        } else {
                            info!("Failed to replicate to {}", replica);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn match_prefix(&self, prefix: &str) -> Vec<PathBuf> {
        self.files_to_nodes
            .iter()
            .filter(|x| x.0.to_str().map_or(false, |path| path.starts_with(prefix)))
            .map(|x| x.0)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_put_and_delete_files() {
        let mut index = FileIndex::new();
        for port in 3000..3010 {
            index.add_node(SocketAddr::new([127, 0, 0, 1].into(), port));
        }

        index.put_file("hello");
        assert_eq!(index.nodes_for_file("hello").len(), 4);
        assert_eq!(index.nodes_for_file("hello2").len(), 0);

        index.put_file("hello2");
        assert_eq!(index.nodes_for_file("hello2").len(), 4);

        index.delete_file("hello2");
        assert_eq!(index.nodes_for_file("hello2").len(), 0);

        index.delete_file("hello");
        assert_eq!(index.nodes_for_file("hello").len(), 0);
    }
}
