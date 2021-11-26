use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;

use tempdir::TempDir;
use walkdir::WalkDir;

use crate::request::{
    MasterRequest, PartitionStyle, WorkerJuiceRequest, WorkerMapleRequest, WorkerRequest,
};
use crate::task::Task;

use libsdfs::client::SdfsClient;

use libutils::anyhow::Result;
use libutils::bincode;
use libutils::log::*;
use libutils::net::{read_with_len_tokio, write_with_len_tokio};
use libutils::partition;
use libutils::tokio;
use libutils::tokio::io::AsyncBufReadExt;
use libutils::tokio::io::BufReader;
use libutils::tokio::net::{TcpListener, TcpStream};
use libutils::tokio::process::Command;
use libutils::tokio::sync::Mutex;

struct MasterState {
    // Global task queue
    task_queue: Mutex<VecDeque<Task>>,
    // For each worker, we keep a queue of task shards. This allows us
    // to easily reallocate failed task shards
    workers: Mutex<HashMap<SocketAddr, VecDeque<Vec<PathBuf>>>>,
    // The current task being executed by the cluster
    curr_task: Option<(Task, Instant)>,
}

pub struct MjDaemon {
    listener: TcpListener,
    sdfs_client: SdfsClient,
    id: SocketAddr,
}

// Reconstructs a WorkerRequest from the current task and a set of partitioned files
fn create_task_shard(state: &MasterState, files: &Vec<PathBuf>) -> Option<WorkerRequest> {
    state.curr_task.as_ref().map(|(ref task, _)| match task {
        Task::Maple(request) => WorkerRequest::Maple(WorkerMapleRequest {
            exe: request.exe.clone(),
            prefix: request.prefix.clone(),
            src_dir: request.src_dir.clone(),
            files: files.clone(),
        }),
        Task::Juice(request) => WorkerRequest::Juice(WorkerJuiceRequest {
            exe: request.exe.clone(),
            prefix: request.prefix.clone(),
            dest: request.dest.clone(),
            delete_input: request.delete_input,
            files: files.clone(),
        }),
    })
}

impl MjDaemon {
    pub fn new(listener: TcpListener, sdfs_client: SdfsClient, id: SocketAddr) -> Self {
        Self {
            listener,
            sdfs_client,
            id,
        }
    }

    pub async fn run_master(&self) {
        let mut state = MasterState {
            task_queue: Mutex::new(VecDeque::new()),
            workers: Mutex::new(HashMap::new()),
            curr_task: None,
        };
        loop {
            let (socket, _) = self.listener.accept().await.unwrap();
            if let Err(e) = self.handle_connection_master(socket, &mut state).await {
                error!("An error occurred when handling connection: {}", e);
            }
        }
    }

    /// Runs the given task on the cluster. The input task is partitioned on its input files
    /// and is distributed across all workers.
    async fn launch_task(&self, task: Task, state: &mut MasterState) -> Result<()> {
        match task {
            Task::Maple(task) => {
                let files: Vec<PathBuf> = self
                    .sdfs_client
                    .match_prefix(&task.src_dir.to_string_lossy())
                    .await?;
                let mut workers = state.workers.lock().await;
                let partition = partition::range(
                    &files,
                    std::cmp::min(task.num_maples as usize, workers.len()),
                );

                // Make a copy of the keys to prevent concurrent modification
                let keys = workers.keys().cloned().collect::<Vec<_>>();
                for (worker, files) in keys.iter().zip(partition) {
                    let request = WorkerMapleRequest {
                        exe: task.exe.clone(),
                        prefix: task.prefix.clone(),
                        src_dir: task.src_dir.clone(),
                        files: files.into(),
                    };
                    let message = bincode::serialize(&WorkerRequest::Maple(request)).unwrap();
                    let mut worker_conn = TcpStream::connect(worker)
                        .await
                        .expect("failed to connect to worker");
                    write_with_len_tokio(&mut worker_conn, &message)
                        .await
                        .expect("failed to write to worker");

                    workers.entry(*worker).or_default().push_back(files.into());
                }
            }
            Task::Juice(task) => {
                let files: Vec<PathBuf> = self.sdfs_client.match_prefix(&task.prefix).await?;
                let mut workers = state.workers.lock().await;
                let num_groups = std::cmp::min(task.num_juices as usize, workers.len());
                let partition: Vec<Vec<PathBuf>> = match task.partition_style {
                    PartitionStyle::Hash => partition::hash(&files, num_groups)
                        .into_iter()
                        .map(|x| x.into_iter().cloned().collect())
                        .collect(),
                    PartitionStyle::Range => partition::range(&files, num_groups)
                        .into_iter()
                        .map(|x| x.to_vec())
                        .collect(),
                };

                // Make a copy of the keys to prevent concurrent modification
                let keys = workers.keys().cloned().collect::<Vec<_>>();
                for (worker, files) in keys.iter().zip(partition) {
                    let request = WorkerJuiceRequest {
                        exe: task.exe.clone(),
                        prefix: task.prefix.clone(),
                        dest: task.dest.clone(),
                        delete_input: task.delete_input.clone(),
                        files: files.clone(),
                    };
                    let message = bincode::serialize(&WorkerRequest::Juice(request)).unwrap();
                    let mut worker_conn = TcpStream::connect(worker)
                        .await
                        .expect("failed to connect to worker");
                    write_with_len_tokio(&mut worker_conn, &message)
                        .await
                        .expect("failed to write to worker");

                    workers.entry(*worker).or_default().push_back(files.into());
                }
            }
        }
        Ok(())
    }

    /// Queues or launches the given task depending on the current state of the cluster.
    /// If a task is currently being executed, then queue it for later.
    async fn queue_or_launch_task(&self, task: Task, state: &mut MasterState) -> Result<()> {
        if state.curr_task.is_none() {
            // We can begin executing this task!
            state.curr_task = Some((task.clone(), Instant::now()));
            self.launch_task(task, state).await?;
        } else {
            // Enqueue the current task for later
            let mut queue = state.task_queue.lock().await;
            queue.push_back(task);
        }
        Ok(())
    }

    /// Handles incoming connections as the master node
    async fn handle_connection_master(
        &self,
        mut socket: TcpStream,
        state: &mut MasterState,
    ) -> Result<()> {
        let buf = read_with_len_tokio(&mut socket).await?;
        let request: MasterRequest = bincode::deserialize(&buf).unwrap();
        match request {
            MasterRequest::Join(member) => {
                state.workers.lock().await.insert(member.0, VecDeque::new());
                Ok(())
            }
            MasterRequest::Maple(request) => {
                info!("Received Maple task");
                self.queue_or_launch_task(Task::Maple(request), state).await
            }
            MasterRequest::Juice(request) => {
                info!("Received Juice task");
                self.queue_or_launch_task(Task::Juice(request), state).await
            }
            MasterRequest::Leave(member) => {
                let mut workers = state.workers.lock().await;
                if let Some((member, task_shards)) = workers.remove_entry(&member.0) {
                    if task_shards.is_empty() {
                        return Ok(());
                    }

                    // Worker failed during execution
                    info!("{} left - reallocating tasks...", member);

                    for task_shard in task_shards {
                        // Find a new worker, preferably an idle one
                        let (new_worker, new_worker_tasks) = workers
                            .iter_mut()
                            .min_by(|a, b| a.1.len().cmp(&b.1.len()))
                            .expect("all workers are dead :(");

                        // Construct the new request based off the current task
                        let new_request =
                            create_task_shard(&state, &task_shard).expect("impossible");

                        // Worker is idle, so prompt them to start this task shard immediately
                        if new_worker_tasks.is_empty() {
                            let message = bincode::serialize(&new_request).unwrap();
                            let mut worker_conn = TcpStream::connect(new_worker)
                                .await
                                .expect("failed to connect to worker");
                            write_with_len_tokio(&mut worker_conn, &message)
                                .await
                                .expect("failed to write to worker");
                        }
                        new_worker_tasks.push_back(task_shard);
                    }
                }
                Ok(())
            }
            MasterRequest::Complete(worker_id) => {
                info!("Worker {} completed their current task", worker_id);
                let mut workers = state.workers.lock().await;

                // Remove the worker's current task shard
                workers
                    .get_mut(&worker_id)
                    .map(|t| t.pop_front().expect("worker didn't have a task?"));

                // Check if all workers are done. If so, then the entire task is complete.
                let is_done = workers.iter().all(|(_, tasks)| tasks.is_empty());
                if is_done {
                    let (_, start) = state.curr_task.as_ref().expect("impossible");
                    info!(
                        "Task completed in {} seconds",
                        start.elapsed().as_secs_f64()
                    );

                    // Check if a new task exists in the global task queue
                    let new_task = {
                        let mut global_task_queue = state.task_queue.lock().await;
                        global_task_queue.pop_front()
                    };
                    if let Some(new_task) = new_task {
                        drop(workers);
                        state.curr_task = Some((new_task.clone(), Instant::now()));
                        self.launch_task(new_task, state).await?;
                    } else {
                        state.curr_task = None;
                    }
                } else {
                    if let Some(tasks) = workers.get_mut(&worker_id) {
                        if let Some(new_task_shard) = tasks.front() {
                            // Give the new task shard to the worker
                            let new_task = create_task_shard(&state, &new_task_shard).unwrap();
                            let mut stream = TcpStream::connect(&worker_id)
                                .await
                                .expect("failed to connect to worker");
                            let message =
                                bincode::serialize(&new_task).expect("failed to serialize message");
                            write_with_len_tokio(&mut stream, &message).await?;
                        }
                    }
                }
                Ok(())
            }
        }
    }

    pub async fn run_worker(&mut self, master: SocketAddr) {
        loop {
            let (socket, _) = self.listener.accept().await.unwrap();
            if let Err(e) = self.handle_connection_worker(socket, master).await {
                error!("An error occurred when handling connection: {}", e);
            }
        }
    }

    /// Handles incoming connections as a worker node
    async fn handle_connection_worker(
        &self,
        mut socket: TcpStream,
        master: SocketAddr,
    ) -> Result<()> {
        let buf = read_with_len_tokio(&mut socket).await?;
        let request: WorkerRequest =
            bincode::deserialize(&buf).expect("failed to deserialize request");
        match request {
            WorkerRequest::Maple(request) => {
                info!("Starting Maple for {:?}", &request.files);
                let tmp_dir = TempDir::new("job")?;

                // Fetch exe
                let exe_path = tmp_dir.path().join("exe");
                self.sdfs_client
                    .get(request.exe.as_path(), &exe_path)
                    .await?;
                let mut perms = std::fs::metadata(&exe_path)?.permissions();
                perms.set_mode(0o544);
                fs::set_permissions(&exe_path, perms).expect("failed to set permissions on exe");

                let output_dir = tmp_dir.path().join("output");
                std::fs::create_dir(&output_dir).unwrap();

                // Iterate over all files
                for file in &request.files {
                    let input_path = tmp_dir.path().join("input");
                    self.sdfs_client.get(file, &input_path).await?;

                    let input_file =
                        std::fs::File::open(input_path).expect("failed to open input file");
                    let mut child = Command::new(&exe_path)
                        .stdin(Stdio::from(input_file))
                        .stdout(Stdio::piped())
                        .spawn()
                        .expect("failed to spawn exe");

                    let stdout = child
                        .stdout
                        .take()
                        .expect("child did not have a handle to stdout");
                    let stdout_reader = BufReader::new(stdout);
                    let mut stdout_lines = stdout_reader.lines();

                    // Ensure the child process is spawned in the runtime so it can
                    // make progress on its own while we await for any output.
                    tokio::spawn(async move {
                        child
                            .wait()
                            .await
                            .expect("child encountered an error during execution");
                    });

                    while let Some(line) = stdout_lines.next_line().await? {
                        let mut split = line.split(" ");
                        let key = split.next().expect("invalid output format");
                        let value = split.next().expect("invalid output format");
                        let cleaned_key: String = key
                            .chars()
                            .filter(|x| x.is_alphanumeric() || x.is_ascii_punctuation())
                            .collect();

                        let output_path = output_dir.join(&cleaned_key);
                        let mut output_file = fs::OpenOptions::new()
                            .append(true)
                            .create(true)
                            .open(&output_path)
                            .unwrap();
                        writeln!(output_file, "{}", value).expect("failed to write to output file");
                    }
                }

                // Upload all files to SDFS
                for entry in WalkDir::new(output_dir)
                    .into_iter()
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_type().is_file())
                {
                    let mut sdfs_filename = request.prefix.clone();
                    sdfs_filename.push('_');
                    sdfs_filename.push_str(&entry.path().file_name().unwrap().to_string_lossy());

                    self.sdfs_client
                        .put(entry.path(), Path::new(&sdfs_filename), true)
                        .await
                        .expect("failed to upload results");
                }
                info!("Completed Maple for {:?}", &request.files);
            }
            WorkerRequest::Juice(request) => {
                info!("Starting Juice for {:?}", &request.files);
                let tmp_dir = TempDir::new("job")?;

                // Fetch exe
                let exe_path = tmp_dir.path().join("exe");
                self.sdfs_client
                    .get(request.exe.as_path(), &exe_path)
                    .await?;
                let mut perms = std::fs::metadata(&exe_path)?.permissions();
                perms.set_mode(0o544);
                fs::set_permissions(&exe_path, perms).expect("failed to set permissions on exe");

                let output_dir = tmp_dir.path().join("output");
                std::fs::create_dir(&output_dir).unwrap();
                let output_path = output_dir.join("juice_output");
                let mut output_file = fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&output_path)
                    .unwrap();

                let prefix = request.prefix.clone();
                // Iterate over all files
                for file in &request.files {
                    let key = &file.file_name().unwrap().to_str().unwrap()[prefix.len() + 1..];
                    let input_path = tmp_dir.path().join("input");
                    self.sdfs_client.get(file, &input_path).await?;

                    let input_file =
                        std::fs::File::open(input_path).expect("failed to open input file");
                    let mut child = Command::new(&exe_path)
                        .arg(key)
                        .stdin(Stdio::from(input_file))
                        .stdout(Stdio::piped())
                        .spawn()
                        .expect("failed to spawn exe");

                    let stdout = child
                        .stdout
                        .take()
                        .expect("child did not have a handle to stdout");
                    let stdout_reader = BufReader::new(stdout);
                    let mut stdout_lines = stdout_reader.lines();

                    // Ensure the child process is spawned in the runtime so it can
                    // make progress on its own while we await for any output.
                    tokio::spawn(async move {
                        child
                            .wait()
                            .await
                            .expect("child encountered an error during execution");
                    });

                    while let Some(line) = stdout_lines.next_line().await? {
                        writeln!(output_file, "{}", line).expect("failed to write to output file");
                    }

                    if request.delete_input {
                        info!("Deleting {:?}", file);
                        self.sdfs_client.delete(file).await?;
                    }
                }

                // Upload all files to SDFS
                for entry in WalkDir::new(output_dir)
                    .into_iter()
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_type().is_file())
                {
                    self.sdfs_client
                        .put(entry.path(), Path::new(&request.dest), true)
                        .await
                        .expect("failed to upload results");
                }
                info!("Completed Juice for {:?}", &request.files);
            }
        }

        // Notify the master that we're done with our task
        let mut stream = TcpStream::connect(master)
            .await
            .expect("failed to connect to master");
        let message = bincode::serialize(&MasterRequest::Complete(self.id))
            .expect("failed to serialize message");
        write_with_len_tokio(&mut stream, &message).await
    }
}
