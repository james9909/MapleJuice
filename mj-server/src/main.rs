use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;

use clap::{App, Arg};

use libmj::daemon::MjDaemon;
use libmj::request::MasterRequest;

use libheartbeat::command::Command as FdCommand;
use libheartbeat::heartbeat::{FdDaemon, HeartbeatStyle};
use libheartbeat::member::MemberId;

use libsdfs::client::SdfsClient;
use libsdfs::daemon::SdfsDaemon;
use libsdfs::message::Message;

use libutils::bincode;
use libutils::log::*;
use libutils::logger::initialize_logging;
use libutils::net::{get_ip, get_socket_addr, read_with_len, write_with_len, write_with_len_tokio};
use libutils::tokio;
use libutils::tokio::net::{TcpListener, TcpStream, UdpSocket};
use libutils::tokio::sync::mpsc;

// Run MJ server on base_port, SDFS server on base_port+1, and the failure detector on base_port+2
const SDFS_PORT_OFFSET: u16 = 1;
const FD_PORT_OFFSET: u16 = 2;

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("mj-server")
        .version("0.1")
        .author("James Wang <jameswang9909@hotmail.com>, Rishi Desai <desai.rishi1@gmail.com>")
        .about("MapleJuice daemon")
        .arg(
            Arg::new("master")
                .short('m')
                .long("master")
                .value_name("MASTER")
                .about("The hostname of the master"),
        )
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .about("Base port for all services")
                .default_value("3000"),
        )
        .arg(
            Arg::new("log")
                .short('l')
                .long("log")
                .value_name("LOG_PATH")
                .about("File to log to (default: stdout)"),
        )
        .arg(
            Arg::new("base_path")
                .short('b')
                .long("base_path")
                .value_name("BASE_PATH")
                .about("Base path for storing files")
                .default_value("files"),
        )
        .get_matches();

    initialize_logging(matches.value_of("log"))?;

    let base_port: u16 = matches.value_of_t_or_exit("port");

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<FdCommand>();
    std::thread::spawn(move || {
        let mut buffer = String::new();
        loop {
            std::io::stdin()
                .read_line(&mut buffer)
                .map(|_| {
                    let command = &buffer.trim_end();
                    // Attempt to parse as failure detector command
                    match FdCommand::from_str(&command) {
                        Ok(c) => {
                            cmd_tx.send(c).expect("failed to send command");
                        }
                        Err(e) => {
                            if *command == "store" {
                                let mut stream = std::net::TcpStream::connect(format!(
                                    "0.0.0.0:{}",
                                    base_port + SDFS_PORT_OFFSET
                                ))
                                .expect("failed to connect to self");

                                let message = bincode::serialize(&Message::Store()).unwrap();
                                write_with_len(&mut stream, &message).unwrap();
                                let res = read_with_len(&mut stream).unwrap();
                                let files: Vec<String> = bincode::deserialize(&res)
                                    .expect("failed to deserialize response");

                                if files.is_empty() {
                                    info!("No files are stored on this machine.");
                                } else {
                                    info!("Files:");
                                    for file in files {
                                        info!("{}", file);
                                    }
                                }
                            } else {
                                error!("{}", e)
                            }
                        }
                    }
                })
                .unwrap();
            buffer.clear();
        }
    });

    let master: Option<SocketAddr> = matches
        .value_of("master")
        .map(|i| get_socket_addr(i).unwrap());

    let ip = get_ip()?;
    let (member_tx, mut member_rx) = mpsc::unbounded_channel::<(MemberId, bool)>();
    let member_rx_task = {
        // Listen for membership updates
        tokio::spawn(async move {
            loop {
                let update = member_rx.recv().await;
                if let Some((mut member, joined)) = update {
                    // Calculate port offsets
                    let member_base_port = member.0.port() - FD_PORT_OFFSET;
                    let member_sdfs_port = member_base_port + SDFS_PORT_OFFSET;

                    if joined {
                        // A node has joined

                        // notify SDFS
                        let mut stream =
                            TcpStream::connect(format!("0.0.0.0:{}", base_port + SDFS_PORT_OFFSET))
                                .await
                                .unwrap();
                        member.0.set_port(member_sdfs_port);
                        let message = bincode::serialize(&Message::Join(member)).unwrap();
                        write_with_len_tokio(&mut stream, &message).await.unwrap();

                        if master.is_none() {
                            // notify MJ
                            let mut stream = TcpStream::connect(format!("0.0.0.0:{}", base_port))
                                .await
                                .unwrap();
                            member.0.set_port(member_base_port);
                            let message = bincode::serialize(&MasterRequest::Join(member)).unwrap();
                            write_with_len_tokio(&mut stream, &message).await.unwrap();
                        }
                    } else {
                        // A node has left/failed

                        // notify SDFS
                        let mut stream =
                            TcpStream::connect(format!("0.0.0.0:{}", base_port + SDFS_PORT_OFFSET))
                                .await
                                .unwrap();
                        member.0.set_port(member_sdfs_port);
                        let message = bincode::serialize(&Message::Leave(member)).unwrap();
                        write_with_len_tokio(&mut stream, &message).await.unwrap();

                        if master.is_none() {
                            // notify MJ
                            let mut stream = TcpStream::connect(format!("0.0.0.0:{}", base_port))
                                .await
                                .unwrap();
                            member.0.set_port(member_base_port);
                            let message =
                                bincode::serialize(&MasterRequest::Leave(member)).unwrap();
                            write_with_len_tokio(&mut stream, &message).await.unwrap();
                        }
                    }
                }
            }
        })
    };

    let base_path = matches.value_of_os("base_path").unwrap().to_owned();
    if let Err(e) = tokio::fs::remove_dir_all(&base_path).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(e.into());
        }
    };

    let sdfs_task = {
        let sdfs_listener =
            TcpListener::bind(format!("0.0.0.0:{}", base_port + SDFS_PORT_OFFSET)).await?;
        let master = master.map(|m| SocketAddr::new(m.ip(), m.port() + SDFS_PORT_OFFSET));
        let sdfs_daemon = SdfsDaemon::new(sdfs_listener, master, base_path);
        tokio::spawn(async move {
            sdfs_daemon.run().await;
        })
    };
    let mj_task = {
        let sdfs_master = match master {
            Some(master) => SocketAddr::new(master.ip(), master.port() + SDFS_PORT_OFFSET),
            None => SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                base_port + SDFS_PORT_OFFSET,
            ),
        };
        let sdfs_client = SdfsClient::new(sdfs_master);
        let mj_listener = TcpListener::bind(format!("0.0.0.0:{}", base_port)).await?;
        let id = SocketAddr::new(get_ip()?, base_port);
        let mut mj_daemon = MjDaemon::new(mj_listener, sdfs_client, id);
        tokio::spawn(async move {
            if let Some(master) = master {
                mj_daemon.run_worker(master).await;
            } else {
                mj_daemon.run_master().await;
            }
        })
    };

    let fd_task = {
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", base_port + FD_PORT_OFFSET)).await?;
        tokio::spawn(async move {
            let fd_daemon = FdDaemon::new(
                ip.to_owned(),
                base_port + FD_PORT_OFFSET,
                HeartbeatStyle::AllToAll,
                0.0,
            );
            let master = master.map(|m| SocketAddr::new(m.ip(), m.port() + FD_PORT_OFFSET));
            fd_daemon
                .run(socket, master, cmd_rx, member_tx)
                .await
                .unwrap();
        })
    };
    tokio::select! {
        res = fd_task => {
            if let Err(e) = res {
                error!("Error in failure detector: {}", e);
            }
        }
        res = member_rx_task => {
            if let Err(e) = res {
                error!("Error in failure detector: {}", e);
            }
        }
        res = sdfs_task => {
            if let Err(e) = res {
                error!("Error in SDFS daemon: {}", e);
            }
        }
        res = mj_task => {
            if let Err(e) = res {
                error!("Error in MJ daemon: {}", e);
            }
        }
    };

    Ok(())
}
