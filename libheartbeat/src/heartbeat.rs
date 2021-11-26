use crate::command::Command;
use crate::member::{MemberId, MemberMap};
use crate::message::Message;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use libutils::anyhow::Result;
use libutils::bincode;
use libutils::log::*;
use libutils::serde::{Deserialize, Serialize};
use libutils::time::{time_since_epoch, Timestamp};
use libutils::tokio;
use libutils::tokio::net::UdpSocket;
use libutils::tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use libutils::tokio::time::{sleep, Duration};

const HEARTBEAT_EXPIRY: u128 = Duration::from_secs(2).as_millis();
const MAX_BUF_SIZE: usize = 1024;
const MEMBER_SUBSET_K: usize = 4; // Number of subsets to gossip for
const C: f64 = 1.25; // Number of rounds of log(n) to gossip for

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HeartbeatStyle {
    Gossip,
    AllToAll,
}

impl HeartbeatStyle {
    pub fn sleep_interval(&self, n: usize) -> Duration {
        let millis = match self {
            HeartbeatStyle::Gossip => {
                ((HEARTBEAT_EXPIRY as f64)
                    / (C * std::cmp::max((n as f64).log2().ceil() as u64, 1) as f64) as f64)
                    as u64
            }
            HeartbeatStyle::AllToAll => 500,
        };
        Duration::from_millis(millis)
    }

    async fn heartbeat(
        &self,
        members: &mut MemberMap,
        join_time: Timestamp,
        socket: &Arc<UdpSocket>,
        fail_rate: f32,
        member_tx: &mut mpsc::UnboundedSender<(MemberId, bool)>,
    ) {
        if members.is_empty() {
            return;
        }
        let now = time_since_epoch();
        match self {
            HeartbeatStyle::AllToAll => {
                let message = Message::Ping(join_time);
                broadcast_message(members.keys(), socket, message, fail_rate).await;
            }
            HeartbeatStyle::Gossip => {
                let message = Message::Gossip(join_time, members.clone());
                broadcast_message(members.random(MEMBER_SUBSET_K), socket, message, fail_rate)
                    .await;
            }
        }

        // CLean up failed nodes
        members.retain(|(addr, join_time), &mut timestamp| {
            let retain = (now - timestamp) < HEARTBEAT_EXPIRY;
            if !retain {
                info!("Node {}_{} has failed", addr, join_time);
                member_tx.send(((*addr, *join_time), false)).unwrap()
            };
            retain
        });
    }
}

async fn heartbeat(
    inner: &Arc<Inner>,
    socket: &Arc<UdpSocket>,
    fail_rate: f32,
    member_tx: &mut mpsc::UnboundedSender<(MemberId, bool)>,
) {
    let mut members = inner.members.lock().await;
    let join_time = *inner.join_time.read().await;

    inner
        .style
        .read()
        .await
        .heartbeat(&mut members, join_time, socket, fail_rate, member_tx)
        .await;
}

async fn introduce(join_time: Timestamp, socket: &Arc<UdpSocket>, addr: SocketAddr) {
    let message = Message::Join(join_time);
    let encoded = bincode::serialize(&message).expect("failed to serialize message");
    socket
        .send_to(&encoded, &addr)
        .await
        .expect("failed to send to socket");
}

async fn broadcast_message<'a, I>(
    members: I,
    socket: &Arc<UdpSocket>,
    message: Message,
    fail_rate: f32,
) where
    I: IntoIterator<Item = &'a MemberId>,
{
    let encoded = bincode::serialize(&message).expect("failed to serialize message");
    for (addr, join_time) in members {
        if rand::random::<f32>() < fail_rate {
            continue;
        }
        socket
            .send_to(&encoded, &addr)
            .await
            .expect("failed to send to socket");
        debug!("Sent {} to {}_{}", message, addr, join_time);
    }
}

async fn handle_message(
    inner: &Arc<Inner>,
    message: Message,
    self_addr: &SocketAddr,
    sender_addr: SocketAddr,
    socket: &Arc<UdpSocket>,
    member_tx: &mut mpsc::UnboundedSender<(MemberId, bool)>,
) {
    debug!("Received {} from {}", message, sender_addr);

    let now = time_since_epoch();
    let mut members = inner.members.lock().await;
    match message {
        Message::Ping(join_time) => {
            if members.update_member((sender_addr, join_time), now) {
                member_tx.send(((sender_addr, join_time), true)).unwrap();
            }
        }
        Message::Join(join_time) => {
            info!("{}_{} joined the group", sender_addr, join_time);
            member_tx.send(((sender_addr, join_time), true)).unwrap();
            let message = Message::JoinAck(*inner.join_time.read().await, members.clone());
            let encoded = bincode::serialize(&message).expect("failed to serialize message");
            socket
                .send_to(&encoded, &sender_addr)
                .await
                .expect("failed to send to socket");

            members.insert((sender_addr, join_time), now);
        }
        Message::JoinAck(join_time, new_member_list) => {
            info!("Updating membership list from introducer");
            let self_join_time = inner.join_time.read().await;
            // Send membership update for self
            member_tx
                .send(((*self_addr, *self_join_time), true))
                .unwrap();

            *members = new_member_list;
            members.insert((sender_addr, join_time), now);
            for key in members.keys() {
                member_tx.send((*key, true)).unwrap();
            }
        }
        Message::Leave(join_time) => {
            info!("{}_{} left the group", sender_addr, join_time);
            members.remove(&(sender_addr, join_time));
            member_tx.send(((sender_addr, join_time), false)).unwrap();
        }
        Message::Gossip(join_time, new_member_list) => {
            // Merge membership lists
            for (member, &last_seen) in new_member_list.iter() {
                let (addr, _) = member;
                if addr == self_addr {
                    // Skip ourselves
                    return;
                }
                if now - last_seen > HEARTBEAT_EXPIRY {
                    return;
                }
                // Only deal with members who still alive
                if members.update_member(*member, last_seen) {
                    member_tx.send((*member, true)).unwrap();
                }
            }
            if members.update_member((sender_addr, join_time), now) {
                member_tx.send(((sender_addr, join_time), true)).unwrap();
            }
        }
        Message::SetStyle(new_style) => {
            info!("Setting heartbeat style to {:?}", new_style);
            *inner.style.write().await = new_style;
        }
    };
}

pub struct Inner {
    members: Mutex<MemberMap>,
    join_time: RwLock<Timestamp>,
    style: RwLock<HeartbeatStyle>,
}

pub struct FdDaemon {
    ip: IpAddr,
    port: u16,
    fail_rate: f32,
    inner: Arc<Inner>,
}

impl FdDaemon {
    pub async fn run(
        self,
        socket: UdpSocket,
        introducer: Option<SocketAddr>,
        mut cmd_rx: mpsc::UnboundedReceiver<Command>,
        member_tx: mpsc::UnboundedSender<(MemberId, bool)>,
    ) -> Result<()> {
        let socket = Arc::new(socket);

        let fail_rate = self.fail_rate;

        // Signal that send/receive tasks should sleep
        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        // Signal that send/receive tasks should wake up
        let (start_tx, _) = broadcast::channel::<()>(1);
        // Signal that send/receive tasks are ready to sleep
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(2);

        let self_addr = SocketAddr::new(self.ip.clone(), self.port.clone());
        let recv_task = {
            let mut shutdown_rx = shutdown_tx.subscribe();
            let mut start_rx = start_tx.subscribe();
            let shutdown_complete_tx = shutdown_complete_tx.clone();
            let socket = Arc::clone(&socket);
            let inner = Arc::clone(&self.inner);
            let mut member_tx = member_tx.clone();
            tokio::spawn(async move {
                let mut buf = [0; MAX_BUF_SIZE];
                loop {
                    loop {
                        let maybe_conn = tokio::select! {
                            res = socket.recv_from(&mut buf) => res.ok(),
                            _ = shutdown_rx.recv() => None
                        };
                        match maybe_conn {
                            Some((n, addr)) => {
                                let message: Message = bincode::deserialize(&buf[..n])
                                    .expect("failed to deserialize message");
                                handle_message(
                                    &inner,
                                    message,
                                    &self_addr,
                                    addr,
                                    &socket,
                                    &mut member_tx,
                                )
                                .await;
                            }
                            None => break,
                        }
                    }
                    shutdown_complete_tx.send(()).await.unwrap();
                    start_rx.recv().await.unwrap();
                }
            })
        };
        let send_task = {
            let mut shutdown_rx = shutdown_tx.subscribe();
            let mut start_rx = start_tx.subscribe();
            let shutdown_complete_tx = shutdown_complete_tx.clone();
            let socket = Arc::clone(&socket);
            let inner = Arc::clone(&self.inner);
            let mut member_tx = member_tx.clone();
            tokio::spawn(async move {
                loop {
                    if let Some(i_addr) = introducer {
                        introduce(*inner.join_time.read().await, &socket, i_addr).await;
                    }
                    loop {
                        tokio::select! {
                            _ = shutdown_rx.recv() => break,
                            _ = heartbeat(&inner, &socket, fail_rate, &mut member_tx) => {
                                let style = inner.style.read().await;
                                let n = inner.members.lock().await.len();
                                let delay = style.sleep_interval(n);
                                sleep(delay).await;
                            }
                        }
                    }
                    shutdown_complete_tx.send(()).await.unwrap();
                    start_rx.recv().await.unwrap();
                }
            })
        };
        let command_task = {
            let inner = Arc::clone(&self.inner);
            let socket = Arc::clone(&socket);
            tokio::spawn(async move {
                loop {
                    let c = cmd_rx.recv().await.expect("failed to listen for event");
                    match c {
                        Command::LeaveGroup => {
                            shutdown_tx.send(()).unwrap();

                            // Wait for all tasks to finish their work
                            // We expect 2 messages for this receiver (2 tasks)
                            shutdown_complete_rx.recv().await;
                            shutdown_complete_rx.recv().await;

                            let message = Message::Leave(*inner.join_time.read().await);
                            broadcast_message(
                                inner.members.lock().await.keys(),
                                &socket,
                                message,
                                0.0,
                            )
                            .await;
                            info!("Left group");
                        }
                        Command::JoinGroup => {
                            self.inner.members.lock().await.clear();
                            *self.inner.join_time.write().await = time_since_epoch();
                            start_tx.send(()).unwrap();
                            info!("Joining group");
                        }
                        Command::WhoAmI => info!(
                            "Id: {}",
                            Self::id(self.ip, self.port, *self.inner.join_time.read().await)
                        ),
                        Command::ShowMembershipList => {
                            info!("Membership list:");
                            let members = self.inner.members.lock().await;
                            members.iter().for_each(|((addr, join_time), last_seen)| {
                                info!("{}_{}:\t\t{}", addr, join_time, last_seen);
                            });

                            let now = time_since_epoch();
                            let id =
                                Self::id(self.ip, self.port, *self.inner.join_time.read().await);
                            info!("{} (me):\t{}", id, now);
                        }
                        Command::SetStyle(style) => {
                            info!("Changing heartbeat style to {:?}", style);
                            // Change locally
                            *inner.style.write().await = style.clone();

                            // Broadcast to everyone
                            let message = Message::SetStyle(style);
                            let members = self.inner.members.lock().await;
                            broadcast_message(members.keys(), &socket, message, 0.0).await;
                        }
                    }
                }
            })
        };
        tokio::select! {
            res = recv_task => {
                if let Err(e) = res {
                    error!("Error when receiving: {}", e);
                }
            }
            res = send_task => {
                if let Err(e) = res {
                    error!("Error when sending: {}", e);
                }
            }
            res = command_task => {
                if let Err(e) = res {
                    error!("Error when handling command: {}", e);
                }
            }
        };

        Ok(())
    }
}

impl FdDaemon {
    pub fn new(ip: IpAddr, port: u16, style: HeartbeatStyle, fail_rate: f32) -> Self {
        Self {
            ip,
            port,
            fail_rate,
            inner: Arc::new(Inner {
                members: Mutex::new(MemberMap::new()),
                join_time: RwLock::new(time_since_epoch()),
                style: RwLock::new(style),
            }),
        }
    }

    fn id(ip: IpAddr, port: u16, join_time: Timestamp) -> String {
        format!("{}:{}_{}", ip, port, join_time)
    }
}
