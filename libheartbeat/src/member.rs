use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

use rand::prelude::IteratorRandom;
use rand::thread_rng;

use libutils::log::*;
use libutils::serde::{Deserialize, Serialize};
use libutils::time::Timestamp;

pub type MemberId = (SocketAddr, Timestamp);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemberMap(HashMap<MemberId, Timestamp>);

impl Deref for MemberMap {
    type Target = HashMap<MemberId, Timestamp>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MemberMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl MemberMap {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn update_member(&mut self, id: MemberId, now: Timestamp) -> bool {
        let mut new = false;
        self.entry(id)
            .and_modify(|e| {
                if now > *e {
                    *e = now
                }
            })
            .or_insert_with(|| {
                info!("Adding {}_{} to the membership list", id.0, id.1);
                new = true;
                now
            });
        new
    }

    pub fn remove_failed(&mut self, now: Timestamp, expiry: u128) {
        self.0.retain(|(addr, join_time), &mut timestamp| {
            let retain = (now - timestamp) < expiry;
            if !retain {
                info!("Node {}_{} has failed", addr, join_time);
            };
            retain
        });
    }

    pub fn random(&self, n: usize) -> Vec<&MemberId> {
        self.keys()
            .into_iter()
            .choose_multiple(&mut thread_rng(), n)
    }
}
