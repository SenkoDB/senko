use std::net::{IpAddr, SocketAddr};

use compact_str::CompactString;

use crate::state::{InstanceFlags, SentinelPeer, SentinelWorld, update_world};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloMessage {
    pub sentinel_ip: IpAddr,
    pub sentinel_port: u16,
    pub sentinel_runid: String,
    pub current_epoch: u64,
    pub master_name: String,
    pub master_ip: IpAddr,
    pub master_port: u16,
    pub master_config_epoch: u64,
}

impl HelloMessage {
    pub fn encode(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{}",
            self.sentinel_ip,
            self.sentinel_port,
            self.sentinel_runid,
            self.current_epoch,
            self.master_name,
            self.master_ip,
            self.master_port,
            self.master_config_epoch
        )
    }

    pub fn decode(input: &str) -> Option<Self> {
        let parts = input.split(',').collect::<Vec<_>>();
        if parts.len() != 8 {
            return None;
        }
        Some(Self {
            sentinel_ip: parts[0].parse().ok()?,
            sentinel_port: parts[1].parse().ok()?,
            sentinel_runid: parts[2].to_owned(),
            current_epoch: parts[3].parse().ok()?,
            master_name: parts[4].to_owned(),
            master_ip: parts[5].parse().ok()?,
            master_port: parts[6].parse().ok()?,
            master_config_epoch: parts[7].parse().ok()?,
        })
    }
}

pub fn apply_hello(world: &SentinelWorld, hello: &HelloMessage, now: u64) {
    let _ = update_world(world, |snapshot| {
        if let Some(master) = snapshot.masters.get_mut(&hello.master_name) {
            let peer_id = CompactString::from(hello.sentinel_runid.as_str());
            master.sentinels.insert(
                peer_id.clone(),
                SentinelPeer {
                    id: peer_id,
                    addr: SocketAddr::new(hello.sentinel_ip, hello.sentinel_port),
                    flags: InstanceFlags::SENTINEL,
                    last_hello: now,
                    last_ok_ping: now,
                    down_since: None,
                    voted_leader: None,
                    voted_epoch: 0,
                    runid: hello.sentinel_runid.clone(),
                },
            );
            if hello.master_config_epoch > master.config_epoch {
                master.addr = SocketAddr::new(hello.master_ip, hello.master_port);
                master.config_epoch = hello.master_config_epoch;
            }
            snapshot.epoch = snapshot.epoch.max(hello.current_epoch);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_round_trip_parses() {
        let hello = HelloMessage {
            sentinel_ip: "127.0.0.1".parse().expect("ip"),
            sentinel_port: 26_379,
            sentinel_runid: "runid".to_owned(),
            current_epoch: 2,
            master_name: "m".to_owned(),
            master_ip: "127.0.0.1".parse().expect("ip"),
            master_port: 6_379,
            master_config_epoch: 3,
        };
        assert_eq!(HelloMessage::decode(&hello.encode()), Some(hello));
    }
}
