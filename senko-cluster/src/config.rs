use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_addr: SocketAddr,
    pub cluster_addr: SocketAddr,
    pub num_shards: usize,
    pub node_timeout_ms: u64,
    pub proxy_remote: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        let node_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379);
        Self {
            enabled: false,
            cluster_addr: default_cluster_addr(node_addr),
            node_addr,
            num_shards: num_cpus::get().max(1),
            node_timeout_ms: 15_000,
            proxy_remote: false,
        }
    }
}

impl ClusterConfig {
    #[inline]
    pub fn with_node_addr(node_addr: SocketAddr) -> Self {
        Self {
            node_addr,
            cluster_addr: default_cluster_addr(node_addr),
            ..Self::default()
        }
    }
}

#[inline]
fn default_cluster_addr(node_addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(node_addr.ip(), node_addr.port().saturating_add(10_000))
}
