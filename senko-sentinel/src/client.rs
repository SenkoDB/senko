use std::net::SocketAddr;

use ahash::RandomState;
use bytes::BytesMut;
use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
    net::TcpStream,
};
use hashbrown::HashMap;
use senko_core::{SenkoError, SenkoResult};
use senko_proto::{ParseStatus, RespParser, RespSerializer};

pub struct SentinelClientPool {
    streams: HashMap<SocketAddr, TcpStream, RandomState>,
}

impl Default for SentinelClientPool {
    fn default() -> Self {
        Self {
            streams: HashMap::with_hasher(RandomState::new()),
        }
    }
}

impl SentinelClientPool {
    pub async fn ping(&mut self, addr: SocketAddr) -> SenkoResult<()> {
        let _ = self.request(addr, &[b"PING"]).await?;
        Ok(())
    }

    pub async fn info(&mut self, addr: SocketAddr) -> SenkoResult<Vec<u8>> {
        self.request(addr, &[b"INFO", b"replication"]).await
    }

    pub async fn slaveof_no_one(&mut self, addr: SocketAddr) -> SenkoResult<Vec<u8>> {
        self.request(addr, &[b"SLAVEOF", b"NO", b"ONE"]).await
    }

    pub async fn slaveof(&mut self, addr: SocketAddr, target: SocketAddr) -> SenkoResult<Vec<u8>> {
        let ip = target.ip().to_string();
        let port = target.port().to_string();
        self.request(addr, &[b"SLAVEOF", ip.as_bytes(), port.as_bytes()])
            .await
    }

    pub async fn publish_hello(&mut self, addr: SocketAddr, payload: &str) -> SenkoResult<Vec<u8>> {
        self.request(
            addr,
            &[b"PUBLISH", b"__sentinel__:hello", payload.as_bytes()],
        )
        .await
    }

    async fn request(&mut self, addr: SocketAddr, args: &[&[u8]]) -> SenkoResult<Vec<u8>> {
        let stream = match self.streams.entry(addr) {
            hashbrown::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            hashbrown::hash_map::Entry::Vacant(entry) => {
                entry.insert(TcpStream::connect(addr).await?)
            }
        };
        let mut out = BytesMut::with_capacity(64);
        RespSerializer::write_array_header(&mut out, args.len());
        for arg in args {
            RespSerializer::write_bulk_string(&mut out, arg);
        }
        let BufResult(result, _) = stream.write_all(out.freeze()).await;
        result?;
        let BufResult(result, mut input) = stream.read(Vec::with_capacity(4096)).await;
        let read = result?;
        input.truncate(read);
        let parser = RespParser::new();
        match parser.parse(&input)? {
            ParseStatus::Complete(_, _) => Ok(input),
            ParseStatus::Incomplete(_) => Err(SenkoError::Protocol("incomplete sentinel response")),
        }
    }
}
