use bytes::Bytes;
use compio::net::TcpStream;

#[derive(Debug)]
pub struct ConnectionState {
    pub stream: TcpStream,
    pub read_buffer: Bytes,
}

impl ConnectionState {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buffer: Bytes::new(),
        }
    }

    pub fn replace_read_buffer(&mut self, buffer: Bytes) -> Bytes {
        std::mem::replace(&mut self.read_buffer, buffer)
    }
}
