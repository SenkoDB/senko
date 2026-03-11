use bytes::Bytes;
use compact_str::CompactString;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageKind {
    Message,
    PMessage { pattern: CompactString },
    SMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubMessage {
    pub channel: CompactString,
    pub payload: Bytes,
    pub kind: MessageKind,
}
