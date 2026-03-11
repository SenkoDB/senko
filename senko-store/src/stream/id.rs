use std::fmt;

use compact_str::CompactString;
use senko_core::SenkoError;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    // Sentinel for '*' auto-generated IDs. Handlers should resolve this before insert.
    pub const AUTO: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    // Sentinel for 'ms-*' partial auto IDs.
    pub const PARTIAL_AUTO_SEQ: u64 = u64::MAX;

    pub fn auto_generate(last_id: StreamId, now_ms: u64) -> StreamId {
        if now_ms > last_id.ms {
            StreamId { ms: now_ms, seq: 0 }
        } else {
            StreamId {
                ms: last_id.ms.max(now_ms),
                seq: last_id.seq.saturating_add(1),
            }
        }
    }

    pub fn parse(s: &[u8]) -> Result<StreamId, SenkoError> {
        parse_with_default_seq(s, 0)
    }

    pub fn parse_range_start(s: &[u8]) -> Result<StreamId, SenkoError> {
        if s == b"-" {
            return Ok(StreamId::ZERO);
        }
        parse_with_default_seq(s, 0)
    }

    pub fn parse_range_end(s: &[u8]) -> Result<StreamId, SenkoError> {
        if s == b"+" {
            return Ok(StreamId::MAX);
        }
        parse_with_default_seq(s, u64::MAX)
    }

    pub fn to_string(&self) -> CompactString {
        CompactString::new(format!("{}-{}", self.ms, self.seq))
    }

    pub fn as_be_bytes(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.ms.to_be_bytes());
        out[8..].copy_from_slice(&self.seq.to_be_bytes());
        out
    }
}

impl Ord for StreamId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ms
            .cmp(&other.ms)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

fn parse_with_default_seq(s: &[u8], default_seq: u64) -> Result<StreamId, SenkoError> {
    if s == b"*" {
        return Ok(StreamId::AUTO);
    }

    let text = std::str::from_utf8(s).map_err(|_| SenkoError::Protocol("invalid stream id"))?;
    if text.is_empty() {
        return Err(SenkoError::Protocol("invalid stream id"));
    }

    if let Some((ms_str, seq_str)) = text.split_once('-') {
        let ms = parse_u64(ms_str)?;
        if seq_str == "*" {
            return Ok(StreamId {
                ms,
                seq: StreamId::PARTIAL_AUTO_SEQ,
            });
        }
        let seq = parse_u64(seq_str)?;
        Ok(StreamId { ms, seq })
    } else {
        let ms = parse_u64(text)?;
        Ok(StreamId {
            ms,
            seq: default_seq,
        })
    }
}

fn parse_u64(s: &str) -> Result<u64, SenkoError> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(SenkoError::Protocol("invalid stream id"));
    }
    s.parse::<u64>()
        .map_err(|_| SenkoError::Protocol("invalid stream id"))
}

#[cfg(test)]
mod tests {
    use super::StreamId;

    #[test]
    fn stream_id_parsing_variants() {
        assert_eq!(StreamId::parse(b"*").unwrap(), StreamId::AUTO);
        assert_eq!(
            StreamId::parse(b"1234567890-0").unwrap(),
            StreamId {
                ms: 1234567890,
                seq: 0
            }
        );
        assert_eq!(
            StreamId::parse(b"1234567890").unwrap(),
            StreamId {
                ms: 1234567890,
                seq: 0
            }
        );
        assert_eq!(
            StreamId::parse(b"1234567890-*").unwrap(),
            StreamId {
                ms: 1234567890,
                seq: StreamId::PARTIAL_AUTO_SEQ
            }
        );
        assert_eq!(StreamId::parse_range_start(b"-").unwrap(), StreamId::ZERO);
        assert_eq!(StreamId::parse_range_end(b"+").unwrap(), StreamId::MAX);
    }

    #[test]
    fn stream_id_ordering() {
        assert!(
            StreamId { ms: 1, seq: 0 } < StreamId { ms: 1, seq: 1 }
                && StreamId { ms: 1, seq: 1 } < StreamId { ms: 2, seq: 0 }
        );
    }

    #[test]
    fn auto_generation_handles_clock_skew() {
        let last = StreamId { ms: 1000, seq: 3 };
        assert_eq!(
            StreamId::auto_generate(last, 2000),
            StreamId { ms: 2000, seq: 0 }
        );
        assert_eq!(
            StreamId::auto_generate(last, 1000),
            StreamId { ms: 1000, seq: 4 }
        );
        assert_eq!(
            StreamId::auto_generate(last, 900),
            StreamId { ms: 1000, seq: 4 }
        );
    }
}
