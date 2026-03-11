use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum TsError {
    #[error("ERR TSDB: key already exists")]
    KeyExists,
    #[error("ERR TSDB: the key does not exist")]
    KeyNotFound,
    #[error("ERR TSDB: Wrong index filter expression")]
    BadFilter,
    #[error("ERR TSDB: BLOCK duplicate policy")]
    Blocked,
    #[error("ERR TSDB: invalid timestamp")]
    BadTimestamp,
    #[error("ERR TSDB: invalid value")]
    BadValue,
    #[error("ERR TSDB: compaction rule already exists")]
    RuleExists,
    #[error("ERR TSDB: compaction rule does not exist")]
    RuleNotFound,
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
}
