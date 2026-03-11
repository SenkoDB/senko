use senko_core::{SenkoKey, SenkoValue};

use crate::store::{SetOptions, Store};

#[derive(Debug, Clone, PartialEq)]
pub enum StoreCommand {
    Ping,
    Get(SenkoKey),
    Set {
        key: SenkoKey,
        value: SenkoValue,
        options: SetOptions,
    },
    Del(SenkoKey),
    Exists(SenkoKey),
    TtlMs(SenkoKey),
    PExpireAt {
        key: SenkoKey,
        expires_at: u64,
    },
    Persist(SenkoKey),
}

#[derive(Debug, Clone, PartialEq)]
pub enum StoreResponse {
    Pong,
    Value(Option<SenkoValue>),
    Integer(u64),
    Ttl(Option<i64>),
    Set(crate::store::SetResult),
}

impl Store {
    pub fn execute(&mut self, command: StoreCommand) -> StoreResponse {
        match command {
            StoreCommand::Ping => StoreResponse::Pong,
            StoreCommand::Get(key) => StoreResponse::Value(self.get(key.as_bytes()).cloned()),
            StoreCommand::Set {
                key,
                value,
                options,
            } => StoreResponse::Set(self.set(key, value, options)),
            StoreCommand::Del(key) => StoreResponse::Integer(self.delete(key.as_bytes()) as u64),
            StoreCommand::Exists(key) => StoreResponse::Integer(self.exists(key.as_bytes()) as u64),
            StoreCommand::TtlMs(key) => StoreResponse::Ttl(self.ttl_ms(key.as_bytes())),
            StoreCommand::PExpireAt { key, expires_at } => {
                let exists_before = self.exists(key.as_bytes());
                if exists_before {
                    self.set_expiry(key.as_bytes(), expires_at);
                }
                StoreResponse::Integer(exists_before as u64)
            }
            StoreCommand::Persist(key) => {
                let had_key = self.exists(key.as_bytes());
                let had_ttl = self.ttl_ms(key.as_bytes()).is_some_and(|ttl| ttl >= 0);
                if had_key {
                    self.remove_expiry(key.as_bytes());
                }
                StoreResponse::Integer((had_key && had_ttl) as u64)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_core::SenkoValue;

    use crate::{
        command::{StoreCommand, StoreResponse},
        store::{SetCondition, SetExpiry, SetOptions, Store},
    };

    #[test]
    fn execute_routes_commands_to_store() {
        let mut store = Store::default();

        let set = store.execute(StoreCommand::Set {
            key: CompactString::from("key"),
            value: SenkoValue::from(Bytes::from_static(b"value")),
            options: SetOptions::default(),
        });
        assert!(matches!(set, StoreResponse::Set(result) if result.applied));

        assert_eq!(
            store.execute(StoreCommand::Get(CompactString::from("key"))),
            StoreResponse::Value(Some(SenkoValue::from(Bytes::from_static(b"value"))))
        );
        assert_eq!(
            store.execute(StoreCommand::Exists(CompactString::from("key"))),
            StoreResponse::Integer(1)
        );
        assert_eq!(
            store.execute(StoreCommand::Del(CompactString::from("key"))),
            StoreResponse::Integer(1)
        );
        assert_eq!(
            store.execute(StoreCommand::Get(CompactString::from("key"))),
            StoreResponse::Value(None)
        );
    }

    #[test]
    fn persist_and_ttl_follow_expiry_state() {
        let mut store = Store::default();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis() as u64);
        let _ = store.execute(StoreCommand::Set {
            key: CompactString::from("expiring"),
            value: SenkoValue::from(1_i64),
            options: SetOptions {
                condition: SetCondition::Always,
                expiry: SetExpiry::PxAt(now_ms + 5_000),
                get_old: false,
            },
        });

        let ttl = store.execute(StoreCommand::TtlMs(CompactString::from("expiring")));
        assert!(matches!(ttl, StoreResponse::Ttl(Some(value)) if value >= 0));

        assert_eq!(
            store.execute(StoreCommand::Persist(CompactString::from("expiring"))),
            StoreResponse::Integer(1)
        );
        assert_eq!(
            store.execute(StoreCommand::TtlMs(CompactString::from("expiring"))),
            StoreResponse::Ttl(Some(-1))
        );
    }
}
