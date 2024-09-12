use crate::broadcast::SubscriberId;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("missing field in Redis message: {field}")]
    MissingField { field: &'static str },
    #[error("subscriber `{0}` sink has been dropped")]
    SubscriberDropped(SubscriberId),
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
}
