use crate::broadcast::SubscriberId;
use yrs::StateVector;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("missing field in Redis message: {field}")]
    MissingField { field: &'static str },
    #[error("subscriber `{0}` sink has been dropped")]
    SubscriberDropped(SubscriberId),
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("Failed to decode Yrs update: {0}")]
    Serialization(#[from] yrs::encoding::read::Error),
    #[error("Failed to apply Yrs update: {0}")]
    Update(#[from] yrs::error::UpdateError),
    #[error("S3 operation failed: {0}")]
    S3(#[from] opendal::Error),
    #[error("Missing updates since: {0:?}")]
    MissingUpdate(StateVector),
}
