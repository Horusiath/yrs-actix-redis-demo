use crate::broadcast::SubscriberId;
use actix_ws::Closed;
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
    #[error("Web service error: {0}")]
    Actix(#[from] actix_web::Error),
    #[error("Web socket error: {0}")]
    WsProtocol(#[from] actix_ws::ProtocolError),
    #[error("Yrs sync protocol error: {0}")]
    SyncProtocol(#[from] yrs::sync::Error),
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("{0}")]
    Other(Box<dyn std::error::Error>),
}

impl From<Closed> for Error {
    fn from(_: Closed) -> Self {
        Self::ConnectionClosed
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Other(Box::new(error))
    }
}
