use crate::broadcast::SubscriberId;
use actix_ws::Closed;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
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
    #[error("Missing updates: {0}")]
    MissingUpdate(Box<MissingUpdate>),
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

#[derive(Debug)]
pub struct MissingUpdate {
    pub since: StateVector,
    pub last_msg_id: Option<Arc<str>>,
}

impl MissingUpdate {
    pub fn new(since: StateVector, last_msg_id: Option<Arc<str>>) -> Self {
        Self { since, last_msg_id }
    }
}

impl Display for MissingUpdate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(last_msg_id) = &self.last_msg_id {
            write!(f, "last_msg_id: {:?}, ", last_msg_id)?;
        }
        write!(f, "state vector: {:?}", self.since)?;
        Ok(())
    }
}
