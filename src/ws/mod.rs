use uuid::Uuid;

mod persister;
mod server;
mod session;

pub type SessionId = u64;
pub type DocId = Uuid;
