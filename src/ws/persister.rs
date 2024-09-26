use crate::snapshot::LoadedSnapshot;
use crate::ws::DocId;
use actix::{Actor, Handler, Recipient};
use opendal::Operator;
use redis::aio::ConnectionManager;
use yrs::StateVector;

pub struct Persister {
    storage: Operator,
    conn: ConnectionManager,
}

impl Persister {
    pub fn new(storage: Operator, conn: ConnectionManager) -> Self {
        Self { storage, conn }
    }
}

impl Actor for Persister {
    type Context = actix::Context<Self>;
}

impl Handler<LoadDocMessage> for Persister {
    type Result = ();

    fn handle(&mut self, msg: LoadDocMessage, ctx: &mut Self::Context) -> Self::Result {
        todo!()
    }
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct LoadDocMessage {
    pub topic_id: DocId,
    pub version: Option<StateVector>,
    pub reply_to: Option<Recipient<super::session::Message>>,
}
