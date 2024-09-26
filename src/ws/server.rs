use super::{DocId, SessionId};
use actix::{Actor, Handler, Recipient};
use std::collections::{HashMap, HashSet};

pub struct WsServer {
    sessions: HashMap<SessionId, Recipient<super::session::Message>>,
    topics: HashMap<DocId, HashSet<SessionId>>,
}

impl WsServer {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            topics: HashMap::new(),
        }
    }

    pub fn send_message(
        &self,
        topic_id: &DocId,
        message: yrs::sync::Message,
        sender: Option<&SessionId>,
    ) {
        match message {
            yrs::sync::Message::Sync(yrs::sync::SyncMessage::SyncStep1(state_vector)) => {
                todo!();
            }
            yrs::sync::Message::Sync(yrs::sync::SyncMessage::SyncStep2(init_state)) => {
                todo!();
            }
            yrs::sync::Message::Sync(yrs::sync::SyncMessage::Update(update)) => {
                todo!();
            }
            yrs::sync::Message::Auth(deny_reason) => {
                todo!()
            }
            yrs::sync::Message::Awareness(awareness_update) => {
                todo!()
            }
            yrs::sync::Message::AwarenessQuery => {
                todo!()
            }
            yrs::sync::Message::Custom(tag, bytes) => {
                todo!()
            }
        }
    }
}

impl Actor for WsServer {
    type Context = actix::Context<Self>;
}

impl Handler<Join> for WsServer {
    type Result = ();

    fn handle(&mut self, msg: Join, ctx: &mut Self::Context) -> Self::Result {
        self.sessions.insert(msg.session_id, msg.addr);
        for topic in msg.topics {
            self.topics.entry(topic).or_default().insert(msg.session_id);
        }
    }
}

impl Handler<Leave> for WsServer {
    type Result = ();

    fn handle(&mut self, msg: Leave, ctx: &mut Self::Context) -> Self::Result {
        if self.sessions.remove(&msg.session_id).is_some() {
            let topics = self
                .topics
                .iter_mut()
                .filter(|(&topic_id, _)| match msg.topics {
                    Some(ref topics) => topics.contains(&topic_id),
                    None => true,
                });
            let mut empty_topics = Vec::new();
            for (topic_id, sessions) in topics {
                sessions.remove(&msg.session_id);
                if sessions.is_empty() {
                    empty_topics.push(topic_id.clone());
                }
            }

            for topic in empty_topics {
                self.topics.remove(&topic);
            }
        }
    }
}

impl Handler<ClientMessage> for WsServer {
    type Result = ();

    fn handle(&mut self, msg: ClientMessage, ctx: &mut Self::Context) -> Self::Result {
        self.send_message(&msg.topic, msg.message, Some(&msg.sender));
    }
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct Join {
    /// Current client session identifier.
    pub session_id: SessionId,
    /// Actix WebSocket session actor address.
    pub addr: Recipient<super::session::Message>,
    /// List of topics to join.
    pub topics: Vec<DocId>,
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct Leave {
    /// Current client session identifier.
    pub session_id: SessionId,
    /// List of topics to leave. If `None`, leave all topics.
    pub topics: Option<Vec<DocId>>,
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct ClientMessage {
    pub sender: SessionId,
    pub topic: DocId,
    pub message: yrs::sync::Message,
}
