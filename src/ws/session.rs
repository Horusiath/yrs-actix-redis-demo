use super::server::{Join, Leave, WsServer};
use super::{DocId, SessionId};
use crate::ws::persister::{LoadDocMessage, Persister};
use actix::{
    fut, Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, ContextFutureSpawner, Handler,
    Running, StreamHandler, WrapFuture,
};
use actix_http::ws::{CloseCode, CloseReason, Item, ProtocolError};
use actix_web_actors::ws;
use bytes::{Bytes, BytesMut};
use std::time::{Duration, Instant};
use yrs::encoding::read::{Cursor, Error};
use yrs::sync::MessageReader;
use yrs::updates::decoder::DecoderV1;
use yrs::updates::encoder::Encode;

pub const HEARTBEAT: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct WsSession {
    id: SessionId,
    topics: Vec<DocId>,
    server: Addr<WsServer>,
    persister: Addr<Persister>,
    hb: Instant,
    buf: Option<BytesMut>,
}

impl WsSession {
    pub fn new(
        id: SessionId,
        topics: Vec<DocId>,
        server: Addr<WsServer>,
        persister: Addr<Persister>,
    ) -> Self {
        WsSession {
            id,
            topics,
            server,
            persister,
            hb: Instant::now(),
            buf: None,
        }
    }

    fn hb(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT, |act, ctx| {
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                act.server.do_send(Leave {
                    session_id: act.id,
                    topics: None,
                });
                ctx.stop();
                return;
            }
            ctx.ping(b"");
        });
    }

    fn handle_protocol(&mut self, bytes: Bytes, ctx: &mut ws::WebsocketContext<Self>) {
        let mut decoder = DecoderV1::new(Cursor::new(&bytes));
        let reader = MessageReader::new(&mut decoder);
        for result in reader {
            match result {
                Ok(message) => self.handle_message(message, ctx),
                Err(err) => {
                    tracing::warn!("session `{}` protocol error: {}", self.id, err);
                    ctx.close(Some(CloseReason::from((
                        CloseCode::Protocol,
                        err.to_string(),
                    ))));
                }
            }
        }
    }

    fn handle_message(
        &mut self,
        message: yrs::sync::Message,
        ctx: &mut ws::WebsocketContext<Self>,
    ) {
        match message {
            yrs::sync::Message::Sync(yrs::sync::SyncMessage::SyncStep1(state_vector)) => {
                self.persister.send(LoadDocMessage {
                    topic_id: self.id,
                    version: Some(state_vector),
                    reply_to: Some(ctx.address().recipient()),
                });
            }
            yrs::sync::Message::Sync(yrs::sync::SyncMessage::SyncStep2(init_state)) => {
                todo!();
            }
            yrs::sync::Message::Sync(yrs::sync::SyncMessage::Update(update)) => {
                todo!();
            }
            yrs::sync::Message::Awareness(awareness_update) => {
                todo!()
            }
            yrs::sync::Message::AwarenessQuery => {
                todo!()
            }
            yrs::sync::Message::Auth(Some(deny_reason)) => {
                tracing::warn!("session `{}` action denied: {}", self.id, deny_reason);
                ctx.close(Some(CloseReason::from((CloseCode::Policy, deny_reason))));
            }
            yrs::sync::Message::Auth(None) => { /* do nothing */ }
            yrs::sync::Message::Custom(tag, bytes) => {
                todo!()
            }
        }
    }
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
        let join = Join {
            session_id: self.id.clone(),
            addr: ctx.address().recipient(),
            topics: self.topics.clone(),
        };
        self.server
            .send(join)
            .into_actor(self)
            .then(|res, act, ctx| {
                if let Err(err) = res {
                    tracing::warn!("session `{}` can't join: {}", act.id, err);
                    ctx.stop();
                }
                fut::ready(())
            })
            .wait(ctx);
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        self.server.do_send(Leave {
            session_id: self.id,
            topics: None,
        });
        Running::Stop
    }
}

impl Handler<Message> for WsSession {
    type Result = ();

    fn handle(&mut self, msg: Message, ctx: &mut Self::Context) {
        let bytes = msg.0.encode_v1();
        ctx.binary(bytes);
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession {
    fn handle(&mut self, item: Result<ws::Message, ProtocolError>, ctx: &mut Self::Context) {
        match item {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            Ok(ws::Message::Text(_)) => ctx.close(Some(CloseReason::from(CloseCode::Unsupported))),
            Ok(ws::Message::Binary(bytes)) => {
                self.handle_protocol(bytes, ctx);
            }
            Ok(ws::Message::Continuation(item)) => match item {
                Item::FirstText(_) => ctx.close(Some(CloseReason::from(CloseCode::Unsupported))),
                Item::FirstBinary(bytes) => self.buf = Some(bytes.into()),
                Item::Continue(bytes) => {
                    if let Some(ref mut buf) = self.buf {
                        buf.extend_from_slice(&bytes);
                    } else {
                        ctx.close(Some(CloseReason::from((
                            CloseCode::Protocol,
                            "continuation frame without initialization",
                        )))); // unexpected
                    }
                }
                Item::Last(bytes) => {
                    if let Some(mut buf) = self.buf.take() {
                        buf.extend_from_slice(&bytes);
                        self.handle_protocol(buf.freeze(), ctx);
                    } else {
                        ctx.close(Some(CloseReason::from((
                            CloseCode::Protocol,
                            "last frame without initialization",
                        )))); // unexpected
                    }
                }
            },
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            Ok(_) => { /* do nothing */ }
            Err(err) => {
                tracing::warn!("session `{}` websocket protocol error: {}", self.id, err);
                ctx.stop();
            }
        }
    }
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct Message(pub yrs::sync::Message);
