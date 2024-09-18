use actix_http::ws::{CloseCode, CloseReason};
use actix_web::rt;
use actix_ws::{AggregatedMessage, AggregatedMessageStream, Session};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};

use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use redis::aio::ConnectionManager;
use redis::streams::{StreamId, StreamReadReply};
use redis::AsyncCommands;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;
use yrs::sync::{Message as YMessage, MessageReader, SyncMessage};
use yrs::updates::decoder::DecoderV1;
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, Transact};

use crate::error::Error;
use crate::snapshot::Snapshotter;

pub type SubscriberId = Uuid;

pub type MessageId = Arc<str>;

#[derive(Debug, thiserror::Error)]
#[error("Failed to parse message id")]
pub struct MessageIdParseError;

#[derive(Debug, Clone)]
pub struct BroadcastGroup {
    state: Arc<BroadcastState>,
}

impl BroadcastGroup {
    pub fn new<S: Into<Arc<str>>>(
        stream_id: S,
        conn: ConnectionManager,
        snapshotter: Snapshotter,
    ) -> Self {
        let stream_id = stream_id.into();
        let conn = Mutex::new(conn);
        let state = Arc::new(BroadcastState {
            redis: conn,
            snapshotter,
            stream_id: stream_id.clone(),
            subscribers: DashMap::new(),
            total_updates_sent: Default::default(),
            total_updates_received: Default::default(),
        });
        // we can stop task by dropping the broadcast state
        rt::spawn(Self::handle_messages(Arc::downgrade(&state)));
        Self { state }
    }

    /// Accepts a new subscriber.
    pub fn subscribe(
        &self,
        subscriber_id: SubscriberId,
        session: Session,
        stream: AggregatedMessageStream,
    ) {
        let session = Arc::new(Mutex::new(session));
        let listener = {
            let sink = Arc::downgrade(&session);
            let state = Arc::downgrade(&self.state);
            rt::spawn(async move {
                if let Err(err) = Subscriber::handle(state, subscriber_id, sink, stream).await {
                    tracing::error!("Error handling subscriber messages: {}", err);
                }
            })
        };

        let subscriber = Subscriber {
            id: subscriber_id,
            session,
            listener,
        };
        tracing::info!(
            "Topic `{}` accepted subscriber `{}`",
            self.state.stream_id,
            subscriber_id
        );
        self.state.subscribers.insert(subscriber_id, subscriber);
    }

    async fn handle_messages(state: Weak<BroadcastState>) {
        if let Err(err) = Self::handle_redis_updates(state).await {
            tracing::error!("Error while handling messages: {}", err);
        }
    }

    /// Handles updates from Redis stream.
    async fn handle_redis_updates(state: Weak<BroadcastState>) -> Result<(), Error> {
        let mut last_id: MessageId = "0".into();
        let read_options = redis::streams::StreamReadOptions::default().count(100); // block for 1 second
        let mut conn = if let Some(state) = state.upgrade() {
            state.redis.lock().await.clone()
        } else {
            return Ok(());
        };
        loop {
            let state = match state.upgrade() {
                Some(state) => state,
                None => break,
            };
            let reply: StreamReadReply = conn
                .xread_options(
                    &[state.stream_id.as_ref()],
                    &[last_id.as_ref()],
                    &read_options,
                )
                .await?;

            if !reply.keys.is_empty() {
                let messages = Message::parse_redis_reply(reply);
                let mut dropped_subscribers = HashMap::new();
                for result in messages {
                    let msg = result?;
                    tracing::trace!("Received Redis message: {}", msg.id);
                    last_id = msg.id.clone();
                    state.snapshotter.notify_one();
                    state
                        .total_updates_received
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    for subscriber in state.subscribers.iter() {
                        if subscriber.key() != &msg.sender {
                            let msg_id = msg.id.clone();
                            match subscriber.send_update(msg.update.to_vec()).await {
                                Ok(_) => {
                                    tracing::trace!(
                                        "Topic `{}` sent message `{}` to subscriber `{}`",
                                        state.stream_id,
                                        msg_id,
                                        subscriber.key(),
                                    );
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        "Failed to sent message to subscriber `{}`: {}",
                                        subscriber.id,
                                        err
                                    );
                                    dropped_subscribers.insert(subscriber.id, err);
                                }
                            }
                        }
                    }
                }

                for (id, err) in dropped_subscribers {
                    tracing::trace!("Topic `{}` dropping subscriber `{}`", state.stream_id, id);
                    if let Some((_, subscriber)) = state.subscribers.remove(&id) {
                        if !matches!(err, Error::ConnectionClosed) {
                            // try to gracefully close the session
                            let session = subscriber.session;
                            if let Some(session) = Arc::into_inner(session) {
                                let session = session.into_inner();
                                let _ = session
                                    .close(Some(CloseReason::from((
                                        CloseCode::Error,
                                        err.to_string(),
                                    ))))
                                    .await;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn graceful_shutdown(self) -> Result<(), Error> {
        self.snapshotter.snapshot(None).await?;
        Ok(())
    }
}

pub struct BroadcastState {
    redis: Mutex<ConnectionManager>,
    snapshotter: Snapshotter,
    stream_id: Arc<str>,
    subscribers: DashMap<SubscriberId, Subscriber>,
    total_updates_sent: AtomicU64,
    total_updates_received: AtomicU64,
}

impl BroadcastState {
    pub fn updates_sent(&self) -> u64 {
        self.total_updates_sent
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn updates_received(&self) -> u64 {
        self.total_updates_received
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn load(&self) -> Result<Doc, Error> {
        let loaded = self.snapshotter.load(None).await?;
        Ok(loaded.doc)
    }

    pub async fn cutoff(&self, last_msg_id: MessageId) -> Result<usize, Error> {
        self.snapshotter.cutoff(last_msg_id).await
    }

    async fn handle_message(
        &self,
        bytes: Bytes,
        subscriber_id: SubscriberId,
        session: &Mutex<Session>,
    ) -> Result<(), Error> {
        let mut conn = self.redis.lock().await;
        let sender: &[u8] = subscriber_id.as_bytes();
        let mut decoder = DecoderV1::from(bytes.as_ref());
        let reader = MessageReader::new(&mut decoder);
        for res in reader {
            let msg = res?;
            tracing::trace!(
                "Handling the subscriber's `{}` message: {:?}",
                subscriber_id,
                msg
            );
            match msg {
                YMessage::Sync(SyncMessage::SyncStep1(sv)) => {
                    //TODO: add local state vector to check if remote has missing updates
                    // without any need to load it
                    let loaded = self.snapshotter.load(None).await?;
                    {
                        let txn = loaded.doc.transact();
                        let doc_state = txn.encode_state_as_update_v1(&sv);
                        let reply = YMessage::Sync(SyncMessage::SyncStep2(doc_state)).encode_v1();
                        let mut session = session.lock().await;
                        session.binary(reply).await?;
                    }
                    tracing::trace!("Send update back to subscriber `{}`", subscriber_id);

                    if loaded.last_message_id.is_some() {
                        //TODO: since we already have a newer doc state, we can try to snapshot it
                    }
                }
                YMessage::Sync(SyncMessage::SyncStep2(update)) => {
                    let msg_id: String = conn
                        .xadd(
                            self.stream_id.as_ref(),
                            "*",
                            &[("sender", sender), ("data", update.as_ref())],
                        )
                        .await?;
                    self.total_updates_sent
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tracing::trace!("send update to redis: {}", msg_id);
                }
                YMessage::Sync(SyncMessage::Update(update)) => {
                    let msg_id: String = conn
                        .xadd(
                            self.stream_id.as_ref(),
                            "*",
                            &[("sender", sender), ("data", update.as_ref())],
                        )
                        .await?;
                    self.total_updates_sent
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tracing::trace!("send update to redis: {}", msg_id);
                }
                YMessage::Auth(_deny_reason) => {}
                YMessage::AwarenessQuery => {}
                YMessage::Awareness(_awareness_update) => {}
                YMessage::Custom(_tag, _data) => {}
            }
        }

        Ok(())
    }
}

impl Deref for BroadcastGroup {
    type Target = BroadcastState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl Debug for BroadcastState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastState")
            .field("stream_id", &self.stream_id)
            .field("subscribers", &self.subscribers)
            .field("snapshotter", &self.snapshotter)
            .finish()
    }
}

struct Subscriber {
    id: SubscriberId,
    session: Arc<Mutex<Session>>,
    #[allow(dead_code)]
    listener: JoinHandle<()>,
}

impl Subscriber {
    async fn send_update(&self, update: Vec<u8>) -> Result<(), Error> {
        let msg = YMessage::Sync(SyncMessage::Update(update)).encode_v1();
        let mut session = self.session.lock().await;
        session.binary(msg).await?;
        Ok(())
    }

    async fn handle(
        state: Weak<BroadcastState>,
        subscriber_id: SubscriberId,
        sink: Weak<Mutex<Session>>,
        mut stream: AggregatedMessageStream,
    ) -> Result<(), Error> {
        while let Some(res) = stream.next().await {
            if let Some(state) = state.upgrade() {
                match res? {
                    AggregatedMessage::Text(_) => { /* ignore */ }
                    AggregatedMessage::Binary(bytes) => {
                        if let Some(sink) = sink.upgrade() {
                            state.handle_message(bytes, subscriber_id, &sink).await?;
                        }
                    }
                    AggregatedMessage::Ping(ping) => {
                        if let Some(sink) = sink.upgrade() {
                            sink.lock().await.pong(&ping).await?;
                        }
                    }
                    AggregatedMessage::Pong(pong) => {
                        if let Some(sink) = sink.upgrade() {
                            sink.lock().await.ping(&pong).await?;
                        }
                    }
                    AggregatedMessage::Close(reason) => {
                        tracing::trace!(
                            "Subscriber `{}` closed the connection: {:?}",
                            subscriber_id,
                            reason
                        );
                        break;
                    }
                }
            } else {
                break;
            }
        }
        Ok(())
    }
}

impl Debug for Subscriber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscriber").field("id", &self.id).finish()
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: MessageId,
    pub sender: Uuid,
    pub update: Bytes,
}

impl Message {
    pub fn parse_redis_reply(reply: StreamReadReply) -> impl Iterator<Item = Result<Self, Error>> {
        reply
            .keys
            .into_iter()
            .flat_map(|key| key.ids.into_iter().map(move |data| Message::try_from(data)))
    }
}

impl Eq for Message {}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl TryFrom<StreamId> for Message {
    type Error = Error;

    fn try_from(value: StreamId) -> Result<Self, Self::Error> {
        let sender = value
            .get::<[u8; 16]>("sender")
            .ok_or_else(|| Error::MissingField { field: "sender" })?;
        let payload = value
            .get::<Bytes>("data")
            .ok_or_else(|| Error::MissingField { field: "data" })?;
        Ok(Self {
            id: value.id.into(),
            sender: Uuid::from_bytes(sender),
            update: Bytes::from(payload),
        })
    }
}

#[cfg(test)]
mod test {}
