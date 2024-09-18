use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};

use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use redis::aio::ConnectionManager;
use redis::streams::{StreamId, StreamReadReply};
use redis::{AsyncCommands, FromRedisValue};
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct MessageId {
    pub timestamp: u64,
    pub seq_no: u64,
}

impl MessageId {
    pub fn new(timestamp: u64, seq_no: u64) -> Self {
        Self { timestamp, seq_no }
    }

    pub fn try_parse(s: &str) -> Option<Self> {
        let mut parts = s.split('-');
        let timestamp: u64 = parts.next()?.parse().ok()?;
        let seq_no: u64 = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self { timestamp, seq_no })
    }
}

impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.timestamp, self.seq_no)
    }
}

impl FromStr for MessageId {
    type Err = MessageIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_parse(s).ok_or(MessageIdParseError)
    }
}

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
            updates_sent: Default::default(),
            updates_received: Default::default(),
        });
        // we can stop task by dropping the broadcast state
        tokio::spawn(Self::handle_messages(Arc::downgrade(&state)));
        Self { state }
    }

    /// Accepts a new subscriber.
    pub fn subscribe<Sink, Stream>(
        &self,
        subscriber_id: SubscriberId,
        sink: Weak<Mutex<Sink>>,
        stream: Stream,
    ) where
        Sink: futures::Sink<Bytes, Error = Error> + Unpin + Send + Sync + 'static,
        Stream: futures::Stream<Item = Result<Bytes, Error>> + Unpin + Send + Sync + 'static,
    {
        let listener = {
            let sink = sink.clone();
            let state = Arc::downgrade(&self.state);
            tokio::spawn(async move {
                if let Err(err) = Subscriber::handle(state, subscriber_id, sink, stream).await {
                    tracing::error!("Error handling subscriber messages: {}", err);
                }
            })
        };

        let subscriber = Subscriber {
            id: subscriber_id,
            sink: sink as Weak<
                Mutex<dyn futures::Sink<Bytes, Error = Error> + Unpin + Send + Sync + 'static>,
            >,
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
        let mut last_id: MessageId = MessageId::default();
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
            let last_id_str = last_id.to_string();
            let reply: StreamReadReply = conn
                .xread_options(
                    &[state.stream_id.as_ref()],
                    &[last_id_str.as_str()],
                    &read_options,
                )
                .await?;

            if !reply.keys.is_empty() {
                let messages = Message::parse_redis_reply(reply);
                let mut msg_count = 0;
                let mut dropped_subscribers = HashSet::new();
                for result in messages {
                    let msg = result?;
                    tracing::trace!("Received Redis message: {}", msg.id);
                    last_id = msg.id.clone();
                    msg_count += 1;
                    state
                        .updates_received
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
                                Err(Error::SubscriberDropped(id)) => {
                                    dropped_subscribers.insert(id);
                                }
                                Err(err) => return Err(err),
                            }
                        }
                    }
                }

                for id in dropped_subscribers {
                    tracing::trace!("Topic `{}` dropping subscriber `{}`", state.stream_id, id);
                    state.subscribers.remove(&id);
                }

                state.snapshotter.notify(msg_count, last_id.clone());
            }
        }
        Ok(())
    }
}

pub struct BroadcastState {
    redis: Mutex<ConnectionManager>,
    snapshotter: Snapshotter,
    stream_id: Arc<str>,
    subscribers: DashMap<SubscriberId, Subscriber>,
    updates_sent: AtomicU64,
    updates_received: AtomicU64,
}

impl BroadcastState {
    pub fn updates_sent(&self) -> u64 {
        self.updates_sent.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn updates_received(&self) -> u64 {
        self.updates_received
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn load(&self) -> Result<Doc, Error> {
        let loaded = self.snapshotter.load(None).await?;
        Ok(loaded.doc)
    }

    pub async fn publish(&self, sender: &SubscriberId, msg: Bytes) -> Result<String, Error> {
        tracing::trace!("Publishing message as `{}`: {:?}", sender, msg);
        let mut conn = self.redis.lock().await;
        let sender: &[u8] = sender.as_bytes();
        let msg: &[u8] = msg.as_ref();
        let message_id = conn
            .xadd(
                self.stream_id.as_ref(),
                "*",
                &[("sender", sender), ("msg", msg)],
            )
            .await?;
        Ok(String::from_owned_redis_value(message_id)?)
    }

    pub async fn cutoff(&self, last_msg_id: MessageId) -> Result<usize, Error> {
        self.snapshotter.cutoff(last_msg_id).await
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
    sink: Weak<Mutex<dyn futures::Sink<Bytes, Error = Error> + Unpin + Send + Sync + 'static>>,
    #[allow(dead_code)]
    listener: JoinHandle<()>,
}

impl Subscriber {
    async fn send_update(&self, update: Vec<u8>) -> Result<(), Error> {
        if let Some(sink) = self.sink.upgrade() {
            let msg = YMessage::Sync(SyncMessage::Update(update));
            let mut sink = sink.lock().await;
            sink.send(msg.encode_v1().into()).await?;
            Ok(())
        } else {
            Err(Error::SubscriberDropped(self.id))
        }
    }

    async fn handle<Sink, Stream>(
        state: Weak<BroadcastState>,
        subscriber_id: SubscriberId,
        sink: Weak<Mutex<Sink>>,
        mut stream: Stream,
    ) -> Result<(), Error>
    where
        Sink: futures::Sink<Bytes, Error = Error> + Unpin + Send + Sync + 'static,
        Stream: futures::Stream<Item = Result<Bytes, Error>> + Unpin + Send + Sync + 'static,
    {
        while let Some(msg) = stream.next().await {
            if let Some(state) = state.upgrade() {
                let bytes = msg?;
                let mut conn = state.redis.lock().await;
                let sender: &[u8] = subscriber_id.as_bytes();
                let mut decoder = DecoderV1::from(bytes.as_ref());
                let mut reader = MessageReader::new(&mut decoder);
                for res in reader {
                    let msg = res?;
                    tracing::trace!(
                        "Handling the subscriber's `{}` message: {:?}",
                        subscriber_id,
                        msg
                    );
                    match msg {
                        YMessage::Sync(SyncMessage::SyncStep1(sv)) => {
                            let loaded = match sink.upgrade() {
                                None => {
                                    tracing::trace!(
                                        "Subscriber `{}` has been dropped",
                                        subscriber_id
                                    );
                                    return Ok(());
                                }
                                Some(sink) => {
                                    //TODO: add local state vector to check if remote has missing updates
                                    // without any need to load it
                                    let loaded = state.snapshotter.load(None).await?;
                                    {
                                        let txn = loaded.doc.transact();
                                        let doc_state = txn.encode_state_as_update_v1(&sv);
                                        let reply: Bytes =
                                            YMessage::Sync(SyncMessage::SyncStep2(doc_state))
                                                .encode_v1()
                                                .into();
                                        let mut sink = sink.lock().await;
                                        sink.send(reply).await?;
                                    }
                                    tracing::trace!(
                                        "Send update back to subscriber `{}`",
                                        subscriber_id
                                    );
                                    loaded
                                }
                            };

                            if loaded.last_message_id.is_some() {
                                //TODO: since we already have a newer doc state, we can try to snapshot it
                            }
                        }
                        YMessage::Sync(SyncMessage::SyncStep2(update)) => {
                            let msg_id: String = conn
                                .xadd(
                                    state.stream_id.as_ref(),
                                    "*",
                                    &[("sender", sender), ("data", update.as_ref())],
                                )
                                .await?;
                            state
                                .updates_sent
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            tracing::trace!("send update to redis: {}", msg_id);
                        }
                        YMessage::Sync(SyncMessage::Update(update)) => {
                            let msg_id: String = conn
                                .xadd(
                                    state.stream_id.as_ref(),
                                    "*",
                                    &[("sender", sender), ("data", update.as_ref())],
                                )
                                .await?;
                            state
                                .updates_sent
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            tracing::trace!("send update to redis: {}", msg_id);
                        }
                        YMessage::Auth(deny_reason) => {}
                        YMessage::AwarenessQuery => {}
                        YMessage::Awareness(awareness_update) => {}
                        YMessage::Custom(tag, data) => {}
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
        let id = MessageId::from_str(&value.id).map_err(|_| Error::MissingField { field: "id" })?;
        Ok(Self {
            id,
            sender: Uuid::from_bytes(sender),
            update: Bytes::from(payload),
        })
    }
}

#[cfg(test)]
mod test {}
