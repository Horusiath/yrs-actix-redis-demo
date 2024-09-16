use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
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
pub type MessageId = Arc<str>;

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
        });
        // we can stop task by dropping the broadcast state
        tokio::spawn(Self::handle_messages(Arc::downgrade(&state)));
        Self { state }
    }

    #[inline]
    pub async fn publish(&self, sender: &SubscriberId, msg: Bytes) -> Result<String, Error> {
        self.state.publish(sender, msg).await
    }

    /// Loads document state.
    #[inline]
    pub async fn load(&self) -> Result<Doc, Error> {
        self.state.load().await
    }

    /// Drops all messages in the stream up to the given message ID.
    #[inline]
    pub async fn cutoff(&self, msg_id: &str) -> Result<usize, Error> {
        self.state.cutoff(msg_id).await
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
                let mut msg_count = 0;
                let mut dropped_subscribers = HashSet::new();
                for result in messages {
                    let msg = result?;
                    last_id = msg.id.clone();
                    msg_count += 1;
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

struct BroadcastState {
    redis: Mutex<ConnectionManager>,
    snapshotter: Snapshotter,
    stream_id: Arc<str>,
    subscribers: DashMap<SubscriberId, Subscriber>,
}

impl BroadcastState {
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

    pub async fn cutoff(&self, last_msg_id: &str) -> Result<usize, Error> {
        self.snapshotter.cutoff(last_msg_id).await
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
                                        "Send document back to subscriber `{}`",
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
                            conn.xadd(
                                state.stream_id.as_ref(),
                                "*",
                                &[("sender", sender), ("data", update.as_ref())],
                            )
                            .await?;
                        }
                        YMessage::Sync(SyncMessage::Update(update)) => {
                            conn.xadd(
                                state.stream_id.as_ref(),
                                "*",
                                &[("sender", sender), ("data", update.as_ref())],
                            )
                            .await?;
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
        let stream_msg_id = value.id;
        Ok(Self {
            id: stream_msg_id.into(),
            sender: Uuid::from_bytes(sender),
            update: Bytes::from(payload),
        })
    }
}

#[cfg(test)]
mod test {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};
    use opendal::services::MemoryConfig;
    use opendal::{Builder, Configurator, OperatorBuilder};
    use redis::{Client, Commands};
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tokio::sync::Mutex;
    use uuid::Uuid;
    use yrs::sync::{
        Awareness, DefaultProtocol, Message as YMessage, MessageReader, Protocol, SyncMessage,
    };
    use yrs::updates::decoder::{Decode, DecoderV1};
    use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
    use yrs::{Doc, GetString, Origin, Text, TextRef, Transact, Update};

    use crate::broadcast::BroadcastGroup;
    use crate::error::Error;
    use crate::snapshot::Snapshotter;

    struct TestPeer {
        awareness: Arc<Awareness>,
        text: TextRef,
        subscriber_id: Uuid,
        inbound_tx: Arc<Mutex<TestSink>>,
        outbound_rx: Option<TestStream>,
        outbound_tx: UnboundedSender<Bytes>,
    }

    impl TestPeer {
        pub fn new() -> Self {
            let subscriber_id = Uuid::new_v4();
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let (inbound_tx, mut inbound_rx) = unbounded_channel::<Bytes>();
            let (outbound_tx, outbound_rx) = unbounded_channel::<Bytes>();
            {
                let sink = outbound_tx.clone();
                let origin: Origin = doc.client_id().into();
                doc.observe_update_v1_with("test-send", move |txn, e| {
                    if txn.origin() != Some(&origin) {
                        return; // we only send our own updates
                    }
                    tracing::trace!(
                        "TestPeer `{}` sending update: {:#?}",
                        subscriber_id,
                        Update::decode_v1(&e.update).unwrap()
                    );
                    let msg = YMessage::Sync(SyncMessage::Update(e.update.clone()));
                    let bytes = Bytes::from(msg.encode_v1());
                    sink.send(bytes).unwrap();
                })
                .unwrap();
            }
            let awareness = Arc::new(Awareness::new(doc));
            {
                let awareness = Arc::downgrade(&awareness);
                tokio::spawn(async move {
                    while let Some(msg) = inbound_rx.recv().await {
                        match awareness.upgrade() {
                            None => return,
                            Some(awareness) => {
                                let mut decoder = DecoderV1::from(msg.as_ref());
                                let mut reader = MessageReader::new(&mut decoder);
                                for res in reader {
                                    let msg = res.unwrap();
                                    tracing::trace!(
                                        "TestPeer `{}` received: {:?}",
                                        subscriber_id,
                                        msg
                                    );
                                    DefaultProtocol.handle_message(&awareness, msg).unwrap();
                                }
                            }
                        }
                    }
                });
            }
            Self {
                subscriber_id,
                awareness,
                text,
                inbound_tx: Arc::new(Mutex::new(TestSink(inbound_tx))),
                outbound_tx,
                outbound_rx: Some(TestStream(outbound_rx)),
            }
        }

        pub fn connect(&mut self, broadcast_group: &BroadcastGroup) {
            let sink = Arc::downgrade(&self.inbound_tx);
            let stream = self.outbound_rx.take().unwrap();
            broadcast_group.subscribe(self.subscriber_id, sink, stream);

            // initializing the connection
            let bytes: Bytes = {
                let mut encoder = EncoderV1::new();
                DefaultProtocol
                    .start(&self.awareness, &mut encoder)
                    .unwrap();
                encoder.to_vec().into()
            };
            self.outbound_tx.send(bytes).unwrap();
        }
    }

    struct TestSink(UnboundedSender<Bytes>);

    impl futures::Sink<Bytes> for TestSink {
        type Error = Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
            self.0
                .send(item)
                .map_err(|_| Error::SubscriberDropped(Default::default()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    struct TestStream(UnboundedReceiver<Bytes>);

    impl futures::Stream for TestStream {
        type Item = Result<Bytes, Error>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.0.poll_recv(cx) {
                Poll::Ready(Some(msg)) => Poll::Ready(Some(Ok(msg))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    fn test_subscriber<T>() -> (Arc<Mutex<TestSink>>, TestStream) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Arc::new(Mutex::new(TestSink(tx))), TestStream(rx))
    }

    #[tokio::test]
    async fn broadcast_except_self() {
        let _ = env_logger::builder().is_test(true).try_init();

        let stream_id: Arc<str> = format!("test-stream-{}", Uuid::new_v4()).into();
        let redis_client = Client::open("redis://localhost:6379").unwrap();
        let conn = redis_client.get_connection_manager().await.unwrap();
        let memory = MemoryConfig::default().into_builder().build().unwrap();
        let operator = OperatorBuilder::new(memory).finish();
        let snapshotter = Snapshotter::new(operator, conn.clone(), stream_id.clone(), 100);
        let group = BroadcastGroup::new(stream_id, conn, snapshotter);

        let mut p1 = TestPeer::new();
        let mut p2 = TestPeer::new();
        p1.connect(&group);
        p2.connect(&group);

        {
            let doc = p1.awareness.doc();
            let mut tx = doc.transact_mut_with(doc.client_id());
            p1.text.insert(&mut tx, 0, "Hello, World!");
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        {
            let doc = p2.awareness.doc();
            let tx = doc.transact();
            let str = p2.text.get_string(&tx);
            assert_eq!(str, "Hello, World!");
        }
    }
}
