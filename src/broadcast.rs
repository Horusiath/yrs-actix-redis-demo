use crate::error::Error;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use opendal::Operator;
use redis::aio::ConnectionManager;
use redis::streams::{StreamId, StreamRangeReply, StreamReadReply};
use redis::{AsyncCommands, FromRedisValue};
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::ReusableBoxFuture;
use uuid::Uuid;
use yrs::Doc;

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
        operator: Operator,
        options: Options,
    ) -> Self {
        let stream_id = stream_id.into();
        let conn = Mutex::new(conn);
        let state = Arc::new(BroadcastState {
            redis: conn,
            s3: operator,
            stream_id: stream_id.clone(),
            subscribers: DashMap::new(),
        });
        // we can stop task by dropping the broadcast state
        tokio::spawn(Self::handle_messages(
            Arc::downgrade(&state),
            options.snapshot_threshold,
        ));
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

    /// Drops all messages in the stream before the given message ID.
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
        Sink: futures::Sink<Message, Error = Error> + Unpin + Send + Sync + 'static,
        Stream: futures::Stream<Item = Result<Bytes, Error>> + Unpin + Send + Sync + 'static,
    {
        let state = Arc::downgrade(&self.state);
        let listener = tokio::spawn(async move {
            if let Err(err) = Subscriber::handle(state, subscriber_id, stream).await {
                tracing::error!("Error handling subscriber messages: {}", err);
            }
        });

        let subscriber = Subscriber {
            id: subscriber_id,
            sink: sink as Weak<
                Mutex<dyn futures::Sink<Message, Error = Error> + Unpin + Send + Sync + 'static>,
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

    async fn handle_messages(state: Weak<BroadcastState>, snapshot_threshold: usize) {
        if let Err(err) = Self::handle_messages_internal(state, snapshot_threshold).await {
            tracing::error!("Error while handling messages: {}", err);
        }
    }

    async fn handle_messages_internal(
        state: Weak<BroadcastState>,
        snapshot_threshold: usize,
    ) -> Result<(), Error> {
        let mut last_id: MessageId = "0".into();
        let read_options = redis::streams::StreamReadOptions::default()
            .count(1000) // up to 1000 messages
            .block(1000); // block for 1 second
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
                let mut messages = Message::parse_redis_reply(reply);
                let mut dropped_subscribers = HashSet::new();
                for result in messages {
                    let msg = result?;
                    last_id = msg.id.clone();
                    for subscriber in state.subscribers.iter() {
                        if subscriber.key() != &msg.sender {
                            let msg_id = msg.id.clone();
                            match subscriber.send(msg.clone()).await {
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
            }
        }
        Ok(())
    }
}

struct BroadcastState {
    redis: Mutex<ConnectionManager>,
    s3: Operator,
    stream_id: Arc<str>,
    subscribers: DashMap<SubscriberId, Subscriber>,
}

impl BroadcastState {
    pub async fn load(&self) -> Result<Doc, Error> {
        todo!()
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
        let mut conn = self.redis.lock().await;
        let value = conn
            .xrange(self.stream_id.as_ref(), "-", last_msg_id)
            .await?;
        let value = StreamRangeReply::from_owned_redis_value(value)?;
        let msg_ids: Vec<_> = value
            .ids
            .into_iter()
            .map(|stream_id| stream_id.id)
            .collect();
        let count = msg_ids.len();
        conn.xdel(self.stream_id.as_ref(), &msg_ids).await?;
        Ok(count)
    }
}

impl Debug for BroadcastState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastState")
            .field("stream_id", &self.stream_id)
            .field("subscribers", &self.subscribers)
            .finish()
    }
}

struct SnapshotProgress {
    threshold: usize,
    counter: usize,
    last_msg_id: MessageId,
    pending: Option<ReusableBoxFuture<'static, Result<Doc, Error>>>,
}

struct Subscriber {
    id: SubscriberId,
    sink: Weak<Mutex<dyn futures::Sink<Message, Error = Error> + Unpin + Send + Sync + 'static>>,
    listener: JoinHandle<()>,
}

impl Subscriber {
    async fn send(&self, msg: Message) -> Result<(), Error> {
        if let Some(sink) = self.sink.upgrade() {
            let mut sink = sink.lock().await;
            sink.send(msg).await?;
            Ok(())
        } else {
            Err(Error::SubscriberDropped(self.id))
        }
    }

    async fn handle<Stream>(
        state: Weak<BroadcastState>,
        subscriber_id: SubscriberId,
        mut stream: Stream,
    ) -> Result<(), Error>
    where
        Stream: futures::Stream<Item = Result<Bytes, Error>> + Unpin + Send + Sync + 'static,
    {
        while let Some(msg) = stream.next().await {
            if let Some(state) = state.upgrade() {
                let msg = msg?;
                let mut conn = state.redis.lock().await;
                let sender: &[u8] = subscriber_id.as_bytes();
                let msg: &[u8] = msg.as_ref();
                conn.xadd(
                    state.stream_id.as_ref(),
                    "*",
                    &[("sender", &sender), ("data", &msg)],
                )
                .await?;
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
    pub data: Bytes,
}

impl Message {
    fn parse_redis_reply(reply: StreamReadReply) -> impl Iterator<Item = Result<Self, Error>> {
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
            data: Bytes::from(payload),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Options {
    pub snapshot_threshold: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            snapshot_threshold: 100,
        }
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};
    use opendal::services::MemoryConfig;
    use opendal::{Builder, Configurator, OperatorBuilder};
    use redis::Client;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
    use tokio::sync::Mutex;
    use tokio::time::timeout;

    use crate::broadcast::BroadcastGroup;
    use crate::error::Error;

    struct TestSink<T>(UnboundedSender<T>);

    impl<T> futures::Sink<T> for TestSink<T> {
        type Error = Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
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

    struct TestStream<T>(UnboundedReceiver<T>);

    impl<T> futures::Stream for TestStream<T> {
        type Item = Result<T, Error>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.0.poll_recv(cx) {
                Poll::Ready(Some(msg)) => Poll::Ready(Some(Ok(msg))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    fn test_subscriber<T>() -> (Arc<Mutex<TestSink<T>>>, TestStream<T>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Arc::new(Mutex::new(TestSink(tx))), TestStream(rx))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test() {
        let _ = env_logger::builder().is_test(true).try_init();

        let redis_client = Client::open("redis://localhost:6379").unwrap();
        let conn = redis_client.get_connection_manager().await.unwrap();
        let memory = MemoryConfig::default().into_builder().build().unwrap();
        let operator = OperatorBuilder::new(memory).finish();
        let group = BroadcastGroup::new("test-stream", conn, operator, Default::default());

        let client1 = uuid::Uuid::new_v4();
        let (client_tx1, server_rx1) = test_subscriber();
        let (server_tx1, mut client_rx1) = test_subscriber();
        group.subscribe(client1, Arc::downgrade(&server_tx1), server_rx1);

        let client2 = uuid::Uuid::new_v4();
        let (client_tx2, server_rx2) = test_subscriber();
        let (server_tx2, mut client_rx2) = test_subscriber();
        group.subscribe(client2, Arc::downgrade(&server_tx2), server_rx2);

        client_tx1
            .lock()
            .await
            .send("Hello, world!".into())
            .await
            .unwrap();
        let msg = timeout(Duration::from_millis(100), client_rx2.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        println!("Received message: {:?}", msg);
        assert_eq!(msg.data, Bytes::from("Hello, world!"));
        assert_eq!(msg.sender, client1);

        let res = timeout(Duration::from_millis(100), client_rx1.next()).await;
        assert!(res.is_err(), "client shouldn't receive its own message");

        group.cutoff(&msg.id).await.unwrap();
    }
}
