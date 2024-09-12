use crate::error::Error;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use opendal::Operator;
use redis::aio::ConnectionManager;
use redis::streams::{StreamId, StreamRangeReply, StreamReadReply};
use redis::{AsyncCommands, FromRedisValue};
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub type SubscriberId = Uuid;

pub struct BroadcastGroup {
    redis_conn: Arc<Mutex<ConnectionManager>>,
    s3: Operator,
    stream_id: Arc<str>,
    msg_handler: JoinHandle<()>,
    subscribers: Arc<DashMap<SubscriberId, Subscriber>>,
}

impl BroadcastGroup {
    pub fn new<S: Into<Arc<str>>>(
        stream_id: S,
        conn: ConnectionManager,
        operator: Operator,
    ) -> Self {
        let stream_id = stream_id.into();
        let subscribers = Arc::new(DashMap::new());
        let msg_handler = tokio::spawn(Self::handle_messages(
            conn.clone(),
            operator.clone(),
            stream_id.clone(),
            Arc::downgrade(&subscribers),
        ));
        let conn = Arc::new(Mutex::new(conn));
        Self {
            s3: operator,
            redis_conn: conn,
            stream_id,
            msg_handler,
            subscribers,
        }
    }

    /// Drops all messages in the stream before the given message ID.
    pub async fn cutoff(&self, msg_id: &str) -> Result<(), Error> {
        let mut conn = self.redis_conn.lock().await;
        let value = conn.xrange(self.stream_id.as_ref(), "-", msg_id).await?;
        let value = StreamRangeReply::from_owned_redis_value(value)?;
        let msg_ids: Vec<_> = value
            .ids
            .into_iter()
            .map(|stream_id| stream_id.id)
            .collect();
        conn.xdel(self.stream_id.as_ref(), &msg_ids).await?;
        Ok(())
    }

    pub async fn publish(&self, sender: &SubscriberId, msg: Bytes) -> Result<String, Error> {
        tracing::trace!("Publishing message as `{}`: {:?}", sender, msg);
        let mut conn = self.redis_conn.lock().await;
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

    pub fn subscribe<Sink, Stream>(
        &self,
        subscriber_id: SubscriberId,
        sink: Weak<Mutex<Sink>>,
        stream: Stream,
    ) where
        Sink: futures::Sink<Message, Error = Error> + Unpin + Send + Sync + 'static,
        Stream: futures::Stream<Item = Result<Bytes, Error>> + Unpin + Send + Sync + 'static,
    {
        let conn = self.redis_conn.clone();
        let stream_id = self.stream_id.clone();
        let sender = subscriber_id.clone();
        let listener = tokio::spawn(async move {
            if let Err(err) = Subscriber::handle(stream_id, sender, conn, stream).await {
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
            self.stream_id,
            subscriber_id
        );
        self.subscribers.insert(subscriber_id, subscriber);
    }

    async fn handle_messages(
        conn: ConnectionManager,
        operator: Operator,
        stream_id: Arc<str>,
        subscribers: Weak<DashMap<SubscriberId, Subscriber>>,
    ) {
        if let Err(err) = Self::handle_messages_err(conn, operator, stream_id, subscribers).await {
            tracing::error!("Error handling messages: {}", err);
        }
    }

    async fn handle_messages_err(
        mut conn: ConnectionManager,
        operator: Operator,
        stream_id: Arc<str>,
        subscribers: Weak<DashMap<Uuid, Subscriber>>,
    ) -> Result<(), Error> {
        let mut last_id = "0".to_string();
        let read_options = redis::streams::StreamReadOptions::default().count(100);
        loop {
            let subscribers = match subscribers.upgrade() {
                Some(subscribers) => subscribers,
                None => break,
            };
            let reply: StreamReadReply = conn
                .xread_options(&[stream_id.as_ref()], &[&last_id], &read_options)
                .await?;

            if !reply.keys.is_empty() {
                for stream_key in reply.keys {
                    last_id = stream_key.key;
                    for data in stream_key.ids {
                        let msg = Message::try_from(data)?;
                        let mut dropped_subscribers = Vec::new();
                        for subscriber in subscribers.iter() {
                            if subscriber.key() != &msg.sender {
                                let msg_id = msg.stream_msg_id.clone();
                                match subscriber.send(msg.clone()).await {
                                    Ok(_) => {
                                        tracing::trace!(
                                            "Topic `{}` sent message `{}` to subscriber `{}`",
                                            stream_id,
                                            msg_id,
                                            subscriber.key(),
                                        );
                                    }
                                    Err(Error::SubscriberDropped(id)) => {
                                        dropped_subscribers.push(id);
                                    }
                                    Err(err) => return Err(err),
                                }
                            }
                        }
                        for id in dropped_subscribers {
                            tracing::trace!("Topic `{}` dropping subscriber `{}`", stream_id, id);
                            subscribers.remove(&id);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for BroadcastGroup {
    fn drop(&mut self) {
        self.msg_handler.abort();
    }
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
        stream_id: Arc<str>,
        subscriber_id: SubscriberId,
        conn: Arc<Mutex<ConnectionManager>>,
        mut stream: Stream,
    ) -> Result<(), Error>
    where
        Stream: futures::Stream<Item = Result<Bytes, Error>> + Unpin + Send + Sync + 'static,
    {
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            let mut conn = conn.lock().await;
            let sender: &[u8] = subscriber_id.as_bytes();
            let msg: &[u8] = msg.as_ref();
            conn.xadd(
                stream_id.as_ref(),
                "*",
                &[("sender", &sender), ("data", &msg)],
            )
            .await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub stream_msg_id: Arc<str>,
    pub sender: Uuid,
    pub data: Bytes,
}

impl Eq for Message {}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.stream_msg_id == other.stream_msg_id
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
            stream_msg_id: stream_msg_id.into(),
            sender: Uuid::from_bytes(sender),
            data: Bytes::from(payload),
        })
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
        let group = BroadcastGroup::new("test-stream", conn, operator);

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

        group.cutoff(&msg.stream_msg_id).await.unwrap()
    }
}
