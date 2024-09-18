use crate::error::Error;
use bytes::Bytes;
use flate2::bufread::GzDecoder;
use futures::{SinkExt, Stream, StreamExt};
use serde::Deserialize;
use smallvec::{smallvec, SmallVec};
use std::fs::File;
use std::io::{BufReader, Read};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::connect_async;
use uuid::Uuid;
use yrs::sync::protocol::AsyncProtocol;
use yrs::sync::{Awareness, DefaultProtocol, Message, MessageReader, SyncMessage};
use yrs::updates::decoder::DecoderV1;
use yrs::updates::encoder::Encode;
use yrs::{merge_updates_v1, Doc, Text, TextRef, Transact, TransactionMut, UpdateEvent};

pub struct TestPeer {
    pub awareness: Arc<Awareness>,
    pub text: TextRef,
    pub subscriber_id: Uuid,
}

impl TestPeer {
    pub fn new() -> Self {
        let subscriber_id = Uuid::new_v4();
        let doc = Doc::new();
        let text = doc.get_or_insert_text("text");
        let awareness = Arc::new(Awareness::new(doc));
        Self {
            subscriber_id,
            awareness,
            text,
        }
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), Error> {
        let (sender, rx) = unbounded_channel();
        let (ws_stream, _resp) = connect_async(url).await?;
        let (sink, stream) = ws_stream.split();
        {
            let sender = sender.clone();
            self.awareness
                .doc()
                .observe_update_v1_with(format!("send-{}", url), move |_txn, e| {
                    let msg = Message::Sync(SyncMessage::Update(e.update.clone()));
                    sender.send(msg).unwrap();
                })
                .unwrap();
        }

        tokio::spawn(async move {
            if let Err(err) = Self::send_messages(rx, sink).await {
                tracing::error!("TestPeer send error: {:?}", err);
            }
        });
        let awareness = self.awareness.clone();
        tokio::spawn(async move {
            if let Err(err) = Self::receive_messages(stream, sender, awareness).await {
                tracing::error!("TestPeer receive error: {:?}", err);
            }
        });

        Ok(())

        /*
        let sink = self.inbound_tx.clone();
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
        self.outbound_tx.send(bytes).unwrap();*/
    }

    async fn receive_messages<S, E>(
        mut stream: S,
        sender: UnboundedSender<Message>,
        awareness: Arc<Awareness>,
    ) -> Result<(), Error>
    where
        S: Stream<Item = Result<tokio_tungstenite::tungstenite::Message, E>> + Unpin,
        Error: From<E>,
    {
        if let Some(msg) = stream.next().await {
            let msg = msg?;
            match msg {
                tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
                    let mut decoder = DecoderV1::from(bytes.as_ref());
                    let reader = MessageReader::new(&mut decoder);
                    for res in reader {
                        let msg = res?;
                        tracing::trace!("TestPeer received: {:?}", msg);
                        if let Some(reply) = DefaultProtocol.handle_message(&awareness, msg).await?
                        {
                            sender.send(reply).map_err(|_| Error::ConnectionClosed)?;
                        };
                    }
                }
                tokio_tungstenite::tungstenite::Message::Close(reason) => {
                    let reason = reason.map(|r| r.reason.to_string()).unwrap_or_default();
                    tracing::info!("TestPeer connection closed: {}", reason);
                    return Ok(());
                }
                tokio_tungstenite::tungstenite::Message::Text(_) => {}
                tokio_tungstenite::tungstenite::Message::Ping(_) => {}
                tokio_tungstenite::tungstenite::Message::Pong(_) => {}
                tokio_tungstenite::tungstenite::Message::Frame(_) => {}
            }
        }
        Ok(())
    }

    async fn send_messages<S, E>(
        mut rx: UnboundedReceiver<Message>,
        mut sink: S,
    ) -> Result<(), Error>
    where
        S: futures::Sink<tokio_tungstenite::tungstenite::Message, Error = E> + Unpin,
        E: Into<Error>,
    {
        while let Some(msg) = rx.recv().await {
            if let Message::Sync(SyncMessage::Update(update)) = msg {
                // try to eagerly fetch more updates if possible
                let update = Self::eager_prefetch(&mut rx, update)?;
                sink.send(tokio_tungstenite::tungstenite::Message::Binary(update))
                    .await
                    .map_err(|_| Error::ConnectionClosed)?;
            } else {
                let bytes = msg.encode_v1();
                sink.send(tokio_tungstenite::tungstenite::Message::Binary(bytes))
                    .await
                    .map_err(|_| Error::ConnectionClosed)?;
            }
        }
        Ok(())
    }

    fn eager_prefetch(rx: &mut UnboundedReceiver<Message>, buf: Vec<u8>) -> Result<Vec<u8>, Error> {
        let mut updates: SmallVec<[Vec<u8>; 1]> = smallvec![buf];
        let mut other = None;
        // try to eagerly fetch more updates if they are already in the queue
        while let Ok(msg) = rx.try_recv() {
            match msg {
                Message::Sync(SyncMessage::Update(update)) => {
                    // we stack updates together until we reach a non-update message
                    updates.push(update);
                }
                msg => {
                    // other type of message, we cannot compact updates anymore,
                    // so we just prepend the update message and then add new one and send them
                    // all together
                    other = Some(msg.encode_v1());
                    break;
                }
            }
        }
        let compacted = if updates.len() == 1 {
            std::mem::take(&mut updates[0])
        } else {
            merge_updates_v1(updates)? // try to compact updates together
        };
        let mut buf = Message::Sync(SyncMessage::Update(compacted)).encode_v1();
        if let Some(other) = other {
            buf.extend_from_slice(&other);
        }
        Ok(buf)
    }

    /// Returns notifier that will complete when given number of updates has been received.
    pub fn update_barrier<F>(&self, predicate: F) -> Arc<tokio::sync::Notify>
    where
        F: Fn(&TransactionMut, &UpdateEvent) -> bool + Send + Sync + 'static,
    {
        let barrier = Arc::new(tokio::sync::Notify::new());
        {
            let barrier = barrier.clone();
            self.awareness
                .doc()
                .observe_update_v1_with("countdown", move |txn, e| {
                    if predicate(txn, e) {
                        barrier.notify_waiters();
                    }
                })
                .unwrap();
        }
        barrier
    }
}

struct TestSink(UnboundedSender<Bytes>);

impl futures::Sink<Bytes> for TestSink {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        self.0
            .send(item)
            .map_err(|_| Error::SubscriberDropped(Default::default()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
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

/// Load the testing data at the specified file. If the filename ends in .gz, it will be
/// transparently uncompressed.
///
/// This method panics if the file does not exist, or is corrupt. It'd be better to have a try_
/// variant of this method, but given this is mostly for benchmarking and testing, I haven't felt
/// the need to write that code.
pub fn load_test_scenario(filename: &str) -> TestData {
    // let start = SystemTime::now();
    // let mut file = File::open("benchmark_data/automerge-paper.json.gz").unwrap();
    let file = File::open(filename).unwrap();

    let mut reader = BufReader::new(file);
    // We could pass the GzDecoder straight to serde, but it makes it way slower to parse for
    // some reason.
    let mut raw_json = vec![];

    if filename.ends_with(".gz") {
        let mut reader = GzDecoder::new(reader);
        reader.read_to_end(&mut raw_json).unwrap();
    } else {
        reader.read_to_end(&mut raw_json).unwrap();
    }

    let data: TestData = serde_json::from_reader(raw_json.as_slice()).unwrap();
    data
}

/// (position, delete length, insert content).
#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
pub struct TestPatch(pub usize, pub usize, pub String);

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
pub struct TestTxn {
    // time: String, // ISO String. Unused.
    pub patches: Vec<TestPatch>,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
pub struct TestData {
    #[serde(default)]
    pub using_byte_positions: bool,

    #[serde(rename = "startContent")]
    pub start_content: String,
    #[serde(rename = "endContent")]
    pub end_content: String,

    pub txns: Vec<TestTxn>,
}

impl TestData {
    pub fn run(self, doc: &Doc) {
        let txt = doc.get_or_insert_text("text");
        let client_id = doc.client_id();
        let mut i = 0;
        for t in self.txns {
            if i % 1000 == 0 {
                //sleep(std::time::Duration::from_millis(15));
            }
            let mut txn = doc.transact_mut_with(client_id);
            for patch in t.patches {
                let at = patch.0;
                let delete = patch.1;
                let content = patch.2;

                if delete != 0 {
                    txt.remove_range(&mut txn, at as u32, delete as u32);
                }
                if !content.is_empty() {
                    txt.insert(&mut txn, at as u32, &content);
                }
            }
            i += 1;
        }
    }
}
