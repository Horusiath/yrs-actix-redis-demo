use crate::error::Error;
use async_trait::async_trait;
use bytes::Bytes;
use flate2::bufread::GzDecoder;
use futures::{SinkExt, Stream, StreamExt};
use serde::Deserialize;
use smallvec::{smallvec, SmallVec};
use std::fs::File;
use std::io::{BufReader, Read};
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::tungstenite::ClientRequestBuilder;
use yrs::sync::protocol::AsyncProtocol;
use yrs::sync::{Awareness, Message, MessageReader, SyncMessage};
use yrs::updates::decoder::DecoderV1;
use yrs::updates::encoder::{Encode, EncoderV1};
use yrs::{merge_updates_v1, AsyncTransact, Doc, Origin, ReadTxn, Text, TextRef, Update};

pub struct TestPeer {
    pub awareness: Arc<Awareness>,
    pub text: TextRef,
    pub name: Arc<str>,
}

impl TestPeer {
    pub fn new<S: Into<Arc<str>>>(name: S) -> Self {
        let name = name.into();
        let doc = Doc::new();
        let text = doc.get_or_insert_text("text");
        let awareness = Arc::new(Awareness::new(doc));
        Self {
            name,
            awareness,
            text,
        }
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), Error> {
        let url: Uri = format!("{}?subscriber_id={}", url, self.name)
            .parse()
            .unwrap(); //self.awareness.doc().client_id();
        let request = ClientRequestBuilder::new(url.clone());
        let (sender, rx) = unbounded_channel();
        let (ws_stream, _resp) = connect_async(request).await?;
        let (sink, stream) = ws_stream.split();
        {
            let sender = sender.clone();
            let myself: Origin = self.awareness.doc().client_id().into();
            self.awareness
                .doc()
                .observe_update_v1_with(format!("send-{}", url), move |txn, e| {
                    if txn.origin() == Some(&myself) {
                        // only send updates that we have originated
                        let msg = Message::Sync(SyncMessage::Update(e.update.clone()));
                        sender.send(msg).unwrap();
                    }
                })
                .unwrap();
        }

        let name = self.name.clone();
        tokio::spawn(async move {
            if let Err(err) = Self::send_messages(rx, sink).await {
                tracing::error!("peer `{}` send error: {:?}", name, err);
            }
        });
        let name = self.name.clone();
        let awareness = self.awareness.clone();
        tokio::spawn(async move {
            if let Err(err) = Self::receive_messages(stream, sender, awareness, name.clone()).await
            {
                tracing::error!("peer `{}` receive error: {:?}", name, err);
            }
        });

        Ok(())
    }

    async fn receive_messages<S, E>(
        mut stream: S,
        sender: UnboundedSender<Message>,
        awareness: Arc<Awareness>,
        name: Arc<str>,
    ) -> Result<(), Error>
    where
        S: Stream<Item = Result<tokio_tungstenite::tungstenite::Message, E>> + Unpin,
        Error: From<E>,
    {
        let protocol = RepairProtocol::default();
        let init = protocol.start::<EncoderV1>(&awareness).await?;
        for msg in init {
            sender.send(msg).map_err(|_| Error::ConnectionClosed)?;
        }
        if let Some(msg) = stream.next().await {
            let msg = msg?;
            tracing::trace!("peer `{}` received message", name);
            match msg {
                tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
                    let mut decoder = DecoderV1::from(bytes.as_ref());
                    let reader = MessageReader::new(&mut decoder);
                    for res in reader {
                        let msg = res?;
                        tracing::trace!("peer `{}` parsed message: {:?}", name, msg);
                        if let Some(reply) = protocol.handle_message(&awareness, msg).await? {
                            sender.send(reply).map_err(|_| Error::ConnectionClosed)?;
                        };
                    }
                }
                tokio_tungstenite::tungstenite::Message::Close(reason) => {
                    let reason = reason.map(|r| r.reason.to_string()).unwrap_or_default();
                    tracing::info!("peer `{}` connection closed: {}", name, reason);
                    return Ok(());
                }
                tokio_tungstenite::tungstenite::Message::Text(_) => {}
                tokio_tungstenite::tungstenite::Message::Ping(_) => {}
                tokio_tungstenite::tungstenite::Message::Pong(_) => {}
                tokio_tungstenite::tungstenite::Message::Frame(_) => {}
            }
        }
        tracing::debug!("peer `{}` closed", name);
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
            sink.flush().await.map_err(|_| Error::ConnectionClosed)?;
        }
        Ok(())
    }

    fn eager_prefetch(rx: &mut UnboundedReceiver<Message>, buf: Vec<u8>) -> Result<Vec<u8>, Error> {
        const SIZE_THRESHOLD: usize = 64 * 1024;
        let mut size_hint = buf.len();
        let mut updates: SmallVec<[Vec<u8>; 1]> = smallvec![buf];
        let mut other = None;
        // try to eagerly fetch more updates if they are already in the queue
        while let Ok(msg) = rx.try_recv() {
            match msg {
                Message::Sync(SyncMessage::Update(update)) => {
                    size_hint += update.len();
                    // we stack updates together until we reach a non-update message
                    updates.push(update);

                    if size_hint >= SIZE_THRESHOLD {
                        break; // potential size of the update may be over threshold, stop here and send what we have
                    }
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
            tracing::debug!("Compacting {} updates ({} bytes)", updates.len(), size_hint);
            merge_updates_v1(updates)? // try to compact updates together
        };
        let mut buf = Message::Sync(SyncMessage::Update(compacted)).encode_v1();
        if let Some(other) = other {
            buf.extend_from_slice(&other);
        }
        Ok(buf)
    }

    /// Waits until current peer state is equal to the other peer state.
    pub async fn wait_sync(&self, other: &Self) {
        let len = other.text.len(&other.awareness.doc().transact().await);
        let barrier = Arc::new(tokio::sync::Notify::new());
        let notified = barrier.notified();
        {
            let barrier = barrier.clone();
            let txt = self.text.clone();
            self.awareness
                .doc()
                .observe_update_v1_with("waiter", move |txn, _| {
                    if txt.len(txn) == len {
                        barrier.notify_waiters();
                    }
                })
                .unwrap();
        }
        if self.text.len(&self.awareness.doc().transact().await) == len {
            return;
        }
        notified.await
    }
}

#[derive(Default)]
pub struct RepairProtocol {
    repair_in_progress: AtomicBool,
}

#[async_trait]
impl AsyncProtocol for RepairProtocol {
    async fn handle_sync_step2(
        &self,
        awareness: &Awareness,
        update: Update,
    ) -> Result<Option<Message>, yrs::sync::Error> {
        let mut txn = awareness.doc().transact_mut().await;
        txn.apply_update(update)?;
        let has_missing =
            txn.store().pending_update().is_some() || txn.store().pending_ds().is_some();
        if has_missing {
            let repair_in_progress = self
                .repair_in_progress
                .swap(true, std::sync::atomic::Ordering::SeqCst);
            if repair_in_progress {
                Ok(None)
            } else {
                tracing::trace!("Requesting repair");
                let sv = txn.state_vector();
                Ok(Some(Message::Sync(SyncMessage::SyncStep1(sv))))
            }
        } else {
            if self
                .repair_in_progress
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                tracing::trace!("Repaired");
            }
            Ok(None)
        }
    }
    async fn handle_update(
        &self,
        awareness: &Awareness,
        update: Update,
    ) -> Result<Option<Message>, yrs::sync::Error> {
        self.handle_sync_step2(awareness, update).await
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
    pub async fn run(self, doc: &Doc, delay: Option<Duration>) {
        let txt = doc.get_or_insert_text("text");
        let client_id = doc.client_id();
        for t in self.txns.into_iter().take(1) {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let mut txn = doc.transact_mut_with(client_id).await;
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
        }
    }
}
