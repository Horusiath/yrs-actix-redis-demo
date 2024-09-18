use crate::broadcast::BroadcastGroup;
use crate::error::Error;
use bytes::Bytes;
use flate2::bufread::GzDecoder;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufReader, Read};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread::sleep;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use uuid::Uuid;
use yrs::sync::{Awareness, DefaultProtocol, Message, MessageReader, Protocol, SyncMessage};
use yrs::updates::decoder::DecoderV1;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, Origin, Text, TextRef, Transact, TransactionMut, UpdateEvent};

pub struct TestPeer {
    pub awareness: Arc<Awareness>,
    pub text: TextRef,
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
                    "TestPeer `{}` sending update ({} bytes)",
                    subscriber_id,
                    e.update.len()
                );

                let msg = Message::Sync(SyncMessage::Update(e.update.clone()));
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
                            let reader = MessageReader::new(&mut decoder);
                            for res in reader {
                                let msg = res.unwrap();
                                tracing::trace!("TestPeer `{}` received: {:?}", subscriber_id, msg);
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

fn test_subscriber<T>() -> (Arc<Mutex<TestSink>>, TestStream) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (Arc::new(Mutex::new(TestSink(tx))), TestStream(rx))
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
