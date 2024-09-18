use crate::broadcast::{Message, MessageId};
use crate::error::Error;
use crate::lease::{Lease, LeaseAcquisition};
use bytes::Bytes;
use opendal::{ErrorKind, Operator};
use redis::aio::ConnectionManager;
use redis::streams::{StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, FromRedisValue};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use yrs::encoding::read::{Cursor, Read};
use yrs::encoding::write::Write;
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::{Encoder, EncoderV1};
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

#[derive(Debug)]
pub struct Snapshotter {
    state: Arc<SnapshotterState>,
}

impl Snapshotter {
    pub fn new(
        s3: Operator,
        conn: ConnectionManager,
        stream_id: Arc<str>,
        snapshot_threshold: usize,
    ) -> Self {
        let state = SnapshotterState {
            redis: Mutex::new(conn),
            s3,
            snapshot_threshold,
            stream_id,
            update_ticks: Default::default(),
        };
        Self {
            state: Arc::new(state),
        }
    }

    pub fn notify(&self, msg_count: usize, last_message_id: MessageId) {
        let prev = self
            .state
            .update_ticks
            .fetch_add(msg_count, std::sync::atomic::Ordering::SeqCst);

        if prev + msg_count >= self.state.snapshot_threshold {
            let state = self.state.clone();
            tokio::spawn(async move {
                if let Err(err) = state.snapshot(Some(last_message_id.clone())).await {
                    tracing::warn!("Failed to snapshot at `{}`: {:?}", last_message_id, err);
                }
            });
        }
    }

    #[inline]
    pub async fn snapshot(&self, up_to: Option<MessageId>) -> Result<bool, Error> {
        self.state.snapshot(up_to).await
    }

    #[inline]
    pub async fn load(&self, up_to: Option<MessageId>) -> Result<LoadedSnapshot, Error> {
        self.state.load(up_to).await
    }

    #[inline]
    pub async fn cutoff(&self, last_message_id: MessageId) -> Result<usize, Error> {
        self.state.cutoff(last_message_id).await
    }
}

struct SnapshotterState {
    redis: Mutex<ConnectionManager>,
    s3: Operator,
    snapshot_threshold: usize,
    update_ticks: AtomicUsize,
    stream_id: Arc<str>,
}

impl SnapshotterState {
    /// Requests a snapshot of the document state up to the given message id.
    /// This snapshot will be generated from the previous snapshot method plus all the Redis
    /// updates on top of it.
    ///
    /// If the snapshot didn't change since the last one, no new snapshot will be generated.
    /// If another snapshot request is in progress, this method will be no-op.
    pub async fn snapshot(&self, up_to: Option<MessageId>) -> Result<bool, Error> {
        let counter = self.update_ticks.load(std::sync::atomic::Ordering::SeqCst);
        let result = if let Some(lease_acq) = self.lease().await? {
            let snapshot = self.load(up_to).await?;

            let has_changed = if let Some(msg_id) = snapshot.last_message_id {
                // snapshot state changed
                self.store(snapshot.doc, msg_id.clone()).await?;
                tracing::debug!(
                    "Snapshotted document `{}` at timestamp {}",
                    self.stream_id,
                    msg_id
                );
                true
            } else {
                false
            };

            self.release(lease_acq).await?;
            Ok(has_changed)
        } else {
            Ok(false)
        };
        self.update_ticks
            .fetch_sub(counter, std::sync::atomic::Ordering::SeqCst);
        result
    }

    pub async fn load(&self, up_to: Option<MessageId>) -> Result<LoadedSnapshot, Error> {
        let snapshot = self.load_snapshot().await?;
        let mut snapshot_info = None;
        let doc = Doc::new();
        let mut txn = doc.transact_mut();
        if let Some(snapshot) = snapshot {
            let mut decoder = DecoderV1::new(Cursor::from(&snapshot));
            let timestamp = decoder.read_u64()?;
            let seq_no = decoder.read_u64()?;
            snapshot_info = Some((MessageId { timestamp, seq_no }, snapshot.len()));
            let update = Update::decode(&mut decoder)?;
            txn.apply_update(update)?;
            if txn.store().pending_update().is_some() {
                return Err(Error::MissingUpdate(txn.state_vector()));
            }
        }
        let reply = self.get_updates().await?;
        let mut messages = Vec::new();
        for res in Message::parse_redis_reply(reply) {
            let msg = res?;
            if Some(msg.id) == up_to {
                break;
            }
            messages.push(msg);
        }
        let mut last_message_id = None;
        let mut i = 0;
        for msg in messages {
            let update = Update::decode_v1(&msg.update)?;
            txn.apply_update(update)?;
            if txn.store().pending_update().is_some() {
                return Err(Error::MissingUpdate(txn.state_vector()));
            }
            last_message_id = Some(msg.id);
            i += 1;
        }
        drop(txn);
        let snapshot_id = if let Some((msg_id, size)) = snapshot_info {
            tracing::info!(
                "Loaded snapshot from {} ({} bytes) + {} messages",
                msg_id,
                size,
                i
            );
            Some(msg_id)
        } else {
            None
        };
        Ok(LoadedSnapshot {
            doc,
            snapshot_id,
            last_message_id,
        })
    }

    pub async fn store(&self, doc: Doc, msg_id: MessageId) -> Result<(), Error> {
        let txn = doc.transact();
        let mut encoder = EncoderV1::new();
        encoder.write_u64(msg_id.timestamp);
        encoder.write_u64(msg_id.seq_no);
        txn.encode_state_as_update(&StateVector::default(), &mut encoder);
        let snapshot: Bytes = encoder.to_vec().into();

        let len = snapshot.len();
        tracing::info!("Storing document snapshot at `{}` ({} bytes)", msg_id, len);

        self.upload_snapshot(snapshot).await?;
        self.cutoff(msg_id).await?;
        Ok(())
    }

    async fn get_updates(&self) -> Result<StreamReadReply, Error> {
        let mut conn = self.redis.lock().await;
        let options = StreamReadOptions::default().count(self.snapshot_threshold);
        Ok(conn
            .xread_options(&[self.stream_id.as_ref()], &["0"], &options)
            .await?)
    }

    /// Removes all messages from Redis up to the last message id.
    pub async fn cutoff(&self, last_message_id: MessageId) -> Result<usize, Error> {
        tracing::debug!(
            "Pruning Redis stream `{}` up to {}",
            self.stream_id,
            last_message_id
        );
        let mut conn = self.redis.lock().await;
        let value = conn
            .xrange(self.stream_id.as_ref(), "-", last_message_id.to_string())
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

    async fn load_snapshot(&self) -> Result<Option<Bytes>, Error> {
        let path = format!("{}/snapshot.y1", self.stream_id);
        match self.s3.read_with(&path).await {
            Ok(buf) => Ok(Some(buf.current())),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    async fn upload_snapshot(&self, doc_state: Bytes) -> Result<(), Error> {
        //TODO: we could have multiple snapshots
        let path = format!("{}/snapshot.y1", self.stream_id);
        self.s3.write_with(&path, doc_state).await?;
        Ok(())
    }

    async fn lease(&self) -> Result<Option<LeaseAcquisition>, Error> {
        let mut conn = self.redis.lock().await;
        conn.lease(self.stream_id.clone(), Duration::from_secs(60))
            .await
    }

    async fn release(&self, acq: LeaseAcquisition) -> Result<(), Error> {
        let mut conn = self.redis.lock().await;
        conn.release(acq).await?;
        Ok(())
    }
}

impl Debug for SnapshotterState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotterState")
            .field("snapshot_threshold", &self.snapshot_threshold)
            .field("update_ticks", &self.update_ticks)
            .finish()
    }
}

#[derive(Debug)]
pub struct LoadedSnapshot {
    pub doc: Doc,
    pub snapshot_id: Option<MessageId>,
    pub last_message_id: Option<MessageId>,
}
