use crate::broadcast::{Message, MessageId};
use crate::error::Error;
use crate::error::MissingUpdate;
use crate::lease::{Lease, LeaseAcquisition};
use actix_web::rt;
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
use tokio::time::MissedTickBehavior;
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
        snapshot_interval: Duration,
    ) -> Self {
        let state = SnapshotterState::new(stream_id, conn, s3, snapshot_interval);
        Self { state }
    }

    pub fn notify_one(&self) {
        self.state
            .update_ticks
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
    update_ticks: AtomicUsize,
    stream_id: Arc<str>,
}

impl SnapshotterState {
    fn new(
        stream_id: Arc<str>,
        conn: ConnectionManager,
        s3: Operator,
        snapshot_interval: Duration,
    ) -> Arc<Self> {
        let state = Arc::new(Self {
            redis: Mutex::new(conn),
            update_ticks: AtomicUsize::new(0),
            s3,
            stream_id,
        });
        {
            let state = Arc::downgrade(&state);
            rt::spawn(async move {
                let mut interval = tokio::time::interval(snapshot_interval);
                interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    match state.upgrade() {
                        None => break, // state dropped
                        Some(state) => {
                            if let Err(err) = state.snapshot(None).await {
                                tracing::warn!("Failed to snapshot: {:?}", err);
                            }
                        }
                    }
                }
            });
        }
        state
    }
    /// Requests a snapshot of the document state up to the given message id.
    /// This snapshot will be generated from the previous snapshot method plus all the Redis
    /// updates on top of it.
    ///
    /// If the snapshot didn't change since the last one, no new snapshot will be generated.
    /// If another snapshot request is in progress, this method will be no-op.
    pub async fn snapshot(&self, up_to: Option<MessageId>) -> Result<bool, Error> {
        let updates = self
            .update_ticks
            .swap(0, std::sync::atomic::Ordering::SeqCst);
        if updates == 0 {
            return Ok(false); // nothing to snapshot
        }
        let result = if let Some(lease_acq) = self.lease().await? {
            let snapshot = self.load(up_to).await?;

            let has_changed = if let Some(msg_id) = snapshot.last_message_id {
                // snapshot state changed
                self.store(snapshot.doc, msg_id.clone()).await?;
                true
            } else {
                false
            };

            self.release(lease_acq).await?;
            Ok(has_changed)
        } else {
            Ok(false) // another process is doing snapshot atm.
        };
        result
    }

    pub async fn load(&self, up_to: Option<MessageId>) -> Result<LoadedSnapshot, Error> {
        let snapshot = self.load_snapshot().await?;
        let messages = self.get_updates(up_to).await?;
        let mut snapshot_info = None;
        let doc = Doc::new();
        let mut txn = doc.transact_mut();
        if let Some(snapshot) = snapshot {
            let mut decoder = DecoderV1::new(Cursor::from(&snapshot));
            let msg_id: Arc<str> = decoder.read_string()?.into();
            snapshot_info = Some((msg_id.clone(), snapshot.len()));
            let update = Update::decode(&mut decoder)?;
            txn.apply_update(update)?;
            if txn.store().pending_update().is_some() {
                let missing = MissingUpdate::new(txn.state_vector(), Some(msg_id));
                return Err(Error::MissingUpdate(missing.into()));
            }
        }
        let mut last_message_id = None;
        let mut i = 0;
        for msg in messages {
            let update = Update::decode_v1(&msg.update)?;
            txn.apply_update(update)?;
            if txn.store().pending_update().is_some() {
                let missing = MissingUpdate::new(txn.state_vector(), Some(msg.id));
                return Err(Error::MissingUpdate(missing.into()));
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
        encoder.write_string(&msg_id);
        txn.encode_state_as_update(&StateVector::default(), &mut encoder);
        let snapshot: Bytes = encoder.to_vec().into();

        let len = snapshot.len();
        tracing::info!("Storing document snapshot at `{}` ({} bytes)", msg_id, len);

        self.upload_snapshot(snapshot).await?;
        self.cutoff(msg_id).await?;
        Ok(())
    }

    async fn get_updates(&self, up_to: Option<MessageId>) -> Result<Vec<Message>, Error> {
        const BATCH_SIZE: usize = 1000;
        let mut conn = self.redis.lock().await;
        let options = StreamReadOptions::default().count(BATCH_SIZE);
        let mut messages = Vec::new();
        let mut last_msg_id: Arc<str> = "0".into();
        loop {
            let reply: StreamReadReply = conn
                .xread_options(
                    &[self.stream_id.as_ref()],
                    &[last_msg_id.as_ref()],
                    &options,
                )
                .await?;
            let mut msg_count = 0;

            for res in Message::parse_redis_reply(reply) {
                let msg = res?;
                if Some(&msg.id) == up_to.as_ref() {
                    return Ok(messages);
                }
                last_msg_id = msg.id.clone();
                messages.push(msg);
                msg_count += 1;
            }

            if msg_count < BATCH_SIZE {
                break;
            }
        }
        Ok(messages)
    }

    /// Removes all messages from Redis up to the last message id.
    pub async fn cutoff(&self, last_message_id: MessageId) -> Result<usize, Error> {
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
        let count: usize = conn.xdel(self.stream_id.as_ref(), &msg_ids).await?;
        drop(conn);
        tracing::debug!(
            "Pruned redis stream <= `{}` ({} objects)",
            last_message_id,
            count
        );
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
        //TODO: we could have multiple snapshots - key format: {stream_id}/snapshot-{msg_id}.y1
        // in order to keep the latest snapshot first we'd need to revert it eg. `max_msg_id - msg_id`.
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
