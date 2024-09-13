use crate::error::Error;
use arc_swap::ArcSwapOption;
use opendal::Operator;
use redis::aio::ConnectionManager;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use yrs::Doc;

#[derive(Debug)]
pub struct Snapshotter {
    state: Arc<SnapshotterState>,
}

impl Snapshotter {
    pub fn new(s3: Operator, redis: ConnectionManager, snapshot_threshold: u32) -> Self {
        let state = SnapshotterState {
            redis,
            s3,
            snapshot_threshold,
            update_ticks: AtomicU32::new(0),
        };
        Self {
            state: Arc::new(state),
        }
    }

    pub fn notify(&self, msg_id: Arc<str>) {
        let prev = self
            .state
            .update_ticks
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        if prev >= self.state.snapshot_threshold {
            let state = self.state.clone();
            tokio::spawn(async move {
                if let Err(err) = state.snapshot(Some(msg_id.clone())).await {
                    tracing::warn!("Failed to snapshot at `{}`: {:?}", msg_id, err);
                }
            });
        }
    }

    #[inline]
    pub async fn snapshot(&self, up_to_msg_id: Option<Arc<str>>) -> Result<Doc, Error> {
        self.state.snapshot(up_to_msg_id).await
    }
}

struct SnapshotterState {
    redis: ConnectionManager,
    s3: Operator,
    snapshot_threshold: u32,
    update_ticks: AtomicU32,
}

impl SnapshotterState {
    pub async fn snapshot(&self, up_to_msg_id: Option<Arc<str>>) -> Result<Doc, Error> {
        todo!()
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
