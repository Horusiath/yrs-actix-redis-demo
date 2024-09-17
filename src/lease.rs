use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rand::random;
use redis::aio::ConnectionManager;
use redis::{RedisResult, Value};

use crate::error::Error;

const RELEASE_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
"#;

#[derive(Debug)]
pub struct LeaseAcquisition {
    key: String,
    token: u64,
}

/// This is Redlock algorithm implementation.
/// See: https://redis.io/docs/latest/commands/set#patterns
#[async_trait]
pub trait Lease {
    /// Attempt to acquire lease on a stream for a given time-to-live.
    /// Returns `None` if the lease could not be acquired.
    async fn lease(
        &mut self,
        stream_id: Arc<str>,
        ttl: Duration,
    ) -> Result<Option<LeaseAcquisition>, Error>;

    /// Releases a previously acquired lease (via: [Lease::lease]).
    async fn release(&mut self, acq: LeaseAcquisition) -> Result<bool, Error>;
}

#[async_trait]
impl Lease for ConnectionManager {
    async fn lease(
        &mut self,
        stream_id: Arc<str>,
        ttl: Duration,
    ) -> Result<Option<LeaseAcquisition>, Error> {
        let ttl = ttl.as_millis() as u64;
        let token = random::<u64>();
        let key = format!("{}-lease", stream_id);
        tracing::trace!("acquiring lease {} for {}ms", key, ttl);
        let result: RedisResult<Value> = redis::cmd("SET")
            .arg(&key)
            .arg(token)
            .arg("NX")
            .arg("PX")
            .arg(ttl)
            .query_async(self)
            .await;

        match result {
            Ok(Value::Okay) => Ok(Some(LeaseAcquisition { key, token })),
            Ok(o) => {
                tracing::trace!("lease locked: {:?}", o);
                Ok(None)
            }
            Err(err) => Err(Error::Redis(err)),
        }
    }

    async fn release(&mut self, acq: LeaseAcquisition) -> Result<bool, Error> {
        let script = redis::Script::new(RELEASE_SCRIPT);
        let result: i32 = script
            .key(acq.key)
            .arg(acq.token)
            .invoke_async(self)
            .await?;
        Ok(result == 1)
    }
}

#[cfg(test)]
mod test {
    use crate::lease::Lease;
    use redis::Client;

    #[tokio::test]
    async fn lease_acquisition() {
        let redis_client = Client::open("redis://localhost:6379").unwrap();
        let mut conn = redis_client.get_connection_manager().await.unwrap();

        let l1 = conn
            .lease("stream1".into(), std::time::Duration::from_secs(1))
            .await
            .unwrap();

        assert!(l1.is_some(), "should successfully acquire lease");

        let l2 = conn
            .lease("stream1".into(), std::time::Duration::from_secs(1))
            .await
            .unwrap();

        assert!(l2.is_none(), "should fail to acquire lease");

        conn.release(l1.unwrap()).await.unwrap();

        let l3 = conn
            .lease("stream1".into(), std::time::Duration::from_secs(1))
            .await
            .unwrap();

        assert!(
            l3.is_some(),
            "should successfully acquire lease after it was released"
        );
    }
}
