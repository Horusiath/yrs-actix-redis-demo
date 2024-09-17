use opendal::services::MemoryConfig;
use opendal::Scheme::Memory;
use opendal::{Builder, Configurator, OperatorBuilder};
use redis::Client;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};
use uuid::Uuid;
use yrs::{GetString, ReadTxn, Transact};
use yrs_actix_redis_demo::broadcast::BroadcastGroup;
use yrs_actix_redis_demo::snapshot::Snapshotter;
use yrs_actix_redis_demo::test_utils::{load_test_scenario, TestPeer};

#[tokio::main]
async fn main() {
    env_logger::init();
    let test_scenario = load_test_scenario("./data/automerge-paper.json.gz");

    let stream_id: Arc<str> = format!("test-stream-{}", Uuid::new_v4()).into();
    let redis_client = Client::open("redis://localhost:6379").unwrap();
    let conn = redis_client.get_connection_manager().await.unwrap();
    //let store = opendal::services::S3::default()
    //    .access_key_id("minioadmin")
    //    .secret_access_key("minioadmin")
    //    .region("eu-west-1")
    //    .endpoint("http://localhost:9000")
    //    .bucket("docs")
    //    .build()
    //    .unwrap();
    let store = MemoryConfig::default().into_builder().build().unwrap();
    let operator = OperatorBuilder::new(store).finish();
    let snapshotter = Snapshotter::new(operator, conn.clone(), stream_id.clone(), 20_000);
    let group = BroadcastGroup::new(stream_id, conn, snapshotter);

    let mut p1 = TestPeer::new();
    let mut p2 = TestPeer::new();
    p1.connect(&group);
    p2.connect(&group);

    // setup barrier to await for all updates send by P1 to be received by P2
    let update_count = test_scenario.txns.len();
    let counter = AtomicUsize::new(update_count);
    let barrier = p2.update_barrier(move |_, _| {
        let prev = counter.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        prev == 1
    });

    // start emitting updates on P1
    tracing::info!("applying {} transactions in total", update_count);
    let start = Instant::now();
    test_scenario.run(p1.awareness.doc());
    tracing::info!("all patches applied");
    let p1_str = p1.text.get_string(&p1.awareness.doc().transact());

    // wait for all emitted updates to arrive to P2
    barrier.notified().await;
    let elapsed = start.elapsed();
    tracing::info!("barrier reached - elapsed time: {:?}", elapsed);
    assert_eq!(
        update_count as u64,
        group.updates_received(),
        "all updates received"
    );
    assert_eq!(
        update_count as u64,
        group.updates_sent(),
        "all updates sent"
    );

    // check if P2 integrated all the updates
    let tx = p2.awareness.doc().transact();
    let expected_sv = tx.state_vector();
    assert!(tx.store().pending_update().is_none(), "no pending updates");
    let p2_str = p2.text.get_string(&tx);
    assert_eq!(p1_str, p2_str);

    // add new peer and init it with the same document
    tracing::info!("update doc from scratch");
    let start = Instant::now();
    let mut p3 = TestPeer::new();
    let barrier = p3.update_barrier(move |txn, _| {
        let sv = txn.state_vector();
        sv == expected_sv
    });
    p3.connect(&group);
    barrier.notified().await;
    let elapsed = start.elapsed();
    tracing::info!("doc initialized from scratch - elapsed time: {:?}", elapsed);
    let p3_str = p3.text.get_string(&p3.awareness.doc().transact());
    assert_eq!(p1_str, p3_str);
}
