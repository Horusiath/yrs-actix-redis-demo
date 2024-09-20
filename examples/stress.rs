use tokio::time::Instant;
use yrs::{GetString, ReadTxn, Text, Transact};
use yrs_actix_redis_demo::test_utils::{load_test_scenario, TestPeer};

#[tokio::main]
async fn main() {
    env_logger::init();
    let test_scenario = load_test_scenario("./data/automerge-paper.json.gz");
    const SERVER_URL: &'static str = "ws://localhost:8080/doc";

    let mut p1 = TestPeer::new("A");
    let mut p2 = TestPeer::new("B");
    p1.connect(SERVER_URL).await.unwrap();
    p2.connect(SERVER_URL).await.unwrap();

    tracing::info!(
        "applying {} transactions in total",
        test_scenario.txns.len()
    );

    // start emitting updates on P1
    let start = Instant::now();
    test_scenario.run(p1.awareness.doc(), None).await;
    tracing::info!("all patches applied");

    // setup barrier to wait for all updates to arrive to P2
    let expected_len = p1.text.len(&p1.awareness.doc().transact());
    let txt = p2.text.clone();
    let barrier = p2.update_barrier(move |tx, _| txt.len(tx) == expected_len);

    // wait for all emitted updates to arrive to P2
    barrier.notified().await;
    let elapsed = start.elapsed();
    tracing::info!("barrier reached - elapsed time: {:?}", elapsed);

    // check if P2 integrated all the updates
    let tx = p2.awareness.doc().transact();
    assert!(tx.store().pending_update().is_none(), "no pending updates");
    let p2_str = p2.text.get_string(&tx);
    let p1_str = p1.text.get_string(&p1.awareness.doc().transact());
    assert_eq!(p1_str, p2_str);

    // add new peer and init it with the same document
    tracing::info!("update doc from scratch");
    let start = Instant::now();
    let mut p3 = TestPeer::new("C");
    let txt = p3.text.clone();
    let barrier = p3.update_barrier(move |tx, _| txt.len(tx) == expected_len);
    p3.connect(SERVER_URL).await.unwrap();
    barrier.notified().await;
    let elapsed = start.elapsed();
    tracing::info!("doc initialized from scratch - elapsed time: {:?}", elapsed);
    let p3_str = p3.text.get_string(&p3.awareness.doc().transact());
    assert_eq!(p1_str, p3_str);
}
