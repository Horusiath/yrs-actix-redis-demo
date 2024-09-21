use tokio::time::Instant;
use yrs::{GetString, ReadTxn, Transact};
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

    println!(
        "applying {} transactions in total",
        test_scenario.txns.len()
    );

    // start emitting updates on P1
    let start = Instant::now();
    test_scenario.run(p1.awareness.doc(), None).await;
    println!("all patches applied");

    // wait for all emitted updates to arrive to P2
    p2.wait_sync(&p1).await;
    let elapsed = start.elapsed();
    println!("barrier reached - elapsed time: {:?}", elapsed);

    // check if P2 integrated all the updates
    let tx = p2.awareness.doc().transact();
    assert!(tx.store().pending_update().is_none(), "no pending updates");
    let p2_str = p2.text.get_string(&tx);
    let p1_str = p1.text.get_string(&p1.awareness.doc().transact());
    assert_eq!(p1_str, p2_str);

    // add new peer and init it with the same document
    println!("update doc from scratch");
    let start = Instant::now();
    let mut p3 = TestPeer::new("C");
    p3.connect(SERVER_URL).await.unwrap();
    p3.wait_sync(&p1).await;
    let elapsed = start.elapsed();
    println!("doc initialized from scratch - elapsed time: {:?}", elapsed);
    let p3_str = p3.text.get_string(&p3.awareness.doc().transact());
    assert_eq!(p1_str, p3_str);

    println!("stress test passed");
}
