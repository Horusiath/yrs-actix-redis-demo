pub mod broadcast;
pub mod error;
mod lease;
pub mod snapshot;
pub mod test_utils;

use crate::broadcast::BroadcastGroup;
use actix_web::web::{Data, Query};
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer};
use opendal::{Builder, OperatorBuilder};
use serde::Deserialize;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

const PAYLOAD_SIZE_LIMIT: usize = 2 * 1024 * 1024; // 2MiB

#[actix::main]
async fn main() -> Result<(), actix_web::Error> {
    const PORT: u16 = 8080;
    env_logger::init();

    let stream_id: Arc<str> = "test-stream".into();
    let redis = redis::Client::open("redis://localhost:6379").unwrap();
    let conn = redis.get_connection_manager().await.unwrap();
    let s3 = opendal::services::S3::default()
        .access_key_id("minioadmin")
        .secret_access_key("minioadmin")
        .region("eu-west-1")
        .endpoint("http://localhost:9000")
        .bucket("docs")
        .build()
        .unwrap();
    let snapshot_store = OperatorBuilder::new(s3).finish();
    let snapshotter = snapshot::Snapshotter::new(
        snapshot_store.clone(),
        conn.clone(),
        stream_id.clone(),
        std::time::Duration::from_secs(15), // snapshot and compact doc state every 15 seconds
    );
    let broadcast_group = BroadcastGroup::new(stream_id, conn, snapshotter);

    tracing::info!("Starting web server on port {}", PORT);
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(broadcast_group.clone()))
            .app_data(web::PayloadConfig::new(PAYLOAD_SIZE_LIMIT))
            .route("/doc", web::get().to(handler))
    })
    .bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, PORT)))?
    .run()
    .await?;
    Ok(())
}

async fn handler(
    req: HttpRequest,
    stream: web::Payload,
    broadcast_group: Data<BroadcastGroup>,
    query: Query<PeerInfo>,
) -> Result<HttpResponse, Error> {
    let (res, session, stream) = actix_ws::handle(&req, stream)?;

    let stream = stream
        .aggregate_continuations()
        // aggregate continuation frames up to 2MiB
        .max_continuation_size(PAYLOAD_SIZE_LIMIT);

    let subscriber_id = query.subscriber_id.clone();

    let broadcast_group = broadcast_group.into_inner();
    broadcast_group.subscribe(subscriber_id, session, stream);
    Ok(res)
}

#[derive(Debug, Deserialize)]
pub struct PeerInfo {
    pub subscriber_id: Arc<str>,
}
