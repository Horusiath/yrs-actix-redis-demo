pub mod broadcast;
pub mod error;

use actix_web::web::Data;
use actix_web::{rt, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_ws::AggregatedMessage;
use futures::StreamExt;
use redis::Client;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

#[actix::main]
async fn main() -> Result<(), actix_web::Error> {
    const PORT: u16 = 8080;
    env_logger::init();
    let redis = redis::Client::open("redis://localhost:6379").unwrap();

    tracing::info!("Starting web server on port {}", PORT);
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(redis.clone()))
            .route("/echo", web::get().to(echo))
    })
    .bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, PORT)))?
    .run()
    .await?;
    Ok(())
}

async fn echo(
    req: HttpRequest,
    stream: web::Payload,
    redis: Data<Client>,
) -> Result<HttpResponse, Error> {
    let (res, mut session, stream) = actix_ws::handle(&req, stream)?;

    let mut stream = stream
        .aggregate_continuations()
        // aggregate continuation frames up to 1MiB
        .max_continuation_size(2_usize.pow(20));

    // start task but don't wait for it
    rt::spawn(async move {
        // receive messages from websocket
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(AggregatedMessage::Text(text)) => {
                    // echo text message
                    session.text(text).await.unwrap();
                }

                Ok(AggregatedMessage::Binary(bin)) => {
                    // echo binary message
                    session.binary(bin).await.unwrap();
                }

                Ok(AggregatedMessage::Ping(msg)) => {
                    // respond to PING frame with PONG frame
                    session.pong(&msg).await.unwrap();
                }

                _ => {}
            }
        }
    });

    // respond immediately with response connected to WS session
    Ok(res)
}
