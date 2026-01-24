## Compile and run the server

This is a demo of web service capable of serving yjs/yrs update protocol between clients using WebSocket. 
This service can be scaled over multiple instances (multiplexed over using ie. configured NGINX proxy server).

Messages between different instances are shared over Redis Streams. Additionally Redis is used as a latching 
mechanism for situations that require compacting messages into document state in a way that doesn't impose risk 
of duplicating the compaction work.

Eventually document state is persisted using S3-compatible object storage, which thanks to [OpenDAL](https://opendal.apache.org)
implementation, could be also replaced by other sort of persistent key-value or file storage.

```bash
# build the web app image
docker build -t web-app .
# run all services
docker compose --file docker-compose.yml up -d
```

## Run stress test

```bash
cargo run --release --example stress
```
