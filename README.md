## Compile and run the server

```bash
# build the web app image
docker build -t web-app .
# run all services
docker compose --file docker-compose.yml up -d
```