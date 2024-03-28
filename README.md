# Go Redis

A simple Redis server with GoLang

## Features

- [X] Concurrent connections
- [X] Ping command
- [X] Echo command
- [X] Get and Set commands
- [X] Data expiration
- [X] Replication
- [X] RDB Persistance
- [X] Streams Support

## Resources

- [Build your own Redis](https://app.codecrafters.io/courses/redis/overview?_gl=1*unh9pm*_ga*MTIxODMxNTYxNi4xNzA5ODE3OTE2*_ga_N8D6K4M2HE*MTcxMDA2OTQ4OS4xMC4xLjE3MTAwNzExNjEuMC4wLjA.)

- [Redis serialization protocol specification](https://redis.io/docs/reference/protocol-spec/#resp-simple-strings)

- [Redis replication](https://redis.io/docs/management/replication)

- [Redis RDB File Format](https://rdb.fnordig.de/file_format.html)

- [Redis Streams](https://redis.io/docs/data-types/streams/)

### Run locally

```bash
go build -o redis-server ./app/server.go

./redis-server # starts as master on default port 6379
./redis-server --port 6380 --replicaof localhost 6379 # starts as replica on port 6380
```