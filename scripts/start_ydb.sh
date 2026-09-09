#!/bin/bash

docker run -d --platform linux/amd64 -p 2135:2135 -p 2136:2136 -p 8765:8765 \
  -p 9092:9092 -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 -e YDB_KAFKA_PROXY_PORT=9092 \
  ydbplatform/local-ydb:latest