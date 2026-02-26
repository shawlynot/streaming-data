#!/usr/bin/env bash

set -e 
docker build -t ghcr.io/shawlynot/streaming-data:latest .
docker push ghcr.io/shawlynot/streaming-data:latest