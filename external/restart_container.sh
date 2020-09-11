#!/usr/bin/env bash

set -e

[[ -n $1 ]] || { echo "Container name missing"; exit 1; }
[[ -n $2 ]] || { echo "Container port missing"; exit 1; }
SCRIPT_DIR=$(dirname $0)
CONTAINER="$1"
LOG_FILE="$CONTAINER.log"

cd "$SCRIPT_DIR"

./stop_containers.sh

cd "$CONTAINER"
rm -f "$LOG_FILE"
echo "(Re)starting container: $CONTAINER"
docker-compose up &>> "$LOG_FILE" &
../wait-for-it.sh -t 0 "127.0.0.1:$2"
sleep 5