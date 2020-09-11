#!/usr/bin/env bash

if [ -z $1 ]; then
  echo "No class name given!"
  exit 1
fi

BASE_DIR=$(pwd)
COMMIT_HASH=$(git rev-parse --short HEAD)
COMMIT="${COMMIT_HASH}_${COMMIT_CODE}"
JAR="$BASE_DIR/target/genealog-flink-${COMMIT_HASH}.jar"

${BASE_DIR}/flink-1.9.1/bin/flink run \
  --class flink.explore.$1 $JAR \
