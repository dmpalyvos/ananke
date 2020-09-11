#!/bin/bash

COMMIT="$(git rev-parse --short HEAD)"
mvn clean package
mv "target/genealog-flink-experiments-1.0-SNAPSHOT.jar" "target/genealog-flink-experiments-${COMMIT}.jar"
echo "Using commit $COMMIT"

