#!/bin/bash

COMMIT="$(git rev-parse --short HEAD)"
mvn clean package
mv "target/ananke-1.0-SNAPSHOT.jar" "target/ananke-${COMMIT}.jar"
echo "Using commit $COMMIT"

