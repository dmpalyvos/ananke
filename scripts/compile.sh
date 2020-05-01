#!/bin/bash

COMMIT="$(git rev-parse --short HEAD)"
mvn clean package
mv "target/ananke-0.1.jar" "target/ananke-${COMMIT}.jar"
echo "Using commit $COMMIT"

