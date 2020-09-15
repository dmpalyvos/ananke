#!/usr/bin/env bash

SCRIPT_DIR=$(dirname $0)
cd "$SCRIPT_DIR"
echo "Terminating any running containers..."
for directory in */; do
  echo $directory
  cd "$directory"
  docker-compose down &> /dev/null
  cd ..
done

echo "Removing volumes..."
docker rm -f $(docker ps -a -q) &> /dev/null
docker volume rm $(docker volume ls -q) &> /dev/null

exit 0