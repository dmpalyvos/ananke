#!/bin/bash

REMOTE_DIR="ananke/data/output"
TARGET_DIR="data/remote"

if [ -z $1 ]; then
  echo "Please provide node!"
  exit 1
fi
if [ -z $2 ]; then
  echo "Please provide commit #!"
  exit 1
fi

echo "Zipping..."
ssh $1 "cd $REMOTE_DIR && zip -r $2.zip $2"
echo "Transferring"
scp -r "$1:${REMOTE_DIR}/$2.zip" "$TARGET_DIR"
echo "Unzipping..."
cd $TARGET_DIR
unzip "$2.zip"

