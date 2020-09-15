#!/usr/bin/env bash

ROOT_FOLDER=$1

PREPROCESS_FILES=("throughput" "latency" "traversal")
COPY_FILES=("memory")
EXTENSION=".csv"

if [ -z "$ROOT_FOLDER" ]; then
  echo "Please provide root folder as argument"
  exit 1
fi

if [ ! -e $ROOT_FOLDER ]; then
  echo "ERROR: Directory $ROOT_FOLDER does not exist!"
  exit 1
fi

cd ${ROOT_FOLDER}
for folder in $(ls); do
  rm $folder/*.csv
  echo ">>> Adding parallelism parameter to $folder"
  for execution in $(ls $folder); do
    mv "$folder/${execution}" "$folder/${execution}_1"
  done
done
