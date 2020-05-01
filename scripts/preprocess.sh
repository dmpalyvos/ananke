#!/usr/bin/env bash

shopt -s nullglob # Prevent null globs

ROOT_FOLDER=$1

PREPROCESS_FILES=("rate" "latency" "traversal")
COPY_FILES=("memory" "cpu")
EXTENSION=".csv"


if [ -z "${ROOT_FOLDER}" ]; then
  echo "Please provide root (commit) folder as argument"
  exit 1
fi

if [ ! -e ${ROOT_FOLDER} ]; then
  echo "ERROR: Directory $ROOT_FOLDER does not exist!"
  exit 1
fi

for experiment in "$ROOT_FOLDER"/**/; do
  rm "$experiment"/*.csv 2> /dev/null
  echo ">>> Processing $experiment"
  for execution in "$experiment"/**/; do
    executionCode=$(basename "$execution")
    rep=$(echo $executionCode | cut -d _ -f1)
    parallelism=$(echo $executionCode | cut -d _ -f2)
    counter=0
    for metric in "${PREPROCESS_FILES[@]}"; do
      for file in  "$execution/$metric"_*.csv; do
        baseName=$(basename "$file" "$EXTENSION")
        node=${baseName#${metric}_}
        nodeName=$(echo "$node" | cut -d _ -f1)
        nodeIndex=$(echo "$node" | cut -d _ -f2)
        awk -v rep=$rep -v parallelism=$parallelism -v nodeName=$nodeName -v nodeIndex=$nodeIndex -F "," '{print rep "," parallelism "," nodeName "," nodeIndex "," $0}' "$file" >> "${experiment}/${metric}.csv"
        let counter++
      done
    done
    for metric in "${COPY_FILES[@]}"; do
        awk -v rep=$rep -v parallelism=$parallelism -F "," '{print rep "," parallelism "," $0}' "${execution}/$metric.csv" >> "${experiment}/${metric}.csv"
        let counter++
    done
    # Logical Latency Special Handling
    ./scripts/logicalLatency "$(pwd)/$execution"
    for file in  "$execution/logical-latency_"*.csv; do
      baseName=$(basename "$file" ".out.csv")
      node=${baseName#logical-latency_}
      nodeName=$(echo "$node" | cut -d _ -f1)
      nodeIndex=$(echo "$node" | cut -d _ -f2)
      awk -v rep=$rep -v parallelism=$parallelism -v nodeName=$nodeName -v nodeIndex=$nodeIndex -F "," '{print rep "," parallelism "," nodeName "-" $1 "," nodeIndex "," $2 "," $3}' "$file" >> "${experiment}/logical-latency.csv"
      let counter++
      done
    echo "Processed $experimentId: $counter files"
    echo  "----------------------"
  done
done
