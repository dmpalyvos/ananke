#!/usr/bin/env bash

[[ -n $1 ]] || { echo "Script path required"; exit 1; }
BASE_EXPERIMENT=$1
[[ -e $BASE_EXPERIMENT ]] || { echo "Multipart experiment $BASE_EXPERIMENT does not exist!"; exit 1; }
source $BASE_EXPERIMENT
shift
EXPERIMENT_ARGS="$*"
EXPERIMENTS=()

for part in "${MULTIPARTS[@]}"; do
  path="./scripts/experiments/$MULTIPARTS_DIR/$part"
  [[ -e $path ]] || { echo "Experiment $path does not exist!"; exit 1; }
  EXPERIMENTS+=("$path")
done

COMMIT_CODE=$(date +%j_%H%M)
for experiment in "${EXPERIMENTS[@]}"; do
  echo ">>> (Multipart) RUNNING: $experiment"
  ./scripts/run.sh $experiment -c "$COMMIT_CODE" $EXPERIMENT_ARGS
done
