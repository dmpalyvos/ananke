#!/bin/bash

experiments=$(ls scripts/experiments/*.sh)
COMMIT_CODE=$(date +%j_%H%M)
EXECUTION_ENVIRONMENT=$1
shift
for e in $experiments; do
  ./scripts/run.sh $e ${EXECUTION_ENVIRONMENT} $COMMIT_CODE "$@"
done
