#!/usr/bin/env bash

COMMIT_CODE=$(date +%j_%H%M)
./scripts/run.sh ./scripts/experiments/synthetic1Ank1.sh -c $COMMIT_CODE -x $*
./scripts/run.sh ./scripts/experiments/synthetic1Sqlite.sh -c $COMMIT_CODE -x $*
./external/postgres/init_postgres
./scripts/run.sh ./scripts/experiments/synthetic1Postgres.sh -c $COMMIT_CODE -x $*
./external/mongo/init_mongo
./scripts/run.sh ./scripts/experiments/synthetic1Mongo.sh -c $COMMIT_CODE -x $*

