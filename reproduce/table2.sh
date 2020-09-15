#!/bin/bash

. shared_vars.sh

EXPERIMENT_NAME="external_db_synthetic"
OUTPUT_DATA_FOLDER="${COMMIT_HASH}"_"${EXPERIMENT_NAME}"

cd ..

echo "Reproduce Table 2 in two steps:"
echo "   1) run the 2 underlying experiments"
echo "   2) plot the experiment data"
test -d "${OUTPUT_PATH}/${OUTPUT_DATA_FOLDER}" && { echo "--------"; echo WARNING: Output folder "${OUTPUT_DATA_FOLDER}" exists already. Proceeding anyways.; }
echo "WARNING: This experiment consumes a lot of memory. Please ensure that the Flink taskexecutor is configured to use at least 32GB RAM."
countdown 5 "to begin."

# run experiment
./scripts/run.sh scripts/experiments/synthetic1Ank1.sh -r 10 -d 10 -c "${EXPERIMENT_NAME}" -x
./scripts/run.sh scripts/experiments/synthetic1Sqlite.sh -r 10 -d 10 -c "${EXPERIMENT_NAME}" -x
./external/postgres/init_postgres
./scripts/run.sh scripts/experiments/synthetic1Postgres.sh -r 10 -d 10 -c "${EXPERIMENT_NAME}" -x
./external/mongo/init_mongo
./scripts/run.sh scripts/experiments/synthetic1Mongo.sh -r 10 -d 10 -c "${EXPERIMENT_NAME}" -x


test -d "${OUTPUT_PATH}/${OUTPUT_DATA_FOLDER}" || { echo "--------"; echo CRITICAL: Output folder "${OUTPUT_DATA_FOLDER}" not found, aborting...; exit; }

# plot
python3 "${PLOT_SCRIPT}" --path "${OUTPUT_PATH}" --experiment "${OUTPUT_DATA_FOLDER}" --name carlocal --plot external
