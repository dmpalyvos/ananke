#!/bin/bash

. shared_vars.sh

EXPERIMENT_NAME="synthetic2"
OUTPUT_DATA_FOLDER="${COMMIT_HASH}"_"${EXPERIMENT_NAME}"

cd ..

echo "Reproduce the figure in two steps:"
echo "   1) run the underlying experiment"
echo "   2) plot the experiment data"
test -d "${OUTPUT_PATH}/${OUTPUT_DATA_FOLDER}" && { echo "--------"; echo WARNING: Output folder "${OUTPUT_PATH}/${OUTPUT_DATA_FOLDER}" exists already. Proceeding anyways.; }
echo "WARNING: This experiment consumes a lot of memory. Please ensure that the Flink taskexecutor is configured to use at least 32GB RAM."
countdown 5 "to begin."

# run experiment
./scripts/run.sh scripts/experiments/synthetic2.sh -r 10 -d 10 -c "${EXPERIMENT_NAME}"

test -d "${OUTPUT_PATH}/${OUTPUT_DATA_FOLDER}" || { echo "--------"; echo CRITICAL: Output folder "${OUTPUT_PATH}/${OUTPUT_DATA_FOLDER}" not found, aborting...; exit; }

# plot
python3 "${PLOT_SCRIPT}" --path "${OUTPUT_PATH}" --experiment "${OUTPUT_DATA_FOLDER}" --name synthetic2 --plot synthetic2