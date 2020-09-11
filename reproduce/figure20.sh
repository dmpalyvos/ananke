#!/bin/bash

. shared_vars.sh

EXPERIMENT_NAME="external_db_carlocal"
OUTPUT_DATA_FOLDER="${COMMIT_HASH}"_"${EXPERIMENT_NAME}"

cd ..

echo "Reproduce Fig. 20 in two steps:"
echo "   1) run the 2 underlying experiments"
echo "   2) plot the experiment data"
test -d "${OUTPUT_PATH}/${OUTPUT_DATA_FOLDER}" && { echo "--------"; echo WARNING: Output folder "${OUTPUT_PATH}/${OUTPUT_DATA_FOLDER}" exists already. Proceeding anyways.; }
countdown 5 "to begin."

# run experiment
./scripts/run.sh scripts/experiments/car_local_sql.sh -r 10 -d 10 -c "${EXPERIMENT_NAME}"
./scripts/run.sh scripts/experiments/car_local_mongo.sh -r 10 -d 10 -c "${EXPERIMENT_NAME}"

test -d "${OUTPUT_PATH}/${OUTPUT_DATA_FOLDER}" || { echo "--------"; echo CRITICAL: Output folder "${OUTPUT_PATH}/${OUTPUT_DATA_FOLDER}" not found, aborting...; exit; }

# plot
python3 "${PLOT_SCRIPT}" --path "${OUTPUT_PATH}" --experiment "${OUTPUT_DATA_FOLDER}" --name carlocal --plot external