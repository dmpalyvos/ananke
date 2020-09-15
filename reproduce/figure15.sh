#!/bin/bash

. shared_vars.sh

EXPERIMENT_NAME="provenanceLatency"
OUTPUT_DATA_FOLDER="${COMMIT_HASH}"_"${EXPERIMENT_NAME}"


LR_DATA="${COMMIT_HASH}"_lrPerformance
CC_DATA="${COMMIT_HASH}"_carCloudPerformance
CL_DATA="${COMMIT_HASH}"_carLocalPerformance

cd ..

echo "This requires that the scripts figure10.sh, figure13.sh, figure14.sh have been run before!"
countdown 5 "to begin."

test -d "${OUTPUT_PATH}/${LR_DATA}" || { echo "--------"; echo ERROR: Output folder "${OUTPUT_PATH}/${LR_DATA}" not found. Please run figure10.sh; exit 1; }
test -d "${OUTPUT_PATH}/${CC_DATA}" || { echo "--------"; echo ERROR: Output folder "${OUTPUT_PATH}/${CC_DATA}" not found. Please run figure13.sh; exit 1; }
test -d "${OUTPUT_PATH}/${CL_DATA}" || { echo "--------"; echo ERROR: Output folder "${OUTPUT_PATH}/${CL_DATA}" not found. Please run figure14.sh; exit 1; }


# plot
python3 "${PLOT_SCRIPT}" --path "${OUTPUT_PATH}" --experiment "${LR_DATA}" "${CC_DATA}" "${CL_DATA}" --name all --plot logicallatency