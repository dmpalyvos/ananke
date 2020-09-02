. shared_vars.sh

EXPERIMENT_NAME="provenanceLatency"
OUTPUT_DATA_FOLDER="${COMMIT_HASH}"_"${EXPERIMENT_NAME}"


LR_DATA="${COMMIT_HASH}"_lrPerformance
CC_DATA="${COMMIT_HASH}"_carCloudPerformance
CL_DATA="${COMMIT_HASH}"_carLocalPerformance

cd ..

echo "Reproduce Fig. 16. This requires that the scripts figure10.sh, figure13.sh, figure15.sh have been run before!"

test -d "data/output/${LR_DATA}" || { echo "--------"; echo ERROR: Output folder "${LR_DATA}" not found. Please run fig10.sh; exit 1; }
test -d "data/output/${CC_DATA}" || { echo "--------"; echo ERROR: Output folder "${CC_DATA}" not found. Please run fig13.sh; exit 1; }
test -d "data/output/${CR_DATA}" || { echo "--------"; echo ERROR: Output folder "${CR_DATA}" not found. Please run fig15.sh; exit 1; }


# # plot
# # python3 "${PLOT_SCRIPT}" --plotStyle provenanceLatency --figureTitle "Provenance Latency" --inputData "data/output/${LR_DATA}" "data/output/${CC_DATA}" "data/output/${CR_DATA}"