. shared_vars.sh

EXPERIMENT_NAME="carCloudPerformance"
OUTPUT_DATA_FOLDER="${COMMIT_HASH}"_"${EXPERIMENT_NAME}"

cd ..

echo "Reproduce Fig. 13 in two steps:"
echo "   1) run the underlying experiment"
echo "   2) plot the experiment data"
test -d "data/output/${OUTPUT_DATA_FOLDER}" && { echo "--------"; echo WARNING: Output folder "${OUTPUT_DATA_FOLDER}" exists already. Proceeding anyways.; }
countdown 5 "to begin."

# run experiment
./scripts/run.sh scripts/experiments/car_cloud_local.sh -r 10 -d 10 -c "${EXPERIMENT_NAME}"

test -d "data/output/${OUTPUT_DATA_FOLDER}" || { echo "--------"; echo CRITICAL: Output folder "${OUTPUT_DATA_FOLDER}" not found, aborting...; exit; }

# # plot
# # python3 "${PLOT_SCRIPT}" --plotStyle performance --figureTitle "Performance - Vehicular Tracking" --inputData "data/output/${OUTPUT_DATA_FOLDER}"