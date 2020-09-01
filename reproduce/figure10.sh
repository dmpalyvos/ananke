. shared_vars.sh

EXPERIMENT_NAME="lrAnankeCompare"


# run experiment
cd ..
./scripts/run.sh /scripts/experiments/lrAnankeCompare.sh -r 10 -d 5 -c "${EXPERIMENT_NAME}"

# plot
# python3 "${PLOT_SCRIPT}" --plotStyle performance --figureTitle "Performance - Linear Road Queries" --inputData "${OUTPUT_DATA_FOLDER}"