#!/bin/bash

# AUTOMATIC SETUP SCRIPT 

# This script will download Apache Flink into this directory, then download
# the input data for the experiment, compile the application and eventually
# run two iterations of the Linear Road experiment.

. reproduce/shared_vars.sh
echo "Automatic setup about to start"
countdown 5 "to start."

# 1. Download Apache Flink
HOMEDIR=$(pwd)/flink-1.10.0
echo "Downloading Apache Flink..."
wget https://archive.apache.org/dist/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz
echo "Unpacking Apache Flink..."
tar zxvf flink-1.10.0-bin-scala_2.11.tgz
rm flink-1.10.0-bin-scala_2.11.tgz
echo "Done."

# 2. Download the required datasets
echo "Downloading datasets..."
bash standalone_input_data_downloader.sh

# 3. Set up the configuration file and compile application
echo "Updating config file..."
sed -i "s|PATH_HERE|$HOMEDIR|g" scripts/config.sh
echo "Compiling Ananke..."
./scripts/compile.sh


# 4. Set up plotting requirements
echo "Setting up plotting requirements..."
pip3 install -r python-requirements.txt


# 5. Run a short experiment
echo "---------------------------------------"
echo "Running a short demonstrator experiment in"
countdown 5


EXPERIMENT_NAME="setup_exp"
OUTPUT_DATA_FOLDER="${COMMIT_HASH}"_"${EXPERIMENT_NAME}"
./scripts/run.sh ./scripts/experiments/setup_exp.sh -d 1 -r 1 -c ${EXPERIMENT_NAME}
python3 "${PLOT_SCRIPT}" --path "${OUTPUT_PATH}" --experiment "${OUTPUT_DATA_FOLDER}" --name carcloud --plot soa


echo "If you read \"[YYYY-MM-DD HH:MM:SS] Experiment OK!\" in the output above, and a figure was created and saved to disk then the setup was now succesful (the content of the figure is just a placeholder)."