#!/bin/bash

# SETUP SCRIPT 

# PREREQUISITES: 
# git (pre-installed on ubuntu server 20)
# maven (sudo apt install maven) 
# unzip (sudo apt install unzip) 
# java (sudo apt install default-jdk) / at least X GB of free space

# This script will download Apache Flink into this directory, then download
# the input data for the experiment, compile the application and eventually
# run two iterations of the Linear Road experiment.


# 1. Download Apache Flink
HOMEDIR=$(pwd)/flink-1.10.0
echo "Downloading Apache Flink..."
wget -q --show-progress https://archive.apache.org/dist/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz
echo "Unpacking Apache Flink..."
tar zxvf flink-1.10.0-bin-scala_2.11.tgz
rm zxvf flink-1.10.0-bin-scala_2.11.tgz
echo "Done."

# 2. Download input data for the experiments
# echo "Downloading 2GB of input data."
# CARCLOUD="beijing_allIn1day_ts_sorted.txt"
# LR="h1.txt"
# CARLOCAL="carLocalInput.zip"
# echo "Downloading input data to data/input/"
# echo
# echo "File 1/3"
# wget -q --show-progress -O data/input/${CARCLOUD} https://chalmersuniversity.box.com/shared/static/qzqvlsatyb37a9d3kvfk224ehj0bipki.txt
# echo
# echo "File 2/3"
# wget -q --show-progress -O data/input/${CARLOCAL} https://chalmersuniversity.box.com/shared/static/7s9ewtys69aik5p8bwapbazjs2u9l8vv.zip
# echo
# echo "File 3/3"
# wget -q --show-progress -O data/input/${LR} https://chalmersuniversity.box.com/shared/static/ioal17insfry4naurtybkp44dxev59ta.txt
# echo
# echo "Extracting files..."
# unzip -q data/input/${CARLOCAL} -d data/input/
# rm data/input/${CARLOCAL}
# echo "Input data downloaded."
./standalone_input_data_downloader.sh

# 3. Set up the configuration file and compile application
echo "Updating config file"
sed -i "s|PATH_HERE|$HOMEDIR|g" scripts/config.sh
echo "Compiling Ananke"
./scripts/compile.sh

# 4. Start a Flink Cluster on which to run a short test experiment
echo "Starting Flink Cluster"
./flink-1.10.0/bin/start-cluster.sh

# 5. Run a short experiment
echo "---------------------------------------"
echo "Running a short demonstrator experiment in"
echo -n 5
sleep 1
for counter in 4 3 2 1; do
	echo -n -e  \\b$counter; sleep 1
done
echo

./scripts/run.sh ./scripts/experiments/setup_exp.sh -d 1 -r 1

# 6. Stop the Flink Cluster again
echo "Stopping Flink cluster"
./flink-1.10.0/bin/stop-cluster.sh

echo "If you read \"Experiment OK!\", then the setup was now succesfull."