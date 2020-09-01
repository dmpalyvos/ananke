# Ananke

This repository contains the implementation of the Ananke provenance framework, as well as performance evaluation experiments.
In the following, we are providing necessary setup steps as well as instructions on how to run the experiments from our paper and how to visualize them.
All steps have been tested under **Ubuntu 20.04** and should run with other Linux distributions as well.

## Setup

Two setups are offered, automated and manual. In any case, resolve the dependencies first.

### Dependencies required before setup

- git
- maven (sudo apt install maven) 
- unzip (sudo apt install unzip) 
- java (sudo apt install default-jdk) 
- docker ([see here](https://docs.docker.com/engine/install/ubuntu/)), must be [setup to run without root](https://docs.docker.com/engine/install/linux-postinstall/)

### Automated

This method will automatically download Apache Flink 1.10 and the input datasets, configure path variables, compile the Ananke framework and run a short demonstrator experiment to see whether the setup was succesful.

1. Clone this repository.
2. From the top-level folder, run `./auto_setup.sh`.
3. Done.

### Manual

1. Clone this repository.
2. Download Apache Flink 1.10 from [here](https://archive.apache.org/dist/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz) to a folder of your choosing.
3. Untar Flink: `tar zxvf flink-1.10.0-bin-scala_2.11.tgz`.
4. Open the file `scripts/config.sh` and replace `PATH_HERE` with the location of the untared Flink folder.
5. Download the datasets by running `./standalone_input_data_downloader.sh` from the top-level directory.
6. **Compile Ananke** with our compiler script by running `./scripts/compile.sh` from the top-level directory. 

You are now done with the setup. To run a short demonstrator experiment for checking the success of the setup, execute the following steps:

1. Start a Flink cluster: `$FLINK_HOME_DIR/bin/start-cluster.sh`.
2. Start a short experiment: `./scripts/run.sh ./scripts/experiments/setup_exp.sh -d 1 -r 1`.
3. Don't forget to stop the Flink cluster again: `$FLINK_HOME_DIR/bin/stop-cluster.sh`.

## Running Experiments

Experiment scripts are found in `scripts/experiments` and can be executed by calling `scripts/run.sh` from the top-level directory of the project. The run script takes care of creating output directories based on the commit hash and the date, and it also preprocesses the output after the end of the experiment. It can control maximum duration, number of repetitions, etc. using CLI args. For example:

```bash
# Run the lrAnankeCompare experiment for 10 reps of 10 minutes
./scripts/run.sh ./scripts/experiments/lrAnankeCompare.sh -d 10 -r 10
```

### Automatic reproduction of paper's experiments and plots

Here, we describe how to automatically reproduce the results from our paper on your available hardware.
**Caution: The dataset used for the Smart Grid queries must not be published due to privacy regulations, the corresponding experiments can thus not be reproduced by third parties.**

#### Manual experiment execution and plotting

- Comparison with the state-of-the-art, logical latency: `car_cloud_odroid_logical_latency`, `car_local_odysseus_logical_latency`, `lrAnankeCompare`, `sgAnankeCompare`.
- Synthetic 1 (varying provenance size and overlap): `synthetic1`
- Synthetic 2 (varying #queries and parallelism): `synthetic2`
