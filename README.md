# Ananke

![Poster outlining Ananke's main components](media/ananke_vldb21_poster.png)

This repository contains the implementation of the Ananke provenance framework and related performance evaluation experiments, as described in the publication:

> *Dimitris Palyvos-Giannas, Bastian Havers, Marina Papatriantafilou, and Vincenzo Gulisano. **[Ananke: A Streaming Framework for Live Forward Provenance](https://vldb.org/pvldb/vol14/p391-palyvos-giannas.pdf)**. PVLDB, 14(3): 391 - 403, 2021. doi:10.14778/3430915.3430928*

In the following, we are providing necessary setup steps as well as instructions on how to run the experiments from our paper and how to visualize them.
All steps have been tested under **Ubuntu 20.04**.

## Setup

### Dependencies

The following dependencies are not managed by our automatic setup and need to be installed beforehand.:

- git
- bc
- wget
- maven 
- unzip 
- java 
- python>=3.7 + pip3

For Ubuntu 20.04: `sudo apt-get install git wget bc maven unzip default-jdk python3-pip`

Some experiments rely on docker to be installed:

- docker ([see here](https://docs.docker.com/engine/install/ubuntu/)) must be [setup to run without root](https://docs.docker.com/engine/install/linux-postinstall/) 
- docker-compose ([see here](https://docs.docker.com/compose/install/))

### Automated

This method will automatically download Apache Flink 1.10 and the input datasets, configure path variables, compile the Ananke framework and run a short demonstrator experiment to see whether the setup was succesful.

1. Clone this repository: `git clone https://github.com/dmpalyvos/ananke.git`
2. From the top-level folder, run `./auto_setup.sh`.

### Manual

1. Clone this repository: `git clone https://github.com/dmpalyvos/ananke.git`
2. Download Apache Flink 1.10 from [here](https://archive.apache.org/dist/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz) to a folder of your choosing.
3. Untar Flink: `tar zxvf flink-1.10.0-bin-scala_2.11.tgz`.
4. Open the file `scripts/config.sh` and replace `PATH_HERE` with the location of the untared Flink folder.
5. Download the datasets by running `./standalone_input_data_downloader.sh` from the top-level directory.
6. **Compile Ananke** with our compiler script by running `./scripts/compile.sh` from the top-level directory. 
7. Install the plotting requirements by running `pip3 install -r python-requirements.txt` in the top-level directory.

You are now done with the setup. To run a short demonstrator experiment for checking the success of the setup run the following line:

```bash
./scripts/run.sh ./scripts/experiments/setup_exp.sh -d 1 -r 1
```

### Flink Configuration

To make sure all experiments (especially the ones executed the Xeon-Phi server) have the necessary resources to run, edit `$FLINK_DIR/conf/flink-conf.yaml`, where `$FLINK_DIR` is the directory of your flink installation, and change the following attributes:

```yaml
taskmanager.memory.process.size: 32000m
taskmanager.numberOfTaskSlots: 32
```
*Note: Due to the way Flink and Java allocate and garbage-collect memory, the absolute memory consumption in some experiments might differ from the figures in the paper, 
but this configuration ensures that all experiments can be executed successfully. 
Alternatively, you can find the exact flink configuration of each experiment at `reproduce/flink-conf/`*.

## Running Experiments

### Automatic Reproduction of the Paper's Experiments and Plots

Here, we describe how to automatically reproduce the results from our paper on your available hardware.

The folder `reproduce/` contains one bash script labelled as the corresponding figure in the paper. Executing such a script will run the experiment automatically, store the results, and create a plot of them. When reproducing Figure 19 or Table 2, `dockerd` must be running. Simply enter the folder and execute, e.g.

```bash
# Reproduce Figure 10 in the paper
cd reproduce
./figure10.sh
```
For running variations of the experiments and plotting the results, we suggest inspecting the bash scripts in the `reproduce` folder.
Beware that the hardware the experiments were executed on (as indicated in the paper) may differ from yours.
*Note: The Smart Grid dataset cannot be made available due to agreement limitations.*

### Manual Experiment Execution 

Experiment scripts are found in `scripts/experiments` and can be executed by calling `scripts/run.sh` from the top-level directory of the project. The run script takes care of creating output directories based on the commit hash and the date, and it also preprocesses the output after the end of the experiment. It can control maximum duration, number of repetitions, etc. using CLI args. For example:

```bash
# Run lrAnankeCompare (experiment underlying Figure 10) for 10 reps of 10 minutes
./scripts/run.sh ./scripts/experiments/lrAnankeCompare.sh -d 10 -r 10
```
Result files are stored in the folder `data/output`. 

*Note that the experiment scripts are quite verbose and print various debugging information. It should be safe to ignore any warnings or errors as long as the figures are generated successfully.*

