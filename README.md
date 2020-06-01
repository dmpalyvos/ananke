# Ananke

This repository contains the implementation of the Ananke provenance framework, as well as performance evaluation experiments.

## Instructions

### Input Datasets

- [LinearLoadQueries](https://chalmersuniversity.box.com/s/ioal17insfry4naurtybkp44dxev59ta)
- [Cars #1](https://chalmersuniversity.box.com/s/7s9ewtys69aik5p8bwapbazjs2u9l8vv)
- [Cars #2](https://chalmersuniversity.box.com/s/qzqvlsatyb37a9d3kvfk224ehj0bipki)


### Before Running

- Download and extract [Apache Flink 1.10.0](https://archive.apache.org/dist/flink/flink-1.10.0/)
- Edit `FLINK_DIR` in [scripts/config.sh](./scripts/config.sh)
- Copy (and decompress where needed) all input datasets in `data/input`

### Compiling

The project is compiled by running `./scripts/compile.sh` from the top-level directory. 

### Running Experiments

Experiment scripts are found in `scripts/experiments` and can be executed by calling `scripts/run.sh` from the top-level directory of the project. The run script takes care of creating output directories based on the commit hash and the date, and it also preprocesses the output after the end of the experiment. It can control maximum duration, number of repetitions, etc. using CLI args. For example:

```bash
# Run the lrAnankeCompare experiment for 10 reps of 10 minutes
./scripts/run.sh ./scripts/experiments/lrAnankeCompare.sh -d 10 -r 10
```

#### Experiment Script Description

- Comparison with the state-of-the-art, logical latency: `car_cloud_odroid_logical_latency`, `car_local_odysseus_logical_latency`, `lrAnankeCompare`, `sgAnankeCompare`.
- Synthetic 1 (varying provenance size and overlap): `synthetic1`
- Synthetic 2 (varying #queries and parallelism): `synthetic2`

### Plotting 

The results can be plotted using the Jupyter notebook [graphs](./graphs.ipynb). The notebook contains descriptions about the kind of plot that each cell generates.
