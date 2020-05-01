#!/bin/bah
INPUT_FILE="/home/common/cats_data/car_local/car_local"
SOURCES_NUMBER="1"
SOURCE_IP="localhost"
SOURCE_PORT=9999
DATASOURCE_COMMAND="java -cp %s io.palyvos.provenance.usecases.cars.local.CarLocalDataProvider -p ${SOURCE_PORT} -h ${SOURCE_IP} -f ${INPUT_FILE}"
queryReference="io.palyvos.provenance.usecases.cars.local.%s.queries.%s"
queries=(
"CarLocalQueries"
)

variants=(
"noprovenance"
"provenance"
"provenance"
"provenance"
"provenance2"
"provenance2"
"provenance2"
)
# The suffixes for the experiment results folders
# Need to be in the same order as the variants array
variantAbbreviations=(
"NP"
"GL1"
"ANK1"
"ANK1S"
"GL2"
"ANK2"
"ANK2S"
)
variantExtraArgs=(
"--sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT}"
"--aggregateStrategy sortedPtr --provenanceActivator GENEALOG --sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT}"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE --sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT}"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE_STD --sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT}"
"--aggregateStrategy sortedPtr --provenanceActivator GENEALOG --sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT}"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE --sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT}"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE_STD --sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT}"
)


