INPUT_FILE="/home/bastian/data/beijing_allIn1day_ts_sorted.txt"
SOURCES_NUMBER="1"
SOURCE_IP="10.0.0.1"
SOURCE_PORT=9999
DATASOURCE_COMMAND="java -cp %s io.palyvos.provenance.usecases.cars.cloud.CarCloudDataProvider -p ${SOURCE_PORT} -h ${SOURCE_IP} -f ${INPUT_FILE}"
queryReference="io.palyvos.provenance.usecases.cars.cloud.%s.queries.%s"
queries=(
"CarCloudQueries"
)
variants=(
"noprovenance"
"provenance"
"provenance"
"provenance"
)
# The suffixes for the experiment results folders
# Need to be in the same order as the variants array
variantAbbreviations=(
"NP"
"GL1"
"ANK1"
"ANK1S"
)
variantExtraArgs=(
"--sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT}"
"--aggregateStrategy sortedPtr --provenanceActivator GENEALOG --sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT}"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE --sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT}"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE_STD --sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT}"
)
