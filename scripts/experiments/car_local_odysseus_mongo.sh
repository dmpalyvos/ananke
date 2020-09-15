INPUT_FILE="/home/common/cats_data/car_local/car_local"
SOURCES_NUMBER="1"
SOURCE_IP="localhost"
SOURCE_PORT=9999
DATASOURCE_COMMAND="java -cp %s io.palyvos.provenance.usecases.cars.local.CarLocalDataProvider -p ${SOURCE_PORT} -h ${SOURCE_IP} -f ${INPUT_FILE}"
EXTERNAL_INIT_COMMAND="./external/mongo/init_mongo"
queryReference="io.palyvos.provenance.usecases.cars.local.%s.queries.%s"
queries=(
"CarLocalQueries"
)
variants=(
"provenance"
"provenance"
)
# The suffixes for the experiment results folders
# Need to be in the same order as the variants array
variantAbbreviations=(
"ANK-MG-0"
"ANK-MG-1000"
)
variantExtraArgs=(
"--provenanceActivator ANANKE_MONGO --pollFrequencyMillis 0"
"--provenanceActivator ANANKE_MONGO --pollFrequencyMillis 1000"
)
EXPERIMENT_ARGS="--aggregateStrategy sortedPtr --sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT} "