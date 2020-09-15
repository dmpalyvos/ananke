EXTERNAL_INIT_COMMAND="./external/neo4j/init_neo4j"
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
"provenance"
)
# The suffixes for the experiment results folders
# Need to be in the same order as the variants array
variantAbbreviations=(
"ANK-N4J"
)
variantExtraArgs=(
"--provenanceActivator ANANKE_NEO4J"
)
EXPERIMENT_ARGS="--aggregateStrategy sortedPtr --sourceIP ${SOURCE_IP} --sourcePort ${SOURCE_PORT} "
