EXTERNAL_INIT_COMMAND="./external/mongo/init_mongo"
source scripts/experiments/template_sg.sh
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
EXPERIMENT_ARGS="--aggregateStrategy sortedPtr"
