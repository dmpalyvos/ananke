#!/bin/bah
EXTERNAL_INIT_COMMAND="./external/mongo/init_mongo"
source scripts/experiments/template_lr.sh
variants=(
"provenance"
"provenance"
"provenance"
)
# The suffixes for the experiment results folders
# Need to be in the same order as the variants array
variantAbbreviations=(
"ANK-1"
"ANK-MG-0"
"ANK-MG-1000"
)
variantExtraArgs=(
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE_MONGO --pollFrequencyMillis 0"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE_MONGO --pollFrequencyMillis 1000"
)
