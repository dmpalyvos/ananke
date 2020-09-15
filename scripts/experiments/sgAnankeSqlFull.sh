EXTERNAL_INIT_COMMAND="./external/postgres/init_postgres"
source scripts/experiments/template_sg.sh
variants=(
"provenance"
"provenance"
"provenance"
"provenance"
"provenance"
"provenance"
"provenance"
"provenance"
"provenance"
)
# The suffixes for the experiment results folders
# Need to be in the same order as the variants array
variantAbbreviations=(
"ANK1"
"ANK-SL-0"
"ANK-SL-1000"
"ANK-SL-U-0"
"ANK-SL-U-1000"
"ANK-PG-0"
"ANK-PG-1000"
"ANK-PG-U-0"
"ANK-PG-U-1000"
)
variantExtraArgs=(
"--provenanceActivator ANANKE"
"--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 0"
"--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 1000"
"--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 0 --uniqueDbKeys"
"--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 1000 --uniqueDbKeys"
"--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 0"
"--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 1000"
"--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 0 --uniqueDbKeys"
"--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 1000 --uniqueDbKeys"
)
EXPERIMENT_ARGS="--aggregateStrategy sortedPtr"

