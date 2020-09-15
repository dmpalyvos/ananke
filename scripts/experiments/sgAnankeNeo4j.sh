EXTERNAL_INIT_COMMAND="./external/neo4j/init_neo4j_odroid"
source scripts/experiments/template_sg.sh
variants=(
"provenance"
)
# The suffixes for the experiment results folders
# Need to be in the same order as the variants array
variantAbbreviations=(
"ANK-N4J"
)
variantExtraArgs=(
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE_NEO4J"
)

