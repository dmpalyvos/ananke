source scripts/experiments/template_sg.sh
variants=(
"provenance"
"provenance"
)
# The suffixes for the experiment results folders
# Need to be in the same order as the variants array
variantAbbreviations=(
"ANK1"
"ANK1S"
)
variantExtraArgs=(
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE_STD"
)
EXPERIMENT_ARGS="--graphEncoder TimestampedFileProvenanceGraphEncoder"
