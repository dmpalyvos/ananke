source "scripts/experiments/template_synthetic.sh"
# Auto-generated config
variants=(
"provenance"
"provenance"
"provenance"
)
variantAbbreviations=(
"ANK-1.1.1.0.10"
"ANK-1.1.1.0.50"
"ANK-1.1.1.0.100"
)
variantExtraArgs=(
"--provenanceActivator ANANKE --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 10"
"--provenanceActivator ANANKE --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 50"
"--provenanceActivator ANANKE --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 100"
)
EXPERIMENT_ARGS="--syntheticInputLength 250000 --syntheticDelay 25 --syntheticTupleSize 32 --disableSinkChaining --aggregateStrategy sortedPtr --maxParallelism 1"
