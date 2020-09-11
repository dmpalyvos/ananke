source "scripts/experiments/template_synthetic.sh"
# Auto-generated config
variants=(
"provenance"
"provenance"
"provenance"
"provenance"
"provenance"
"provenance"
)
variantAbbreviations=(
"ANK-PG-U-0.1.1.0.10"
"ANK-PG-U-1000.1.1.0.10"
"ANK-PG-U-0.1.1.0.50"
"ANK-PG-U-1000.1.1.0.50"
"ANK-PG-U-0.1.1.0.100"
"ANK-PG-U-1000.1.1.0.100"
)
variantExtraArgs=(
"--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 0 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 10"
"--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 1000 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 10"
"--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 0 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 50"
"--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 1000 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 50"
"--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 0 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 100"
"--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 1000 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 100"
)
EXPERIMENT_ARGS="--syntheticInputLength 50000 --syntheticDelay 25 --syntheticTupleSize 32 --disableSinkChaining --aggregateStrategy sortedPtr --maxParallelism 1"
