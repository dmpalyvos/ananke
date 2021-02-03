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
"ANK-SL-U-0.1.1.0.10"
"ANK-SL-U-1000.1.1.0.10"
"ANK-SL-U-0.1.1.0.50"
"ANK-SL-U-1000.1.1.0.50"
"ANK-SL-U-0.1.1.0.100"
"ANK-SL-U-1000.1.1.0.100"
)
variantExtraArgs=(
"--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 0 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 10"
"--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 1000 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 10"
"--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 0 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 50"
"--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 1000 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 50"
"--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 0 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 100"
"--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 1000 --uniqueDbKeys --sinkParallelism 1 --syntheticSourceParallelism 1 --syntheticProvenanceOverlap 0 --syntheticProvenanceSize 100"
)
EXPERIMENT_ARGS="--syntheticInputLength 250000 --syntheticDelay 25 --syntheticTupleSize 32 --disableSinkChaining --aggregateStrategy sortedPtr --maxParallelism 1"
