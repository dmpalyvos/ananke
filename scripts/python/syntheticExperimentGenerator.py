INPUT_LENGTH=50000
SOURCE_PARALLELISMS = [1]
SINK_PARALLELISMS = [1]
PROVENANCE_SIZES = [10, 50, 100]
# OVERLAPS = [0, 0.25, 0.50]
OVERLAPS = [0]
# VARIANT_ARGS = {'ANK-PG-U-0': '--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 0 --uniqueDbKeys',
#               'ANK-PG-U-1000': '--provenanceActivator ANANKE_POSTGRES --pollFrequencyMillis 1000 --uniqueDbKeys'}
# VARIANT_ARGS = {'ANK-SL-U-0': '--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 0 --uniqueDbKeys',
#               'ANK-SL-U-1000': '--provenanceActivator ANANKE_SQLITE --pollFrequencyMillis 1000 --uniqueDbKeys'}
# VARIANT_ARGS = {'ANK-MG-0': '--provenanceActivator ANANKE_MONGO --pollFrequencyMillis 0',
#               'ANK-MG-1000': '--provenanceActivator ANANKE_MONGO --pollFrequencyMillis 1000'}
# VARIANT_ARGS = {'ANK-1': '--provenanceActivator ANANKE'}

TEMPLATE_FILE='scripts/experiments/template_synthetic.sh'

variants = []
variantAbbreviations = []
variantExtraArgs = []
EXPERIMENT_ARGS=f'--syntheticInputLength {INPUT_LENGTH} --syntheticDelay 25 --syntheticTupleSize 32 --disableSinkChaining ' \
                  f'--aggregateStrategy sortedPtr '\
                  f'--maxParallelism {max(SOURCE_PARALLELISMS+SINK_PARALLELISMS)}'

for provenanceSize in PROVENANCE_SIZES:
    for overlap in OVERLAPS:
        for sourceParallelism in SOURCE_PARALLELISMS:
            for sinkParallelism in SINK_PARALLELISMS:
                for variantAbbreviation, args in VARIANT_ARGS.items():
                    variants.append('provenance')
                    variantAbbreviations.append(f'{variantAbbreviation}.{sourceParallelism}.{sinkParallelism}.{int(overlap*100)}.{provenanceSize}')
                    variantExtraArgs.append(f'{args} --sinkParallelism {sinkParallelism} ' \
                                                f'--syntheticSourceParallelism {sourceParallelism} ' \
                                                f'--syntheticProvenanceOverlap {int(provenanceSize*overlap)} ' \
                                                f'--syntheticProvenanceSize {provenanceSize}')

print(f'source "{TEMPLATE_FILE}"')
print('# Auto-generated config')
print('variants=(')
for variant in variants:
    print(f'"{variant}"')
print(')')

print('variantAbbreviations=(')
for variantAbbreviation in variantAbbreviations:
    print(f'"{variantAbbreviation}"')
print(')')

print('variantExtraArgs=(')
for variantArgs in variantExtraArgs:
    print(f'"{variantArgs}"')
print(')')

print(f'EXPERIMENT_ARGS="{EXPERIMENT_ARGS}"')