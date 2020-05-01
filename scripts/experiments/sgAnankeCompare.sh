#!/bin/bah
source scripts/experiments/template_sg.sh
variants=(
"noprovenance"
"provenance"
"provenance"
"provenance"
"provenance2"
"provenance2"
"provenance2"
)
# The suffixes for the experiment results folders
# Need to be in the same order as the variants array
variantAbbreviations=(
"NP"
"GL1"
"ANK1"
"ANK1S"
"GL2"
"ANK2"
"ANK2S"
)
variantExtraArgs=(
""
"--aggregateStrategy sortedPtr --provenanceActivator GENEALOG"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE_STD"
"--aggregateStrategy sortedPtr --provenanceActivator GENEALOG"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE"
"--aggregateStrategy sortedPtr --provenanceActivator ANANKE_STD"
)
