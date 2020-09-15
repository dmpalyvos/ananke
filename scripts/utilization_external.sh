#!/usr/bin/env bash

[[ -n $1 ]] || { echo "usage: $0 cpu_file memory_file"; exit 1; }
[[ -n $2 ]] || { echo "usage: $0 cpu_file memory_file"; exit 1; }

NUMBER_CPUS=$(nproc)
TOTAL_MEMORY=$(awk '/MemTotal/ {print $2 / 1024}' /proc/meminfo)
CPU_FILE=$1
MEM_FILE=$2

while sleep 1; do
  CPUMEM=$(top -b -n 1 -p"$(pgrep -d, -f 'postgres|mongo|neo4j')" 2> /dev/null)
  CPUMEM=$(echo "$CPUMEM" | awk -v ncpu="$NUMBER_CPUS" -v nmem="$TOTAL_MEMORY" 'BEGIN{ cpu = 0; memory = 0; } NR > 7 { cpu+=$9; memory += $10 } END { print cpu/ncpu, memory*nmem/100; }')
  CPU=$(echo "$CPUMEM" | cut -d' ' -f1)
  MEM=$(echo "$CPUMEM" | cut -d' ' -f2)
  TIME_SECONDS=$(date +%s)
  echo "LOCAL,0,$TIME_SECONDS,$CPU" >> $CPU_FILE
  echo "LOCAL,0,$TIME_SECONDS,$MEM" >> $MEM_FILE
done