#!/usr/bin/env bash

# CLI ARGS

usage() { echo "Usage: $0 EXPERIMENT_SCRIPT [-d <duration > 0>] [-e <server|odroid>] [-c <commit_code>] [-p <parallelism > 0>] [-r <reps > 0>] [-v <variant_abbreviation>] [-g]" 1>&2; exit 1; }

function configError() {
  echo "Error in experiment configuration: $1"
  exit 1
}

# Experiment script
EXPERIMENT_SCRIPT=$1
if [ -z "$EXPERIMENT_SCRIPT" ]; then
  usage
fi
if [ ! -e "$EXPERIMENT_SCRIPT" ]; then
  echo "Experiment file $1 does not exist!"
  exit 1
fi
shift 


DURATION="1"
REPS="1"
COMMIT_CODE=$(date +%j_%H%M)
[[ $(hostname) = odroid* ]] && EXECUTION_ENVIRONMENT="odroid" || EXECUTION_ENVIRONMENT="normal"

while getopts ":d:e:v:c:p:r:h:g" o; do
    case "${o}" in
        d)
           DURATION=${OPTARG}
           (($(echo "$DURATION > 0" | bc -l))) || usage
           ;;
        c) 
           COMMIT_CODE=${OPTARG}
           ;;
        e)
           EXECUTION_ENVIRONMENT=${OPTARG}
           [[ -n $EXECUTION_ENVIRONMENT ]] || usage
           ;;
        p)
           ARG_PARALLELISM=${OPTARG}
           ((ARG_PARALLELISM > 0)) || usage
           ;;
        r) 
           REPS=${OPTARG}
           ((REPS > 0)) || usage
           ;;
        v)
          FILTER_VARIANT=${OPTARG}
          [[ -n "$FILTER_VARIANT" ]] || usage
          ;;
        g)
          DISTRIBUTED="--distributed"
          ;;
        x)
          DISCARD_OUTPUT="true"
          ;;
        h)
          usage
          ;;
        *)
          break
          ;;
    esac
done
shift $((OPTIND-1))
# Additional CLI args that can be passed to the experiment (after the script args!)
CLI_ARGS="$*"



# Load config file, defining $BASE_DIR and $FLINK_DIR
CONFIG_FILE="scripts/config.sh"
source "$CONFIG_FILE"
# Commit number + unique identifier to separate executions
COMMIT_HASH=$(git rev-parse --short HEAD)
COMMIT="${COMMIT_HASH}_${COMMIT_CODE}"
INPUT_FOLDER="data/input"
OUTPUT_FOLDER="data/output/${COMMIT}"
SINK_FILE="sink"
EXTRA_SLEEP_SEC=15
JAR="$BASE_DIR/target/genealog-flink-experiments-${COMMIT_HASH}.jar"
MEMORY_LOGGER="$BASE_DIR/scripts/python/recordMemory.py"
UTILIZATION_LOGGER="$BASE_DIR/scripts/python/utilization.py"
FLINK_JOB_STOPPER="$BASE_DIR/scripts/python/flinkJobStopper.py"
MEMORY_FILE="memory.csv"
CPU_FILE="cpu.csv"
EXTERNAL_MEMORY_FILE="externalmemory.csv"
EXTERNAL_CPU_FILE="externalcpu.csv"
GC_COUNT_YOUNG_FILE="gc_count_young.csv"
GC_COUNT_OLD_FILE="gc_count_old.csv"
GC_TIME_YOUNG_FILE="gc_time_young.csv"
GC_TIME_OLD_FILE="gc_time_old.csv"
FORCE_GC_CMD="jcmd | grep org.apache.flink.runtime.taskexecutor.TaskManagerRunner | cut -d ' ' -f 1 | xargs -I {} jcmd {} GC.run"
FORCE_TASKSET="jcmd | grep flink | cut -d ' ' -f 1 | xargs -I {} taskset -apc 4-7 {}"
parallelisms=(1) # Default parallelism values, overwritten by specific scripts
sourceReps=(1) # Source repetition values, overwritten by specific scripts
# List of nodes running remote task managers
# Used to copy results
mapfile -t nodes < "$FLINK_DIR/conf/slaves"


# Load experiment Data
echo "[*] Running $EXPERIMENT_SCRIPT using commit $COMMIT"
echo "[*] Additional Experiment CLI Arguments: $EXPERIMENT_ARGS"
source "$EXPERIMENT_SCRIPT"

# Overwrite parameters from CLI arguments
[[ -n "${ARG_PARALLELISM}" ]] && parallelisms=("$ARG_PARALLELISM")

# Verify configuration
[[ -e "$FLINK_DIR" ]] || configError "Flink Directory"
[[ "${#variants[@]}" == "${#variantAbbreviations[@]}" ]] || configError "variantAbbreviations"
[[ "${#variants[@]}" == "${#variantExtraArgs[@]}" ]] || configError "variantExtraArgs"


if [[ -n $DATASOURCE_COMMAND ]]; then
  DATASOURCE_COMMAND=$(printf "$DATASOURCE_COMMAND" "$JAR")
  echo "Datasource command:"
  echo $DATASOURCE_COMMAND
fi


function executeForAllNodes {
  for node in "${nodes[@]}"; do
    if [[ $node == "$(hostname)" || $node == "localhost" ]]; then
      eval "$1" > /dev/null
    else
      ssh "$node" "$1" > /dev/null
    fi
  done
}

function forceTaskset {
  echo "[$(date +%Y-%m-%d\ %H:%M:%S)] Forcing TaskSets on Flink..."
  executeForAllNodes "${FORCE_TASKSET}"
}

function restartCluster {
  "$FLINK_DIR"/bin/stop-cluster.sh
  sleep 30
  "$FLINK_DIR"/bin/start-cluster.sh
  sleep 30
  [[ $EXECUTION_ENVIRONMENT == "odroid" ]] && forceTaskset
  sleep 5
}

# Exit Actions
DONE=0
activeProcesses=()

clearActiveProcs() {
  for proc in "${activeProcesses[@]}"; do
    kill "${proc}"
  done
  python3 "${FLINK_JOB_STOPPER}" 0
  activeProcesses=()
}

on_exit() {
  clearActiveProcs
  DONE=1
}

trap on_exit EXIT

# Odroid specific startup actions
[[ $EXECUTION_ENVIRONMENT == "odroid" ]] && forceTaskset

# Print experiment info
[[ -z $FILTER_VARIANT ]] && numberOfVariants="${#variants[@]}" || numberOfVariants=1
experimentCount=$(( $REPS * ${#parallelisms[@]} * ${#queries[@]} * $numberOfVariants ))
approximateDuration=$(bc <<< " $experimentCount * (${DURATION} + 1)")
echo "---- Experiment Info ----"
echo "> Execution environment: ${EXECUTION_ENVIRONMENT}"
echo "> Repetitions: ${REPS}"
echo "> Parallelism levels ${parallelisms[@]}"
echo "> Queries: ${queries[@]}"
[[ -z $FILTER_VARIANT ]] && echo "> Variants: ${variantAbbreviations[@]}" || echo "> Variants: $FILTER_VARIANT"
[[ -z $DISTRIBUTED ]] || echo "> Distributed execution enabled"
echo "> For ${DURATION} min per run, this experiment will take approx. ${approximateDuration} minutes"
echo "-------------------------"


sleep 2
experimentIndex=0
# # Run
for rep in $(seq 1 $REPS); do
  for query in "${queries[@]}"; do
    for variantID in "${!variants[@]}"; do
      for parallelismId in "${!parallelisms[@]}"; do
        # Compute experiment variables
        parallelism=${parallelisms[${parallelismId}]}
        sourceRep=${sourceReps[${parallelismId}]}
        variant=${variants[$variantID]}
        abbreviation=${variantAbbreviations[$variantID]}
        experiment=$(printf "$queryReference" "$variant" "$query")
        shortName="${query}_${abbreviation}"
        statisticsFolder="$OUTPUT_FOLDER/$shortName/${rep}_${parallelism}"
        inputPath="${BASE_DIR}/${INPUT_FOLDER}/${INPUT_FILE}"
        flinkCmd="${FLINK_DIR}/bin/flink run \
        --class $experiment --parallelism $parallelism $JAR \
        --statisticsFolder "$BASE_DIR/$statisticsFolder" \
        --inputFile ${inputPath} \
        --outputFile $SINK_FILE \
        --sourceRepetitions ${sourceRep} \
        --sourcesNumber ${SOURCES_NUMBER} \
        --autoFlush \
        $DISTRIBUTED \
        ${variantExtraArgs[$variantID]} \
        $EXPERIMENT_ARGS $CLI_ARGS"

        if [[ ! $flinkCmd =~ "sinkParallelism" ]]; then
          flinkCmd="$flinkCmd --sinkParallelism $parallelism"
        fi

        if [[ -n $FILTER_VARIANT && $FILTER_VARIANT != "$abbreviation" ]]; then
          continue
        fi


        restartCluster
        (( experimentIndex++ ))


        commandExecuted=false
        while [[ $commandExecuted == "false" ]]; do
          commandExecuted=true

          echo --------------------------
          echo ">>>>> [$(date +%Y-%m-%d\ %H:%M:%S)] Initiating experiment ${COMMIT} ${experimentIndex}/${experimentCount} <<<<<"
          echo --------------------------

          # GC to clear up memory statistics from previous experiment
          echo "Forcing GC on TaskManagers..."
          executeForAllNodes "${FORCE_GC_CMD}"
          sleep 5

          # Create output directories
          executeForAllNodes "mkdir -p $BASE_DIR/$statisticsFolder"
          # Create always for localhost, even if not in NODES
          mkdir -p "$BASE_DIR/$statisticsFolder"
          # Clean up old data
          executeForAllNodes "rm $BASE_DIR/$statisticsFolder/* > /dev/null 2>&1"
          echo
          echo "[$(date +%Y-%m-%d\ %H:%M:%S)] Executing $experiment/${abbreviation}/parallelism ${parallelism}/rep ${rep}"
          echo
          # Run (optional) init command
          eval "$EXTERNAL_INIT_COMMAND"
          # Start (optional) data source process
          if [[ -n $DATASOURCE_COMMAND ]]; then
            eval "$DATASOURCE_COMMAND &"
            activeProcesses+=("$!")
            sleep 5
          fi
          # Start memory logger
          python3 "$UTILIZATION_LOGGER" \
          --memoryFile "$BASE_DIR/$statisticsFolder/$MEMORY_FILE" \
          --cpuFile "$BASE_DIR/$statisticsFolder/$CPU_FILE" &
          activeProcesses+=("$!")
          # Start external utilization logger
          ./scripts/utilization_external.sh "$BASE_DIR/$statisticsFolder/$EXTERNAL_CPU_FILE" "$BASE_DIR/$statisticsFolder/$EXTERNAL_MEMORY_FILE" &
          activeProcesses+=("$!")
          # Start Task Stopper and then Experiment
          python3 "${FLINK_JOB_STOPPER}" "${DURATION}" & # Task stopper must be started after flink job
          activeProcesses+=("$!")
          echo "${flinkCmd}"
          # Run flink job
          flinkOutput=$(${flinkCmd} 2>&1)
          flinkExitCode=$?
          clearActiveProcs
          # Check if there was a cancellation or a crash
          if [[ $flinkExitCode -ne 0 ]]; then
            if [[ $flinkOutput == *JobCancellationException* ]]; then
              echo "Flink Job Cancelled"
            else
              echo "$flinkOutput"
              echo "Flink job crashed. (Exit Code: $flinkExitCode)"
              commandExecuted=false
              restartCluster
              echo ">>> [WARNING] Retrying experiment..."
            fi
          fi
          # If DONE was set by the exit handler, remove trap and end loop
          (( DONE > 0 )) && break
        done
        echo "[$(date +%Y-%m-%d\ %H:%M:%S)] Experiment OK!"
        sleep ${EXTRA_SLEEP_SEC}
      done
    done
  done
done

sleep 30

# Copy results from remote nodes
for node in "${nodes[@]}"; do
  if [[ $node == "$(hostname)" || ${node} == "localhost" ]]; then
    continue
  fi
  echo "[*] Copying results from $node"
  scp -r "${node}:${BASE_DIR}/${OUTPUT_FOLDER}" "data/output"
done

if [[ $DISCARD_OUTPUT == "true" ]]; then
  find "$OUTPUT_FOLDER" -name logical-latency.csv -delete
  find "$OUTPUT_FOLDER" -name *.out -delete
fi

# Preprocess results
bash "scripts/preprocess.sh" "$OUTPUT_FOLDER"

echo "[*] Finished experiment using commit ${COMMIT}"
