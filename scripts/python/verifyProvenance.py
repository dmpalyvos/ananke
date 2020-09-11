import subprocess
import difflib
import re
import glob
from argparse import ArgumentParser

# Environment
BASE_DIR = '/Users/palivosd/Documents/Workspaces/genealog-flink'
DATA_DIR = ''
TEMPDATA_DIR = f'{BASE_DIR}/data/provtests'
FLINK_VERSION = '1.6.1'
JAR = ''
VERBOSE = False
DETAILED_DIFF = False

NOPROVENANCE_VARIANT = ('noprovenance', 'NP')
PROVENANCE_VARIANT = ('genealog', 'GL')
SINK_FILE = 'sink'


def readMultipleFiles(prefix):
  files = glob.glob(prefix)
  if len(files) == 0:
    raise Exception(f'No input files with format {prefix}')
  lines = []
  for file in files:
    with open(file, 'r') as f:
      lines += f.readlines()
  return lines


def isMetadata(line):
  return '{' in line or '}' in line or line.startswith('---')


def onlyTuples(lines):
  return [line.strip() for line in lines if not isMetadata(line)]


def outputFromProvenance(lines):
  result = []
  for line in lines:
    match = re.match(r'^>.*\{(.+)\}.*', line)
    if match:
      result.append(match.group(1))
  return result


def isDiffLine(line):
  return line.startswith('- ') or line.startswith('+ ')


def printSortedDiffs(lines1, lines2, description, sortKey=None):
  diff = difflib.ndiff(sorted(lines1, key=sortKey), sorted(lines2, key=sortKey))
  delta = [line for line in diff if isDiffLine(line)]
  if DETAILED_DIFF:
    print('\n'.join(delta))
  nDiffs = len(delta)
  if len(lines1) == 0:
    print(f'> Left file empty for {description}')
  elif len(lines2) == 0:
    print(f'> Right file empty for {description}')
  else:
    print(f'> {len(delta)} differences between {description}')


def runFlinkQuery(query, statisticsFolder, inputFile, outputFile,
    parallelism=1):
  result = subprocess.run([f'{BASE_DIR}/flink-{FLINK_VERSION}/bin/flink',
                           'run',
                           '--parallelism', str(parallelism),
                           '--class', query,
                           JAR,
                           '--statisticsFolder', statisticsFolder,
                           '--inputFile', inputFile,
                           '--outputFile', outputFile
                           ], stdout=subprocess.PIPE)
  if VERBOSE:
    print(' '.join(result.args))
  if VERBOSE or result.returncode != 0:
    print(f'return code = {result.returncode}')
    print(str(result.stdout, 'ascii'))


def sgSortKey(line):
  parts = line.split(',')
  return (int(parts[0]), int(parts[1]))


def lrSortKey(line):
  parts = line.split(',')
  return int(parts[1])


def joinWithNones(char, items):
  return char.join(item for item in items if item)


def experimentName(queryName, variant, experiment):
  return joinWithNones('_', [queryName, variant, experiment])


# Query-specific config
QUERIES = {
  'sg': {
    'blackout': (
      'flink.genealog.usecases.smartgrid.{QUERY_TYPE}.queries.SmartGridBlackout',
      sgSortKey),
    'anomaly': (
      'flink.genealog.usecases.smartgrid.{QUERY_TYPE}.queries.SmartGridAnomaly',
      sgSortKey),
  },
  'lr': {
    'accident': (
      'flink.genealog.usecases.linearroad.{QUERY_TYPE}.queries.LinearRoadAccident',
      lrSortKey),
    'stopped': (
      'flink.genealog.usecases.linearroad.{QUERY_TYPE}.queries.LinearRoadStoppedVehicles',
      lrSortKey)
  }
}


def verifyProvenance(commit, rep, parallelism, experiment, queryClass,
    tupleSortKey):
  # Helper variables
  noProvenanceQueryClass = queryClass.format(QUERY_TYPE=NOPROVENANCE_VARIANT[0])
  provenanceQueryClass = queryClass.format(QUERY_TYPE=PROVENANCE_VARIANT[0])
  queryName = noProvenanceQueryClass.split('.')[-1]  # Query name
  provenanceExperimentName = experimentName(queryName, PROVENANCE_VARIANT[1],
                                            experiment)
  noProvenanceExperimentName = experimentName(queryName,
                                              NOPROVENANCE_VARIANT[1],
                                              experiment)

  # File Paths
  PROVENANCE_FILE = f'{DATA_DIR}/{commit}/{provenanceExperimentName}/{rep}_{parallelism}/{SINK_FILE}_*.out'
  NOPROVENANCE_FILE = f'{DATA_DIR}/{commit}/{noProvenanceExperimentName}/{rep}_{parallelism}/{SINK_FILE}_*.out'
  # The path to the provenance output converted to input for verification
  PROVENANCE_INPUT_FILE = f'{TEMPDATA_DIR}/{provenanceExperimentName}_input'
  # Temporary output files
  OUTPUT_FILE_NOPROV_COMPARISON = f'{provenanceExperimentName}_provtest_1'
  OUTPUT_FILE_VARIANT_COMPARISON = f'{provenanceExperimentName}_provtest_2'

  print('+ Validating Provenance for query [{}]'.format(
      joinWithNones('/', [queryName, experiment, commit, str(rep),
                          str(parallelism)])))
  if VERBOSE:
    print(f'Provenance data: {PROVENANCE_FILE}')
    print(f'Original output: {NOPROVENANCE_FILE}')

  # Preprocess provenance and save as input for verification
  rawProvenance = readMultipleFiles(PROVENANCE_FILE)
  provenance = onlyTuples(rawProvenance)
  with open(f'{PROVENANCE_INPUT_FILE}_1_0.txt', 'w') as out:
    # Sort based on query-specific key (i.e., timestamp)
    for line in sorted(set(provenance), key=tupleSortKey):
      out.write(line + '\n')

  ##################
  # Run and compare
  ##################

  # Compare original output with provenance output tuples
  # Initial output of the noprovenance query
  noProvenanceOutput = onlyTuples(readMultipleFiles(NOPROVENANCE_FILE))
  provenanceOutput = outputFromProvenance(rawProvenance)
  printSortedDiffs(noProvenanceOutput, provenanceOutput,
                   'NO-PROVENANCE OUTPUT <-> PROVENANCE OUTPUT')

  # Compare original output with repeated output using provenance as input
  runFlinkQuery(noProvenanceQueryClass, TEMPDATA_DIR, PROVENANCE_INPUT_FILE,
                OUTPUT_FILE_NOPROV_COMPARISON)

  # Output of the noproveance query using the provenance data as input
  repeatedOutput = onlyTuples(
      readMultipleFiles(
          f'{TEMPDATA_DIR}/{OUTPUT_FILE_NOPROV_COMPARISON}_*.out'))
  printSortedDiffs(noProvenanceOutput, repeatedOutput,
                   'NO-PROVENANCE OUTPUT <-> REPEATED OUTPUT (using provenance as input)')

  runFlinkQuery(provenanceQueryClass, TEMPDATA_DIR, PROVENANCE_INPUT_FILE,
                OUTPUT_FILE_VARIANT_COMPARISON)
  # Provenance output when provenance query was run with provenance data as input
  repeatedProvenance = onlyTuples(
      readMultipleFiles(
          f'{TEMPDATA_DIR}/{OUTPUT_FILE_VARIANT_COMPARISON}_*.out'))
  printSortedDiffs(provenance, repeatedProvenance,
                   'INITIAL PROVENANCE <-> REPEATED PROVENANCE')


def verifyAll(commit, rep, parallelism, experiment):
  for queryCategory in QUERIES.values():
    for query in queryCategory.values():
      try:
        verifyProvenance(commit, rep, parallelism, experiment, *query)
      except Exception as e:
        print('[ERROR]', e)
      print('--------------------------')


if __name__ == '__main__':
  parser = ArgumentParser()
  parser.add_argument('commit', help='commit and time code', type=str)
  parser.add_argument('--parallelism', help='parallelism of experiment',
                      type=int, default=1)
  parser.add_argument('--experiment',
                      help='specific experiment type, e.g., DISTRIBUTED',
                      type=str)
  parser.add_argument("--rep", help="repetition id", type=int,
                      default='1')
  parser.add_argument('--data', help='directory of the query outputs', type=str,
                      default='data/output')
  parser.add_argument("--verbose", help="display detailed output",
                      action="store_true")
  parser.add_argument("--diff", help="display detailed diffs",
                      action="store_true")
  args = parser.parse_args()
  VERBOSE = args.verbose
  DETAILED_DIFF = args.diff
  DATA_DIR = args.data
  commitHash = subprocess.check_output('git rev-parse --short HEAD'.split(),
                                       encoding='utf-8').strip()
  print(f'Using genealog-flink {commitHash}')
  JAR = f'{BASE_DIR}/target/genealog-flink-{commitHash}.jar'
  verifyAll(args.commit, args.rep, args.parallelism, args.experiment)
