import glob
import pandas as pd
from argparse import ArgumentParser

SINK_FILE = 'sink'

def getTimestamp(line, tsIndex):
  return int(line.split(',')[tsIndex])

def joinWithNones(char, items):
  return char.join(item for item in items if item)

def verifyOutputOrdering(args):
  queryFolder = joinWithNones('_', [args.query, 'NP', args.experiment])
  filePrefix = f'{args.data}/{args.commit}/{queryFolder}/{args.rep}/{SINK_FILE}_*.out'
  files = glob.glob(filePrefix)
  if len(files) == 0:
    raise Exception(f'No output files with format {filePrefix}')
  for file in files:
    with open(file, 'r') as f:
      lines = f.readlines()
      ts = -1
      for lineNo, line in enumerate(lines):
        if line.startswith('---'):
          continue
        if ts > getTimestamp(line, args.ts):
          print(f'[ERROR] Decreasing timestamp in File {file}, L{lineNo}')


if __name__ == '__main__':
  parser = ArgumentParser()

  parser.add_argument('--query', help='query name', type=str, required=True)
  parser.add_argument('--commit', help='commit and time code', type=str,
                      required=True)
  parser.add_argument('--ts', help='timestamp index', type=int,
                      required=True)

  parser.add_argument("--rep", help="repetition id", type=int,
                      default='1')
  parser.add_argument('--experiment',
                      help='specific experiment type, e.g., DISTRIBUTED',
                      type=str, default=None)
  parser.add_argument('--data', help='directory of the query outputs', type=str,
                      default='data/output')
  args = parser.parse_args()
  verifyOutputOrdering(args)
