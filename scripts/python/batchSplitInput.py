import pandas as pd
from math import floor, log2, ceil
import hashlib
from argparse import ArgumentParser
import signal
import sys


def interruptHandler(sig, frame):
  print('Exiting!')
  sys.exit(0)


def extendDataframe(df):
  copy = df.copy()
  copy['t'] += df['t'].max() + 1
  return df.append(copy, ignore_index=True)


def duplicate(df, times):
  if times <= 0:
      return df
  print('Duplicating {} times'.format(times))
  for i in range(times):
    df = extendDataframe(df)
  return df


def describe(df, name):
  minId = df['id'].min()
  maxId = df['id'].max()
  print(
      '- {} has {} IDs [{} - {}] and {} tuples'.format(
        name, df['id'].nunique(), minId, maxId, df.size))
  print(df.head(3))


def convertToFileID(key, parallelism):
  s = str(key)
  return int(hashlib.sha1(s.encode('utf-8')).hexdigest(), 16) % parallelism


DATA_DIR = 'data/input'
EXTENSION = 'txt'
DRY_RUN = False

# SG: timestamp, id, value
# LR: type, time, id, ...
INDEXES = {'lr': (1, 2), 'sg': (0, 1)}

def split(file, tsIndex, idIndex, duplications, minParallelism, maxParallelism):
  assert duplications >= 0
  if DRY_RUN:
      print('> DRY RUN!')
  filePath = '{}/{}.{}'.format(DATA_DIR, file, EXTENSION)
  print('> Reading from {}'.format(filePath))
  df = pd.read_csv(filePath, header=None)
  df = df.rename(columns={df.columns[tsIndex]: 't', df.columns[idIndex]: 'id'})
  df = duplicate(df, duplications)
  startParallelism = ceil(log2(minParallelism)) 
  endParallelism = floor(log2(maxParallelism)) + 1
  parallelisms = [2 ** i for i in range(startParallelism, endParallelism)]
  describe(df, 'Initial Dataframe')
  print('Parallelism splits: {}'.format(parallelisms))
  for parallelism in parallelisms:
    assert parallelism >= 1
    print('> Processing parallelism {}'.format(parallelism))
    for i in range(parallelism):
      df['file'] = df['id'].apply(convertToFileID, parallelism=parallelism)
      split = df[df['file'] == i].drop(columns=['file'])
      name = '{}_{}_{}'.format(file, parallelism, i)
      describe(split, name)
      if not DRY_RUN:
        split.to_csv('{}/{}.txt'.format(DATA_DIR, name), header=False, index=False)
    print('> Done!')
  print('> All done!')


if __name__ == '__main__':
  signal.signal(signal.SIGINT, interruptHandler)
  signal.signal(signal.SIGTERM, interruptHandler)
  parser = ArgumentParser()
  parser.add_argument('file', help='base file to split')
  parser.add_argument('--query', help='query (lr/sg)', type=str, required=True)
  parser.add_argument("--duplications", help="duplications", type=int,
                      default=0)
  parser.add_argument('--minParallelism', help='minimum parallelism',
                      default=1, type=int)
  parser.add_argument('--maxParallelism', help='maximum parallelism',
                      default=32, type=int)
  parser.add_argument('--dry', help='dry run', action='store_true')
  args = parser.parse_args()
  DRY_RUN = args.dry
  split(args.file, INDEXES[args.query][0], INDEXES[args.query][1],
        args.duplications, args.minParallelism, args.maxParallelism)
