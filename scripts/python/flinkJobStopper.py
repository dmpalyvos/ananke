import time
import requests
import signal
from argparse import ArgumentParser
from argparse import ArgumentError
BASE_URL = 'http://localhost:8081'


def interruptHandler(sig, frame):
  global ENABLED
  print('Exiting job stopper...')
  exit(0)


def getSum(jsonEntries, key='value'):
  return sum(float(entry[key]) for entry in jsonEntries)


def getRunningJobs(url):
  requestURL = '{}/jobs'.format(url)
  r = requests.get(requestURL).json()
  jobs = r['jobs']
  running = [job['id'] for job in jobs if job['status'] == 'RUNNING']
  return set(running)

def cancelJob(url, job):
  requestURL = '{}/jobs/{}'.format(url, job)
  print('Canceling job {}'.format(job))
  result = requests.patch(requestURL).json()
  if result:
    print('Result: {}'.format(result))

if __name__ == '__main__':
  signal.signal(signal.SIGINT, interruptHandler)
  signal.signal(signal.SIGTERM, interruptHandler)
  parser = ArgumentParser()
  parser.add_argument("timeout",
                      help="time to wait until jobs are stopped (minutes)",
                      type=float)
  args = parser.parse_args()
  if args.timeout < 0:
    raise ArgumentError('Timeout must be a positive number')
  waitDuration = args.timeout * 60 # Convert to seconds
  time.sleep(waitDuration)
  currentJobs = getRunningJobs(BASE_URL)
  for job in currentJobs:
    cancelJob(BASE_URL, job)

