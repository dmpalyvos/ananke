import time
import requests
import signal
import re
from argparse import ArgumentParser

ENABLED = True
BASE_URL = 'http://localhost:8081'
MEMORY_METRICS = ['Status.JVM.Memory.Heap.Used',
                  'Status.JVM.Memory.NonHeap.Used',
                  'Status.JVM.Memory.Mapped.MemoryUsed',
                  'Status.JVM.Memory.Direct.MemoryUsed']
CPU_METRIC = 'Status.JVM.CPU.Load'

MEGABYTE = 1e6
SAMPLE_FREQUENCY_SECONDS = 1
GC_RELATIVE_SAMPLE_FREQUENCY = 1


def interruptHandler(sig, frame):
    global ENABLED
    print('Exiting Utilization Recorder...')
    ENABLED = False


def aggregate(jsonEntries, key='value', function=sum):
    return function(float(entry[key]) for entry in jsonEntries)


def getMetricJSON(url, metrics):
    metricRequest = ','.join(metrics)
    requestURL = '{}/metrics?get={}'.format(url, metricRequest)
    r = requests.get(requestURL)
    return r.json()


def cleanupTaskManagerPath(path):
    return re.match(r'.*\/\/([^\/]+)\/.*', path).group(1)


def getTaskManagers(url):
    taskManagers = requests.get('{}/taskmanagers'.format(url)).json()
    return {tm['id']: cleanupTaskManagerPath(tm['path']) for tm in
            taskManagers['taskmanagers']}


def writeValue(file, tm, value):
    file.write(
        '{},0,{},{}\n'.format(tm, int(time.time()), value))


if __name__ == '__main__':
    signal.signal(signal.SIGINT, interruptHandler)
    signal.signal(signal.SIGTERM, interruptHandler)
    parser = ArgumentParser()
    parser.add_argument("--memoryFile",
                        help="output file for the memory consumption",
                        type=str, required=True)
    parser.add_argument("--cpuFile",
                        help="output file for the cpu utilization",
                        type=str, required=True)
    args = parser.parse_args()
    taskManagers = getTaskManagers(BASE_URL)
    with open(args.memoryFile, 'w') as memoryFile, \
        open(args.cpuFile, 'w') as cpuFile:
        print('Utilization Recorder initiated')
        taskManagers = getTaskManagers(BASE_URL)
        while ENABLED:
            second = int(time.time())
            for tmId, tmPath in taskManagers.items():
                tmURL = '{}/taskmanagers/{}'.format(BASE_URL, tmId)
                memoryConsumption = aggregate(getMetricJSON(tmURL, MEMORY_METRICS)) / MEGABYTE
                writeValue(memoryFile, tmPath, memoryConsumption)
                cpuUtilization = aggregate(getMetricJSON(tmURL, [CPU_METRIC])) * 100
                writeValue(cpuFile, tmPath, cpuUtilization)
            time.sleep(SAMPLE_FREQUENCY_SECONDS)
