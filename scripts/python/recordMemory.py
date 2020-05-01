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
GC_TIME_YOUNG = ['Status.JVM.GarbageCollector.G1_Young_Generation.Time']
GC_TIME_OLD = ['Status.JVM.GarbageCollector.G1_Old_Generation.Time']
GC_COUNT_YOUNG = ['Status.JVM.GarbageCollector.G1_Young_Generation.Count']
GC_COUNT_OLD = ['Status.JVM.GarbageCollector.G1_Old_Generation.Count']

MEGABYTE = 1e6
SAMPLE_FREQUENCY_SECONDS = 1
GC_RELATIVE_SAMPLE_FREQUENCY = 1


def interruptHandler(sig, frame):
    global ENABLED
    print('Exiting Memory Recorder...')
    ENABLED = False


def getSum(jsonEntries, key='value'):
    return sum(float(entry[key]) for entry in jsonEntries)


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
    parser.add_argument("--gcTimeYoungFile",
                        help="output file for the Young GC time",
                        type=str, required=True)
    parser.add_argument("--gcTimeOldFile",
                        help="output file for the Old GC time",
                        type=str, required=True)
    parser.add_argument("--gcCountYoungFile",
                        help="output file for the Young GC counts",
                        type=str, required=True)
    parser.add_argument("--gcCountOldFile",
                        help="output file for the Old GC counts",
                        type=str, required=True)
    args = parser.parse_args()
    taskManagers = getTaskManagers(BASE_URL)
    nodeId = 0
    with open(args.memoryFile, 'w', buffering=1) as memoryFile, \
        open(args.gcTimeYoungFile, 'w', buffering=1) as gcTimeYoungFile, \
        open(args.gcTimeOldFile, 'w', buffering=1) as gcTimeOldFile, \
        open(args.gcCountYoungFile, 'w', buffering=1) as gcCountYoungFile, \
        open(args.gcCountOldFile, 'w', buffering=1) as gcCountOldFile:
        print('Memory Recorder initiated')
        while ENABLED:
            second = int(time.time())
            for tmId, tmPath in taskManagers.items():
                tmURL = '{}/taskmanagers/{}'.format(BASE_URL, tmId)
                memoryConsumption = getSum(
                    getMetricJSON(tmURL, MEMORY_METRICS)) / MEGABYTE
                writeValue(memoryFile, tmPath, memoryConsumption)
                if second % GC_RELATIVE_SAMPLE_FREQUENCY == 0:
                    gcCountYoung = getSum(getMetricJSON(tmURL, GC_COUNT_YOUNG))
                    gcCountOld = getSum(getMetricJSON(tmURL, GC_COUNT_OLD))
                    gcTimeYoung = getSum(getMetricJSON(tmURL, GC_TIME_YOUNG))
                    gcTimeOld = getSum(getMetricJSON(tmURL, GC_TIME_OLD))
                    writeValue(gcCountYoungFile, tmPath, gcCountYoung)
                    writeValue(gcCountOldFile, tmPath, gcCountOld)
                    writeValue(gcTimeYoungFile, tmPath, gcTimeYoung)
                    writeValue(gcTimeOldFile, tmPath, gcTimeOld)
            time.sleep(SAMPLE_FREQUENCY_SECONDS)
