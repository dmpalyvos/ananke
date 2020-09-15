import requests
import time
import re
import signal
from argparse import ArgumentParser
from threading import Thread

IGNORE = 'IGNORE'

METRIC_INIT_RETRIES = 5

ENABLED = True
FLINK_ENDPOINT = 'http://localhost:9999'
SAMPLING_FREQ_SEC = 1

# TODO: Other options for flink latency except latency_mean
METRICS = {
    'numRecordsOutPerSecond': {
        'file': '',
        'operators': [{'name': 'SOURCE', 'type': 'Source'}]
    },
    'latency': {
        'file': '',
        'operators': []
    }
}
# FIXME: Situations as the above (with different types and same names) create problems
#        because not all metrics can be resolved
#  - There should be one configuration (e.g., yaml) per experiment variant to avoid such situations.
#  - The easy way to achieve this is to first read experiment configs from a .yaml file

# Every latency path needs a 'source' and a 'sink'
LATENCY = {'file': '',
        'paths': [{'source': 'SOURCE', 'sink': 'ANOMALY'}, {'source': 'SOURCE', 'sink': 'BLACKOUT'}]}


def interruptHandler(sig, frame):
    global ENABLED
    ENABLED = False


def meanValue(list):
    return sum(list) / float(len(list))


def getRunningJob():
    global jobID
    while ENABLED:
        jobs = requests.get(f'{FLINK_ENDPOINT}/jobs').json()['jobs']
        runningJobs = [job for job in jobs if job['status'] == 'RUNNING']
        if len(runningJobs) > 1:
            raise Exception('Multiple running jobs are not supported!')
        elif len(runningJobs) < 1:
            continue
        elif len(runningJobs) == 1:
            jobID = runningJobs[0]['id']
            return jobID
        time.sleep(5)


'''
    Check for flink (internal) latency metrics, between a source to a sink operator.
    Note that these do not actually need to be Sources and Sinks, they can be any kind of operator.
'''
def filterLatencyMetrics(metricId, source, sink):
    pattern = re.compile(
        f'latency.source_id.{source}.operator_id.{sink}.operator_subtask_index.\d+.latency_mean')
    return pattern.match(metricId)


def filterVerticeMetrics(metricId, operatorName, operatorType, metricName):
    # For blank vertice type (not Source or Sink), metrics have the form
    # 1.operatorName.metricName
    if not operatorType:
        pattern = re.compile(f'\d+\.{operatorName}\.{metricName}')
    else:  # Otherwise they have the form 1.operatorType__operatorName.metricName
        pattern = re.compile(f'\d+\.{operatorType}__{operatorName}\.{metricName}')
    return pattern.match(metricId)


def retrieveVertexID(key, vertices):
    for v in vertices:
        if key in v['name']:
            return v['id']
    print(f'Warning: Could not retrieve vertex ID for {key}')
    return None


def initMetrics(vertices):
    while ENABLED:
        status = True
        jobMetrics = requests.get(f'{FLINK_ENDPOINT}/jobs/{jobID}/metrics').json()
        for latencyPath in LATENCY['paths']:
            status = retrieveLatencyMetricID(jobMetrics, latencyPath, vertices)
        for metricName, metricConfig in METRICS.items():
            for operatorConfig in metricConfig['operators']:
                status = retrieveMetricID(metricName, operatorConfig, vertices)
        if status:
            break
        time.sleep(5)
    print('All metrics initialized')


def retrieveLatencyMetricID(jobMetrics, latencyPath, vertices):
    if ignore(latencyPath):
        return True
    print(f'Initializing latency metric: {latencyPath}')
    latencyPath['sourceID'] = retrieveVertexID(latencyPath['source'], vertices)
    latencyPath['sinkID'] = retrieveVertexID(latencyPath['sink'], vertices)
    if not latencyPath['sourceID'] or not latencyPath['sinkID']:
        latencyPath[IGNORE] = True
        print(f'Ignoring latency metrics for path: {latencyPath}')
        return True
    latencyPath['metrics'] = [m['id'] for m in jobMetrics if
                              filterLatencyMetrics(m['id'],
                                                   latencyPath['sourceID'],
                                                   latencyPath['sinkID'])]
    return hasMetrics(latencyPath)


def retrieveMetricID(metricName, operatorConfig, vertices):
    if ignore(operatorConfig):
        return True
    print(f'Initializing metric {metricName} for {operatorConfig}')
    operatorConfig['verticeID'] = retrieveVertexID(operatorConfig['name'], vertices)
    if not operatorConfig['verticeID']:
        operatorConfig[IGNORE] = True
        print(f'Ignoring metric {metricName} for {operatorConfig["name"]}')
        return True
    # From all the metrics for a whole vertex, select only those for the requested operator
    verticeMetrics = requests.get(
        f'{FLINK_ENDPOINT}/jobs/{jobID}/vertices/{operatorConfig["verticeID"]}/metrics').json()
    operatorType = operatorConfig['type'] if 'type' in operatorConfig else None
    operatorConfig['metrics'] = [m['id'] for m in verticeMetrics if
                                 filterVerticeMetrics(m['id'],
                                                      operatorConfig['name'],
                                                      operatorType,
                                                      metricName)]
    return hasMetrics(operatorConfig)


def hasMetrics(config):
    # If not all metrics could not be retrieved, the job is still starting
    if len(config['metrics']) == 0:
        return False
    print('Metric init success!')
    return True


def ignore(metric):
    #FIXME: Wrong implementation, the script does not work 100% when started before the job
    return IGNORE in metric or ('metrics' in metric and len(metric['metrics']) == 0)


def disabled(metric):
    return ('metrics' not in metric) or ignore(metric)


def writeMetricValues(file, name, metricValues, subtaskIndexPos):
    for metricValue in metricValues:
        taskIndex = int(metricValue["id"].split('.')[subtaskIndexPos])
        file.write(
            f'{name},{taskIndex},{int(time.time())},{float(metricValue["value"])}\n')



if __name__ == '__main__':
    signal.signal(signal.SIGINT, interruptHandler)
    signal.signal(signal.SIGTERM, interruptHandler)
    parser = ArgumentParser()
    parser.add_argument("--throughputFile",
                        help="output file for the throughput",
                        type=str, required=True)
    parser.add_argument("--latencyFile",
                        help="output file for the latency",
                        type=str, required=True)
    args = parser.parse_args()
    jobID = getRunningJob()

    vertices = requests.get(f'{FLINK_ENDPOINT}/jobs/{jobID}').json()['vertices']
    Thread(target=initMetrics, args=(vertices,), daemon=True).start()

    LATENCY['file'] = args.latencyFile
    METRICS['numRecordsOutPerSecond']['file'] = args.throughputFile
    METRICS['latency']['file'] = args.latencyFile

    print(f'Metric recorder started for job: {jobID}')
    while ENABLED:
        start = time.time()
        with open(LATENCY['file'], 'a+') as latencyFile:
            for latencyPath in LATENCY['paths']:
                if disabled(latencyPath):
                    continue
                latencies = requests.get(
                    f'{FLINK_ENDPOINT}/jobs/{jobID}/metrics',
                    params={'get': ','.join(latencyPath['metrics'])}).json()
                writeMetricValues(latencyFile, latencyPath['sink'], latencies, -2)
        for _, metricConfig in METRICS.items():
            with open(metricConfig['file'], 'a+') as metricFile:
                for operatorConfig in metricConfig['operators']:
                    if disabled(operatorConfig):
                        continue
                    metricValues = requests.get(
                        f'{FLINK_ENDPOINT}/jobs/{jobID}/vertices/{operatorConfig["verticeID"]}/metrics',
                        params={
                            'get': ','.join(operatorConfig['metrics'])}).json()
                    writeMetricValues(metricFile, operatorConfig['name'], metricValues, 0)
        duration = time.time() - start
        time.sleep(max(SAMPLING_FREQ_SEC - duration, 0))
    print('Exiting Metric Recorder...')
