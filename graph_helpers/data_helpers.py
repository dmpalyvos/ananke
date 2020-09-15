from .global_defs import *
from . import config

class ExperimentParameter:
    def __init__(self, key, units, label=None, name=None, timeSeries=True, componentAggregation=np.sum, queryAggregation=np.sum, executionAggregation=np.mean):
        self.key = key
        self.units = units
        self.label = key if label is None else label
        self.name = key if name is None else name
        self.timeSeries = timeSeries
        self.executionAggregation = executionAggregation
        self.queryAggregation = queryAggregation
        self.componentAggregation = componentAggregation
        
    def __str__(self):
        return f'{self.label} ({self.units})'


def loadData(folder):
    
    def removeWarmupCooldown(df):
        tmax = df.t.max()
        warmup = floor(tmax * config.WARMUP_PERCENTAGE)
        cooldown = ceil(tmax - tmax * config.COOLDOWN_PERCENTAGE)
#         print(f'Removing [0, {warmup}) and ({cooldown}, {tmax}]')
        df.loc[(df.t < warmup) | (df.t > cooldown), 'value'] = np.nan
        return df
    
    def subtractMin(df, key):
        df[key] -= df[key].min()
        return df

    def readCsv(file):
        if not file:
            return pd.DataFrame()
        df = pd.read_csv(f'{file}', names=('rep', 'parallelism', 'node', 'idx', 't', 'value'))
        df['rep'] = df['rep'].astype(int)
        df['parallelism'] = df['parallelism'].astype(int)
        df['value'] = df['value'].astype(float)
        df['t'] = df['t'].astype(int)
        df = df.groupby(['rep', 'parallelism']).apply(subtractMin, key='t')
        df = df.groupby(['rep', 'parallelism']).apply(removeWarmupCooldown)
        return df

    config.DATA = None
    print(f'Loading data from {folder}')
    dataFrames = []
    for experimentDir in os.listdir(folder):
        if not os.path.isdir(folder + '/' + experimentDir):
            continue
        experimentName, experimentVariant = experimentDir.split('_')
        for dataFile in glob.glob(folder + '/' + experimentDir + '/' + '*.csv'):
            parameter = dataFile.split('/')[-1].split('.')[0]
            try:
                df = readCsv(dataFile)
            except Exception as e:
#                 print(f'Failed to read {dataFile}')
                continue
            df['parameter'] = parameter
            df['experiment'] = experimentName
            df['variant'] = experimentVariant
            dataFrames.append(df)
    config.DATA = pd.concat(dataFrames, sort=False, ignore_index=True)
    print('Data loaded!')


def preprocess_data():
    
    print('Preprocessing data...')
    config.DATA['kind'] = 'user'
    config.DATA.loc[config.DATA['parameter'].isin(config.SYSTEM_PARAMETERS), 'kind'] = 'system'
    # Extract transparent/non-transparent variant ending in 1 or 2
    config.DATA['transparent'] = config.DATA['variant'].str.contains('[a-zA-Z]+2[a-zA-Z]?$', regex=True)
    config.DATA['variant'] = config.DATA['variant'].str.replace('^GL\d$', 'GL', regex=True)
    config.DATA['variant'] = config.DATA['variant'].str.replace('LIN', 'ANK', regex=True)
    config.DATA['variant'] = config.DATA['variant'].str.replace('^ANK[12]$', 'ANK-1', regex=True)
    config.DATA['variant'] = config.DATA['variant'].str.replace('^ANK[12]S$', 'ANK-N', regex=True)
    config.DATA['variant'] = config.DATA['variant'].str.replace('^ANK\.', 'ANK-1.', regex=True)
    config.DATA['variant'] = config.DATA['variant'].str.replace('^ANKS\.', 'ANK-N.', regex=True)


    config.DATA.loc[config.DATA['value'] < 0, 'value'] = np.nan
    config.DATA.loc[config.DATA['parameter'] == 'latency', 'value'] /= 1e3 # Convert to seconds
    config.DATA.loc[config.DATA['parameter'] == 'deliverylatency', 'value'] /= 1e3 # Convert to seconds


    # Enrich cpu/memory with "external" measurements from top/ps
    is_external = config.DATA['variant'].str.startswith(config.EXTERNAL_VARIANT_PREFIXES)
    print('Enriching external variants:', ', '.join(config.DATA.loc[is_external, 'variant'].unique()))

    config.DATA.loc[config.DATA['parameter'].isin(['externalcpu', 'externalmemory']) & (~ is_external), 'value'] = 0
    config.DATA = config.DATA.set_index(['rep', 'parallelism', 'experiment', 'variant', 'transparent', 't'])

    if 'externalcpu' in config.DATA['parameter'].values:
        config.DATA.loc[(config.DATA['parameter'] == 'cpu'), 'value'] += config.DATA.loc[(config.DATA['parameter'] == 'externalcpu'), 'value']

    if 'externalmemory' in config.DATA['parameter'].values:
        config.DATA.loc[(config.DATA['parameter'] == 'memory'), 'value'] += config.DATA.loc[(config.DATA['parameter'] == 'externalmemory'), 'value']
        
    if 'provreadtime' in config.DATA['parameter'].values:
        config.DATA.loc[config.DATA['parameter'] == 'provreadtime', 'value'] /= 1e3 # Convert to seconds
        config.DATA.loc[config.DATA['parameter'] == 'provwritetime', 'value'] /= 1e3 # Convert end-latency to seconds
        provReadTime = config.DATA[config.DATA['parameter'] == 'provreadtime']
        provWriteTime = config.DATA[config.DATA['parameter'] == 'provwritetime']
        provTime = provReadTime.copy()
        # Provenance reads potentially happen from multiple sinks so we need to take the maximum of all sinks
        provTime['value'] = provReadTime.value.add(provWriteTime.groupby(provWriteTime.index.names).max().value)
        provTime['parameter'] = 'provtime'
        config.DATA = config.DATA.append(provTime)

    if 'provreads' in config.DATA['parameter'].values:
        provReads = config.DATA[config.DATA['parameter'] == 'provreads']
        provWrites = config.DATA[config.DATA['parameter'] == 'provwrites']
        prDf = provWrites.copy()
        prDf['writes'] = provWrites['value']
        prDf['reads'] = provReads['value']
        prDf['value'] = provReads.groupby(provReads.index.names).sum().value / provWrites.groupby(provWrites.index.names).sum().value
        prDf['value'] = prDf.value.replace(np.inf, np.nan) # Drop inf values
        prDf['parameter'] = 'provratio'
        config.DATA = config.DATA.append(prDf)
    config.DATA.reset_index(inplace=True)        

    print(f'=> Commit: {config.COMMIT}')
    print('-'*100)

    print(f'{"Experiment": <20}{"Variant": <20}{"Transparent": <15}{"Reps": <7}{"Parallelism": <15}{"Duration"}')
    print('-'*100)
    for label, group in config.DATA.groupby(['experiment', 'variant', 'transparent', 'parallelism']):
        reps = group.rep.nunique()
        # Get tmax except logical latency, since this has dummy timestamps
        duration = group.query('parameter != "logical-latency"').t.max() / 60 
        print(f'{label[0]: <20}{label[1]: <20}{label[2]: < 15}{reps: <7}{label[3]: <15}{duration:3.1f} min')


def get(**kwargs):
    if len(kwargs) == 0:
        raise ValueError('Need at least one argument!')
    queryParts = []
    for key, value in kwargs.items():
        queryParts.append(f'({key} == "{value}")')
    queryStr = ' & '.join(queryParts)
    return config.DATA.query(queryStr)


def percentageDiff(value, reference):
    return 100*(value - reference) / reference

def get95CI(data):
    return (1.96*np.std(data))/np.sqrt(len(data))

def expandSyntheticCols(df):
    df = df.copy()
    df[['variant', 'sourceParallelism', 'sinkParallelism', 'provenanceOverlap','provenanceSize']] = df.variant.str.split('\.', expand=True)
    df[['sourceParallelism', 'sinkParallelism', 'provenanceOverlap', 'provenanceSize']] = df[['sourceParallelism', 'sinkParallelism', 'provenanceOverlap','provenanceSize']].apply(pd.to_numeric)
    return df

def renameDbVariants(ignore=None):
    
    config.DATA['variant'] = config.DATA['variant'].str.replace('ANK-MG', 'NoSQL')
    config.DATA['variant'] = config.DATA['variant'].str.replace('ANK-PG-U', 'SQL-P')
    config.DATA['variant'] = config.DATA['variant'].str.replace('ANK-SL-U', 'SQL-I')
    config.DATA['variant'] = config.DATA['variant'].str.replace('-0', '/A')
    config.DATA['variant'] = config.DATA['variant'].str.replace('-1000', '/R')
    print('DB variants renamed!')

