import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from matplotlib import lines, markers
import seaborn as sns
import numpy as np
import os
import glob 
import re
from collections import defaultdict
from scipy import stats
import subprocess
from itertools import cycle
from math import floor, ceil, sqrt
import argparse
plt.style.use('ggplot')
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42
matplotlib.rcParams['hatch.linewidth'] = 0.2
matplotlib.rcParams['xtick.labelsize'] = 10
sns.set_palette(sns.color_palette('Set2', n_colors=14, desat=0.9))

parser = argparse.ArgumentParser(description='Create Ananke plots.')
parser.add_argument("outputDirectory", action="store")
parser.add_argument("commitCode", action="store")
parser.add_argument("figureCode", action="store")
parser.add_argument("figureName", action="store")

args = parser.parse_args()

OUTPUT_DIRECTORY=args.outputDirectory
COMMIT =  args.commitCode
FIGURE_CODE = args.figureCode
FIGURE_NAME = args.figureName


OUTPUT_DIRECTORY="/home/bhavers/repos/ananke/genealog-flink-experiments/data/output"
COMMIT = "b3c7aac_224_1502"
FIGURE_CODE = 'lr'
FIGURE_NAME = "DUMMY_FIGURE_NAME"
SOA_COMP = 2


# ---------------------------------------------------

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

# ---- DATA LOADING ---------------------------------


WARMUP_PERCENTAGE = 0.1
COOLDOWN_PERCENTAGE = 0.1
REPORT_FOLDER = f'{OUTPUT_DIRECTORY}/{COMMIT}'
DATA = None
    
        
def loadData(folder):
    global DATA
    
    def removeWarmupCooldown(df):
        tmax = df.t.max()
        warmup = floor(tmax * WARMUP_PERCENTAGE)
        cooldown = ceil(tmax - tmax * COOLDOWN_PERCENTAGE)
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

    dataFrames = []
    for experimentDir in os.listdir(REPORT_FOLDER):
        if not os.path.isdir(REPORT_FOLDER + '/' + experimentDir):
            continue
        experimentName, experimentVariant = experimentDir.split('_')
        for dataFile in glob.glob(REPORT_FOLDER + '/' + experimentDir + '/' + '*.csv'):
            parameter = dataFile.split('/')[-1].split('.')[0]
            try:
                df = readCsv(dataFile)
            except Exception as e:
                print(f'Failed to read {dataFile}')
                continue
            df['parameter'] = parameter
            df['experiment'] = experimentName
            df['variant'] = experimentVariant
            dataFrames.append(df)
    DATA = pd.concat(dataFrames, sort=False, ignore_index=True)

loadData(REPORT_FOLDER)





# ---- DATA WRANGLING --------------------------------------------------------

def get(**kwargs):
    if len(kwargs) == 0:
        raise ValueError('Need at least one argument!')
    queryParts = []
    for key, value in kwargs.items():
        queryParts.append(f'({key} == "{value}")')
    queryStr = ' & '.join(queryParts)
    return DATA.query(queryStr)


def percentageDiff(value, reference):
    return 100*(value - reference) / reference

def get95CI(data):
    return (1.96*np.std(data))/np.sqrt(len(data))

def expandSyntheticCols(df):
    df = df.copy()
    df[['variant', 'sourceParallelism', 'sinkParallelism', 'provenanceOverlap','provenanceSize']] = df.variant.str.split('\.', expand=True)
    df[['sourceParallelism', 'sinkParallelism', 'provenanceOverlap', 'provenanceSize']] = df[['sourceParallelism', 'sinkParallelism', 'provenanceOverlap','provenanceSize']].apply(pd.to_numeric)
    return df

    
SYSTEM_PARAMETERS = ['gc_count_old', 'gc_count_young', 'gc_time_old', 'gc_time_young', 'memory']
DATA['kind'] = 'user'
DATA.loc[DATA['parameter'].isin(SYSTEM_PARAMETERS), 'kind'] = 'system'
# Extract transparent/non-transparent variant ending in 1 or 2
DATA['transparent'] = DATA['variant'].str.contains('[a-zA-Z]+2[a-zA-Z]?$', regex=True)
DATA['variant'] = DATA['variant'].str.replace('^GL\d$', 'GL', regex=True)
DATA['variant'] = DATA['variant'].str.replace('LIN', 'ANK', regex=True)
DATA['variant'] = DATA['variant'].str.replace('^ANK[12]$', 'ANK-1', regex=True)
DATA['variant'] = DATA['variant'].str.replace('^ANK[12]S$', 'ANK-N', regex=True)
# TEMP FOR POSTGRES - FIXME
DATA['variant'] = DATA['variant'].str.replace('^ANK[12]PG$', 'ANK-PG', regex=True)
DATA['variant'] = DATA['variant'].str.replace('^ANK[12]PGU$', 'ANK-PG-U', regex=True)
# TEMP FOR POSTGRES - FIXME END
DATA['variant'] = DATA['variant'].str.replace('^ANK\.', 'ANK-1.', regex=True)
DATA['variant'] = DATA['variant'].str.replace('^ANKS\.', 'ANK-N.', regex=True)


DATA.loc[DATA['value'] < 0, 'value'] = np.nan
DATA.loc[DATA['parameter'] == 'latency', 'value'] /= 1e3 # Convert latency to seconds

if 'endlatency' in DATA['parameter'].values:
    DATA.loc[DATA['parameter'] == 'endlatency', 'value'] /= 1e3 # Convert end-latency to seconds

# Enrich cpu/memory with "external" measurements from top/ps
DATA = DATA.set_index(['rep', 'parallelism', 'experiment', 'variant', 't'])

if 'externalcpu' in DATA['parameter'].values:
    DATA.loc[(DATA['parameter'] == 'cpu'), 'value'] += DATA.loc[(DATA['parameter'] == 'externalcpu'), 'value']

if 'externalmemory' in DATA['parameter'].values:
    DATA.loc[(DATA['parameter'] == 'memory'), 'value'] += DATA.loc[(DATA['parameter'] == 'externalmemory'), 'value']
DATA.reset_index(inplace=True)        

print(f'=> Commit: {COMMIT}')
print('-'*100)

print(f'{"Experiment": <20}{"Variant": <20}{"Transparent": <15}{"Reps": <7}{"Parallelism": <15}{"Duration"}')
print('-'*100)
for label, group in DATA.groupby(['experiment', 'variant', 'transparent', 'parallelism']):
    reps = group.rep.nunique()
    # Get tmax except logical latency, since this has dummy timestamps
    duration = group.query('parameter != "logical-latency"').t.max() / 60 
    print(f'{label[0]: <20}{label[1]: <20}{label[2]: < 15}{reps: <7}{label[3]: <15}{duration:3.1f} min')




# ---- DATA PLOTTING ----------------------------------------------------------------------------------

def soaComparison2(figsize=(6.5,3.5)):
    
    def aggregageAll(df, timeFunc=np.mean, nodeFunc=np.mean):
        assert DATA.parallelism.nunique() == 1
        assert DATA.experiment.nunique() == 1
        data = df.copy()
        # WARN: Node indexes are mixed with nodes in same aggregation!
        # Aggregate first on time
        # And then on nodes
        data = data.groupby(['rep', 'parallelism', 'variant', 'transparent', 'node', 'idx']).aggregate({'value': timeFunc})\
                    .groupby(level=['rep', 'parallelism', 'variant', 'transparent']).aggregate({'value': nodeFunc})\
                    .reset_index()
        data.columns = ['rep', 'parallelism', 'variant', 'transparent', 'value']
        return data

    def dataFor(parameter, timeFunc=np.mean, nodeFunc=np.mean):
        df = aggregageAll(get(parameter=parameter), timeFunc, nodeFunc)
        df = df[(df.variant != 'GL') | (df.transparent == False)]
        df.loc[df.transparent == True, 'variant'] += '/T'
        return df

    assert DATA.parallelism.nunique() == 1
    assert DATA.experiment.nunique() == 1
    ORDER=None
    COLORS=None
    #ORDER = ['NP', 'GL', 'ANK-1', 'ANK-1/T', 'ANK-PG', 'ANK-N', 'ANK-N/T']
    ORDER = ['NP', 'GL', 'ANK-1', 'ANK-PG', 'ANK-PG-U', 'ANK-N/T']
    COLORS = ['C0', 'C1', 'C2', 'C2', 'C3', 'C3']
    
    fig, axes = plt.subplots(ncols=2, nrows=2, figsize=figsize, sharey=False, sharex=True, squeeze=False)
    axes = axes.flatten()
    hue = None
    #axes[0].ticklabel_format(style='sci',axis='y',scilimits=(0, 2), useMathText=True)
    # Absolute values
    sns.barplot(x='variant', y='value', data=dataFor('rate'), ax=axes[0], order=ORDER, palette=COLORS)
    sns.barplot(x='variant', y='value', data=dataFor('endlatency'), ax=axes[1], order=ORDER, palette=COLORS)
    sns.barplot(x='variant', y='value', data=dataFor('memory'), ax=axes[2], order=ORDER, palette=COLORS)
#     # Plot also max memory as scatter
#     maxMemoryDf = dataFor('memory', timeFunc=np.max, nodeFunc=np.sum).groupby(['variant', 'transparent']).mean().reset_index()
#     maxMemoryDf['variant'] = pd.Categorical(maxMemoryDf.variant, ORDER)
#     maxMemoryDf.sort_values('variant', inplace=True)
#     maxMemoryDf.plot('variant', 'value',kind='scatter', s=50, c=COLORS, ax=axes[2], marker='^')
    sns.barplot(x='variant', y='value', data=dataFor('cpu', nodeFunc=np.sum), ax=axes[3], order=ORDER, palette=COLORS)
    
    for ax in axes:
        ax.set_ylabel('')
        ax.set_xlabel('')
    axes[0].set_title('Rate (t/s)', size=12)
    axes[1].set_title('Latency (s)', size=12)
    axes[2].set_title('Memory (MB)', size=12)
    axes[3].set_title('CPU Utilization (%)', size=12)
    
    for i, ax in enumerate(axes):
        print(f'Percent from NP => [{ax.get_title()}]:', end=' ')
        referenceHeight = ax.patches[0].get_height()
        ax.patches[3].set_hatch('/////')
        ax.patches[5].set_hatch('/////')
        for p in ax.patches[1:]:
            height = p.get_height()
            diff = percentageDiff(height, referenceHeight)
            print(f'{diff:3.0f}%', end= ' ')
#             ax.text(p.get_x()+p.get_width()/2,
#                     height - (height*0.425),
#                     f'{diff:+2.1f}%',
#                     ha='center', rotation=90, size=9, family='sans', weight='ultralight', color='#2f2f2f')
        print()
        referenceHeight2 = ax.patches[1].get_height()
        for p in ax.patches[2:]:
            height = p.get_height()
            diff = percentageDiff(height, referenceHeight2)
            if not np.isfinite(height):
                print('Invalid height for annotation')
                continue
            ax.text(p.get_x()+p.get_width()/2,
                    height - (height*0.7),
                    f'{diff:+2.1f}%',
                    ha='center', rotation=90, size=9, family='sans', weight='bold', color='#ffffff') 


    fig.tight_layout()
    fig.autofmt_xdate(ha='right', rotation=25)
    fig.savefig(f'{REPORT_FOLDER}/eval_soa_comp_{FIGURE_CODE}.pdf', pad_inches=.1, bbox_inches='tight',)
    
    # print string!
    
    printstring1 = "Figure created."
    printstring2 = "Saved {} to {}/eval_soa_comp_{}.pdf".format(FIGURE_NAME, REPORT_FOLDER, FIGURE_CODE)
    number_dashes = len(printstring1) + 2
    print()
    print()
    print("+" + "-" * number_dashes + "+")
    print("| " + printstring1 + " |")
    print("+" + "-" * number_dashes + "+")
    print()
    print(printstring2)



def soaComparison3(figsize=(14,3)):
    
    def aggregageAll(df, timeFunc=np.mean, nodeFunc=np.mean):
        assert DATA.parallelism.nunique() == 1
        assert DATA.experiment.nunique() == 1
        data = df.copy()
        # WARN: Node indexes are mixed with nodes in same aggregation!
        # Aggregate first on time
        # And then on nodes
        data = data.groupby(['rep', 'parallelism', 'variant', 'transparent', 'node', 'idx']).aggregate({'value': timeFunc})\
                    .groupby(level=['rep', 'parallelism', 'variant', 'transparent']).aggregate({'value': nodeFunc})\
                    .reset_index()
        data.columns = ['rep', 'parallelism', 'variant', 'transparent', 'value']
        return data

    def dataFor(parameter, timeFunc=np.mean, nodeFunc=np.mean):
        df = aggregageAll(get(parameter=parameter), timeFunc, nodeFunc)
        df = df[(df.variant != 'GL') | (df.transparent == False)]
        df.loc[df.transparent == True, 'variant'] += '/T'
        return df

    assert DATA.parallelism.nunique() == 1
    assert DATA.experiment.nunique() == 1
    ORDER=None
    COLORS=None
    #ORDER = ['NP', 'GL', 'ANK-1', 'ANK-1/T', 'ANK-PG', 'ANK-N', 'ANK-N/T']
    ORDER = ['ANK-1', 'ANK1-MNG-0', 'ANK1-MNG-1000']
    COLORS = ['C0', 'C1', 'C2', 'C2', 'C3', 'C3']
    
    fig, axes = plt.subplots(ncols=5, nrows=1, figsize=figsize, sharey=False, sharex=True, squeeze=False)
    axes = axes.flatten()
    hue = None
    # Absolute values
    sns.barplot(x='variant', y='value', data=dataFor('rate'), ax=axes[0], order=ORDER, palette=COLORS)
    sns.barplot(x='variant', y='value', data=dataFor('latency'), ax=axes[1], order=ORDER, palette=COLORS)
    sns.barplot(x='variant', y='value', data=dataFor('endlatency'), ax=axes[2], order=ORDER, palette=COLORS)
    sns.barplot(x='variant', y='value', data=dataFor('memory'), ax=axes[3], order=ORDER, palette=COLORS)
    sns.barplot(x='variant', y='value', data=dataFor('cpu', nodeFunc=np.sum), ax=axes[4], order=ORDER, palette=COLORS)
    
    for ax in axes:
        ax.set_ylabel('')
        ax.set_xlabel('')
    axes[0].set_title('Rate (t/s)', size=12)
    axes[1].set_title('Latency (s)', size=12)
    axes[2].set_title('End-Latency (s)', size=12)
    axes[3].set_title('Memory (MB)', size=12)
    axes[4].set_title('CPU Utilization (%)', size=12)
    
    for i, ax in enumerate(axes):
        # print(f'Percent from NP => [{ax.get_title()}]:', end=' ')
        referenceHeight = ax.patches[0].get_height()
#         ax.patches[3].set_hatch('/////')
#         ax.patches[5].set_hatch('/////')
        for p in ax.patches[1:]:
            height = p.get_height()
            diff = percentageDiff(height, referenceHeight)
            # print(f'{diff:3.0f}%', end= ' ')
            diffText = f'{diff:+2.1f}%' if diff <= 200 else f'{height / referenceHeight:0.0f}x'
            # print('FIXME: TIMES CORRECT CALCULATION!')
            ax.text(p.get_x()+p.get_width()/2,
                    height - (height*0.425),
                    diffText,
                    ha='center', rotation=90, size=10, family='sans', weight='normal', color='#fefefe')
        # print()


    fig.tight_layout()
    fig.autofmt_xdate(ha='right', rotation=25)
    fig.savefig(f'{REPORT_FOLDER}/eval_soa_comp_{FIGURE_CODE}.pdf', pad_inches=.1, bbox_inches='tight')

    # print string!
    
    printstring1 = "Figure created."
    printstring2 = "Saved {} to {}/eval_soa_comp_{}.pdf".format(FIGURE_NAME, REPORT_FOLDER, FIGURE_CODE)
    number_dashes = len(printstring1) + 2
    print()
    print()
    print("+" + "-" * number_dashes + "+")
    print("| " + printstring1 + " |")
    print("+" + "-" * number_dashes + "+")
    print()
    print(printstring2)

if SOA_COMP == 2:
	soaComparison2()
	sys.exit(0)
else:
	soaComparison3()
	sys.exit(0)
