from .global_defs import *
from .data_helpers import *
from . import config

def save_fig(fig, base_name, also_in_report_folder=True):
    if also_in_report_folder:
        path = f'{config.REPORT_FOLDER}/{base_name}_{config.FIGURE_CODE}.pdf'
        fig.savefig(path, pad_inches=.1, bbox_inches='tight',)
        print(f'Saved {path}')
    path = f'{config.TEXT_FIGURES_PATH}/{base_name}_{config.FIGURE_CODE}.pdf'
    fig.savefig(path, pad_inches=.1, bbox_inches='tight',)
    print(f'Saved {path}')

def soaComparison3(figsize=(6.5,3.5)):
    def aggregageAll(df, timeFunc=np.mean, nodeFunc=np.mean):
        assert config.DATA.parallelism.nunique() == 1
        assert config.DATA.experiment.nunique() == 1
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

    assert config.DATA.parallelism.nunique() == 1
    assert config.DATA.experiment.nunique() == 1
    ORDER=None
    COLORS=None
    #ORDER = ['ANK-1', 'ANK-PG-1000', 'ANK-PG-U-1000', 'ANK-SL-1000', 'ANK-SL-U-1000', 'ANK-PG-0', 'ANK-PG-U-0', 'ANK-SL-0', 'ANK-SL-U-0', 'ANK-MG-1000', 'ANK-MG-0', 'ANK-N4J']
    #ORDER = ['ANK-1', 'ANK-PG-U-1000', 'ANK-SL-U-1000', 'ANK-MG-1000', 'ANK-PG-U-0', 'ANK-SL-U-0', 'ANK-MG-0']
    ORDER = ['ANK-1', 'SQL-P/R', 'SQL-I/R', 'NoSQL/R', 'SQL-P/A', 'SQL-I/A', 'NoSQL/A']
    COLORS = ['C0', 'C1', 'C2', 'C3', 'C1', 'C2', 'C3']
    
    
    fig, axes = plt.subplots(ncols=2, nrows=2, figsize=figsize, sharey=False, sharex=True, squeeze=False)
    axes = axes.flatten()
    hue = None
    # Absolute values
    sns.barplot(x='variant', y='value', data=dataFor('rate'), ax=axes[0], order=ORDER, palette=COLORS)
    sns.barplot(x='variant', y='value', data=dataFor('latency'), ax=axes[1], order=ORDER, palette=COLORS)
    sns.barplot(x='variant', y='value', data=dataFor('deliverylatency'), ax=axes[2], order=ORDER, palette=COLORS)
    sns.barplot(x='variant', y='value', data=dataFor('cpu', nodeFunc=np.sum), ax=axes[3], order=ORDER, palette=COLORS)
    
    for ax in axes:
        ax.set_ylabel('')
        ax.set_xlabel('')
    axes[0].set_title('Rate (t/s)', size=12)
    axes[1].set_title('Latency (s)', size=12)
    axes[2].set_title('Delivery Latency', size=12)
#     axes[2].set_yscale('log')
    axes[3].set_title('CPU Utilization (%)', size=12)
    
    for i, ax in enumerate(axes):
        print(f'Percent from ANK-1 => [{ax.get_title()}]:', end=' ')
        axisHeight = ax.get_ylim()[1]
        referenceHeight = ax.patches[0].get_height()
        if not np.isfinite(referenceHeight):
            continue
        ax.patches[4].set_hatch('///')
        ax.patches[5].set_hatch('///')
        ax.patches[6].set_hatch('///')
        for p in ax.patches[1:]:
            height = p.get_height()
            if not np.isfinite(height):
                continue
            diff = percentageDiff(height, referenceHeight)
            diffText = f'{diff:+2.1f}%' if diff <= 100 else f'{float(height) / float(referenceHeight):+0.1f}x'
            print(diffText, end=' ')
            textHeight = .2*height if height / axisHeight > 0.6 else height + (axisHeight * 0.1)
            ax.text(p.get_x()+p.get_width()/2,
                    textHeight,
                    diffText,
                    ha='center', rotation=90, size=10, family='sans', weight='normal', color='#3f3f3f')
        print()
    sns.despine()
    fig.tight_layout()
    fig.autofmt_xdate(ha='right', rotation=25)
    save_fig(fig, 'eval_db_comp')

def pivotTable(synthetic):
    def roundPivot(row):
        nr = row.copy().astype(str)
        for i in range(len(row)):
            nr[i] = f'{row[i]:0.2f}'
        return nr.astype(float)

    def computePercentageDiffs(row):
        nr = row.copy().astype(str)
        for i in range(len(row)):
            if i == 0:
                continue
            pdiff = percentageDiff(row[i], row[0])
            if np.isnan(pdiff):
                nr[i] = 'missing'
                continue
            nr[i] = f'{pdiff:+0.0f}%' if pdiff < 100 else f'{row[i]/float(row[0]):+0.1f}x'
        nr[0] = '-'
        return nr
    
    def derivative(values):
        if len(values) < 2:
            return [0]
        return values.diff()
            
    df = expandSyntheticCols(config.DATA) if synthetic else config.DATA
    df['derivative'] = df.groupby(['parameter', 'variant']).value.transform(derivative)
    
    pivot = pd.pivot_table(df, values=['value'], index=['parameter'], columns=['variant'])
    pivot.loc['provratio(R/W)'] = pivot.loc['provreads'] / pivot.loc['provwrites']
    pivot = pivot.reindex(index=['rate', 'latency', 'deliverylatency', 'cpu', 'provratio(R/W)'])

    pivot = pivot.apply(roundPivot, axis=1)
    print()
    print('----------------------------')
    print('----- Comparison Table -----')
    print('----------------------------')
    print()
    print('----------------------------')
    print('----- Absolute Values ------')
    print('----------------------------')
    print(pivot)
    print()
    print('----------------------------')
    print('----- Percentage Diffs -----')
    print('----------------------------')
    percentages = pivot.apply(computePercentageDiffs, axis=1)
    print(percentages)
    print()
    path = f'{config.REPORT_FOLDER}/pivot.xlsx'
    with pd.ExcelWriter(path) as writer:
        pivot.to_excel(writer, 'Values')
        percentages.to_excel(writer, 'Percentages')
        print(f'Saved {path}')


def drillDown():
    PARAMETERS = ['rate', 'latency', 'deliverylatency', 'provtime', 'provratio', 'cpu', 'memory']
    PARAMETER_ORDER = {parameter: i for i, parameter in enumerate(PARAMETERS)}
    df = config.DATA
    #df = expandSyntheticCols(config.DATA)
    df = df[df.parameter.isin(PARAMETERS)]
    #df = df.query('provenanceOverlap ==50 & provenanceSize == 100')
    for variant, vgroup in sorted(df.groupby(['variant'])):
        fig, axes = plt.subplots(ncols=vgroup.parameter.nunique(), figsize=(30, 4), sharex=True)
        fig.suptitle(variant)
        for ax, (parameter, pgroup) in zip(axes, sorted(vgroup.groupby('parameter'), key=lambda t: PARAMETER_ORDER[t[0]])):
            pdata = pgroup.groupby('t').mean().reset_index()
            pdata['value'] = pdata.value.rolling(5, min_periods=1).mean()
            ax.plot('t', 'value', data=pdata)
            ax.set_title(f'{parameter}')


def soaComparison2(figsize=(6.5,3.5)):
    def aggregageAll(df, timeFunc=np.mean, nodeFunc=np.mean):
        assert config.DATA.parallelism.nunique() == 1
        assert config.DATA.experiment.nunique() == 1
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

    assert config.DATA.parallelism.nunique() == 1
    assert config.DATA.experiment.nunique() == 1
    ORDER=None
    COLORS=None
    ORDER = ['NP', 'GL', 'ANK-1', 'ANK-1/T', 'ANK-N', 'ANK-N/T']
    COLORS = ['C0', 'C1', 'C2', 'C2', 'C3', 'C3']
    
    fig, axes = plt.subplots(ncols=2, nrows=2, figsize=figsize, sharey=False, sharex=True, squeeze=False)
    axes = axes.flatten()
    hue = None
    #axes[0].ticklabel_format(style='sci',axis='y',scilimits=(0, 2), useMathText=True)
    # Absolute values
    sns.barplot(x='variant', y='value', data=dataFor('rate'), ax=axes[0], order=ORDER, palette=COLORS)
    sns.barplot(x='variant', y='value', data=dataFor('latency'), ax=axes[1], order=ORDER, palette=COLORS)
    sns.barplot(x='variant', y='value', data=dataFor('memory'), ax=axes[2], order=ORDER, palette=COLORS)
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
        ax.patches[3].set_hatch('///')
        ax.patches[5].set_hatch('///')
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
                    height - (height*0.75),
                    f'{diff:+2.1f}%',
                    ha='center', rotation=90, size=11, family='sans', weight='normal', color='#3f3f3f') 

    sns.despine()
    fig.tight_layout()
    fig.autofmt_xdate(ha='right', rotation=25)
    save_fig(fig, 'eval_soa_comp')


def preprocessLogicalLatency():
    def secondsToMillis(s):
        return s * 1000
    def minutesToMillis(m):
        return secondsToMillis(m * 60)
    def hoursToMillis(h):
        return minutesToMillis(60*h)
    DELAY_CONSTANTS = {'lr': secondsToMillis(120+30), 'sg': hoursToMillis(48), 
                       'carlocal': secondsToMillis(6), 'carcloud': secondsToMillis(300)}
    delayConstant = DELAY_CONSTANTS[config.FIGURE_CODE]
    df = get(parameter='logical-latency').copy()
    assert df.parallelism.nunique() == 1
    df = df[df.transparent == False]
    df.drop(columns=['kind', 'transparent', 'parameter', 'idx', 'parallelism'], inplace=True)
    
    # Preprocess
    df['value'] /= delayConstant
    outputFile = f'{config.LOGICAL_LATENCY_OUTPUT_DIRECTORY}/logical-latency-agg-{config.COMMIT}.csv'
    df.to_csv(outputFile, index=False)
    print(f'Saved {outputFile}')


def plotLogicalLatency(commits):
    NODE_TYPES={'V0': 'SINK', 'V1': 'SOURCE', 'A0': 'SINK-L', 'A1': 'SOURCE-L', '2': 'EDGE'}
    QUERY_DICT = {'CarLocalQueries': 'OA', 'CarCloudQueries': 'VT', 'LinearRoadCombined': 'LR', 'SmartGridCombined': 'SG'}
    QUERY_ORDER = ['LR', 'SG', 'VT', 'OA']
    NODE_ORDER = ['SINK', 'SOURCE', 'EDGE', 'SINK-L', 'SOURCE-L']
    dfs = [pd.read_csv(f'{config.LOGICAL_LATENCY_OUTPUT_DIRECTORY}/logical-latency-agg-{commit}.csv') for commit in commits]
    df = pd.concat(dfs, ignore_index=True, sort=False)
    df['nodeType'] = df.node.apply(lambda name: NODE_TYPES[name.split('-')[-1]])
    df['experiment'] = df.experiment.apply(lambda name: QUERY_DICT[name])
    df = pd.pivot_table(df, values=['value'], index=['variant', 'rep', 'experiment', 'nodeType']).reset_index()
    with sns.plotting_context(rc={"font.size":12,"axes.titlesize":14,"axes.labelsize":13}):
        g = sns.catplot(x='nodeType', y='value', data=df, hue='experiment', kind='bar', order=NODE_ORDER, 
                        legend_out=False, col_order=['ANK-1', 'ANK-N'], aspect=1.4, height=2.5, col='variant', hue_order=QUERY_ORDER)
        g.set_xlabels('').set_ylabels('Provenance Latency (U)').set_titles('{col_name}')
        g.add_legend(title='Query', ncol=2, fontsize=12)
        sns.despine()
        g.axes.flat[0].get_legend().get_title().set_fontsize(13)
        g.fig.autofmt_xdate(ha='right', rotation=25)
        save_fig(g.fig, 'eval_logical_latency', also_in_report_folder=False)

def synthetic1():
    def average(df):
        assert df.transparent.nunique() == 1
        assert df.sourceParallelism.nunique() == 1
        assert df.sinkParallelism.nunique() == 1
        assert df.parallelism.nunique() == 1
        data = df.copy()
        # WARN: Node indexes are mixed with nodes in same aggregation!
        data = data.groupby(['rep', 'provenanceOverlap', 'provenanceSize', 'variant', 'parameter', 'node', 'idx']).aggregate({'value': np.mean})\
                    .groupby(level=['rep', 'provenanceOverlap', 'provenanceSize', 'variant', 'parameter']).aggregate({'value': np.mean})\
                    .reset_index()
        data.columns = ['rep', 'provenanceOverlap', 'provenanceSize', 'variant', 'parameter', 'value']
        return data
    ROW_ORDER=['rate', 'latency', 'cpu', 'memory']
    with sns.plotting_context(rc={"font.size":12,"axes.titlesize":14,"axes.labelsize":13}):
        g = sns.catplot(col='variant', y='value', x='provenanceSize', row='parameter', data=average(expandSyntheticCols(config.DATA)), hue='provenanceOverlap', 
                sharey='row', kind='bar', col_order=['ANK-1', 'ANK-N'], row_order=ROW_ORDER, height=1.5, aspect=1.8,
                legend=False)
        g.set_axis_labels('Provenance (#tuples)', ROW_ORDER).set_titles("{row_name} | {col_name}")
        for i, axes_row in enumerate(g.axes):
            for j, axes_col in enumerate(axes_row):
                row, col = axes_col.get_title().split('|')

                if i == 0:
                    axes_col.set_title(col.strip())
                else:
                    axes_col.set_title('')

                if j == 0:
                    ylabel = axes_col.get_ylabel()
                    axes_col.set_ylabel(config.PARAMETER_LABELS[row.strip()])
        for ax in g.axes.flat:
            ax.ticklabel_format(axis='y', style='sci', scilimits=(0, 3), useMathText=True)

        sns.despine()
        g.fig.align_ylabels(g.axes[:, 0])
        g.add_legend(title='Provenance Overlap (%)', loc='lower center', ncol=3, fontsize=12)
        g._legend.get_title().set_fontsize(13)
        g.fig.tight_layout()
        g.fig.subplots_adjust(bottom=0.2)
        save_fig(g.fig, 'eval_synthetic_comp')


def synthetic2():
    def average(df):
        assert df.provenanceOverlap.nunique() == 1
        assert df.provenanceSize.nunique() == 1
        assert df.parallelism.nunique() == 1
        data = df.copy()
        # WARN: Node indexes are mixed with nodes in same aggregation!
        data = data.groupby(['rep', 'sourceParallelism', 'sinkParallelism', 'variant', 'parameter', 'node', 'idx']).aggregate({'value': np.mean})\
                    .groupby(level=['rep', 'sourceParallelism', 'sinkParallelism', 'variant', 'parameter']).aggregate({'value': np.mean})\
                    .reset_index()
        data.columns = ['rep', 'sourceParallelism', 'sinkParallelism', 'variant', 'parameter', 'value']
        return data
    ROW_ORDER=['rate', 'latency', 'cpu', 'memory']
    with sns.plotting_context(rc={"font.size":12,"axes.titlesize":14,"axes.labelsize":13}):
        g = sns.catplot('sourceParallelism', 'value', kind='bar', row='parameter', data=average(expandSyntheticCols(config.DATA)), 
                    sharey='row', hue='sinkParallelism', col_order=['ANK-1', 'ANK-N'], row_order=ROW_ORDER,
                    height=1.5, aspect=1.2, col='variant',legend=False)

        g.set_axis_labels('#queries', ROW_ORDER).set_titles("{row_name} | {col_name}")
        for i, axes_row in enumerate(g.axes):
            for j, axes_col in enumerate(axes_row):
                row, col = axes_col.get_title().split('|')

                if i == 0:
                    axes_col.set_title(col.strip())
                else:
                    axes_col.set_title('')

                if j == 0:
                    ylabel = axes_col.get_ylabel()
                    axes_col.set_ylabel(config.PARAMETER_LABELS[row.strip()])

        for ax in g.axes.flat:
            ax.set_yscale('log')
        g.fig.align_ylabels(g.axes[:, 0])
        g.add_legend(title='Parallelism', loc='lower center', ncol=5, fontsize=12)
        g._legend.get_title().set_fontsize(13)
        g.fig.tight_layout()
        g.fig.subplots_adjust(bottom=0.2)
        save_fig(g.fig, 'eval_synthetic_comp')
    
PLOT_FUNCTIONS_DICT = {'soa': soaComparison2, 
                       'external': soaComparison3, 
                       'table': lambda: pivotTable(True),
                       'synthetic1': synthetic1,
                       'synthetic2': synthetic2,
                       'logicallatency': plotLogicalLatency}

        