from graph_helpers import config
from graph_helpers.global_defs import *
from graph_helpers.data_helpers import *
from graph_helpers.plot_functions import *
import argparse
import os

if __name__ == '__main__':

    # Directory of the output files
    config.TEXT_FIGURES_PATH = './figures'
    config.LOGICAL_LATENCY_OUTPUT_DIRECTORY = config.TEXT_FIGURES_PATH
    os.makedirs(config.TEXT_FIGURES_PATH, exist_ok=True)

    parser = argparse.ArgumentParser(description='Plot selected graph')
    parser.add_argument('--path', type=str, required=True, help='Path of the experiments')
    parser.add_argument('--experiment', type=str, required=True, nargs='+',
                        help='Experiment code (or codes separated by spaces for logical-latency)')
    parser.add_argument('--name', type=str, required=True, help='Name to be used for the output files and legends (e.g., lr)')
    parser.add_argument('--plot', type=str, required=True, help=f'Type of plot ({",".join(PLOT_FUNCTIONS_DICT.keys())})')
    parser.add_argument('--prepare-logical-latency', action='store_true', dest='logicallatency', help='Also preprocess logical latency')
    args = parser.parse_args()

    config.OUTPUT_DIRECTORY = args.path
    config.FIGURE_CODE = args.name

    plot_function = PLOT_FUNCTIONS_DICT[args.plot]
    if args.plot != 'logicallatency':
        assert len(args.experiment) == 1, 'Multiple experiments allowed only for logical latency plot!'
        config.COMMIT = args.experiment[0]
        config.REPORT_FOLDER = f'{config.OUTPUT_DIRECTORY}/{config.COMMIT}'
        loadData(config.REPORT_FOLDER)
        preprocess_data()
        if args.plot in ['external', 'table']:
            renameDbVariants()
        plot_function() 
        if args.logicallatency:
            preprocessLogicalLatency()
    else:
        plot_function(args.experiment)
    

