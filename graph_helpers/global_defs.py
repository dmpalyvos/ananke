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
from ipywidgets import interact, interactive, fixed, interact_manual
import ipywidgets as widgets
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42
matplotlib.rcParams['hatch.linewidth'] = 0.2
matplotlib.rcParams['xtick.labelsize'] = 10

sns.set_palette(sns.color_palette('Set2', n_colors=14, desat=0.9))
sns.set_style("ticks")   

