import sys
import argparse
import os
import pandas as pd
from matplotlib import pyplot as plt

# Sample command line execution:
# python3.6 Azure-functions-cdf-builder.py --datadir "/home/ubuntu/data/Azure" --figuresdir "/home/ubuntu" --n 12

parser = argparse.ArgumentParser(description = 'Building CDF of Azure functions invocations')

parser.add_argument('--datadir', dest = 'datadir',
                    action = 'store', default = None,
                    help = 'directory with the Azure functions invocations files')

parser.add_argument('--figuresdir', dest = 'figuresdir',
                    action = 'store', default = None,
                    help = 'directory to store the plots with resulting CDF')

parser.add_argument('--n', dest = 'number_of_files',
                    action = 'store', default = 12,
                    help = 'number of files to use from the dataset')

args = parser.parse_args()

if args.datadir is None:
    sys.exit('No data directory specified.')

if args.figuresdir is None:
    sys.exit('No directory to store figures specified.')

def file_id_to_str(file_id : int) -> str:
    return '0' + str(file_id) if file_id < 10 else str(file_id)

filename_pattern_invocations = os.path.join(args.datadir, 'invocations_per_function_md.anon.d{}.csv')

data_collected = pd.DataFrame(columns = ['invocations', 'HashApp', 'HashFunction', 'minute_in_day']).set_index(['HashApp', 'HashFunction', 'minute_in_day'])
start_idx = 1
for file_id in range(start_idx, args.number_of_files + 1):
    filename_invocations = filename_pattern_invocations.format(file_id_to_str(file_id))

    invocations_data_raw = pd.read_csv(filename_invocations)

    invocations_data_http = invocations_data_raw[invocations_data_raw.Trigger == 'http']
    invocations_data = pd.melt(invocations_data_http, id_vars = ['HashApp', 'HashFunction'], value_vars = invocations_data_http.columns[4:]).rename(columns = {'variable': 'minute_in_day', 'value': 'invocations'})

    invocations_data.set_index(['HashApp', 'HashFunction', 'minute_in_day'], inplace = True)

    data_collected = data_collected.add(invocations_data / number_of_files, fill_value = 0)

data_collected = data_collected.groupby(['HashApp', 'minute_in_day']).max()

non_zero_invocations = data_collected[data_collected.invocations > 0]
X = sorted(non_zero_invocations.invocations.unique())

Y = non_zero_invocations.groupby('invocations')['invocations'].count().sort_index().cumsum()
Y /= max(Y)

zero_invocations_count = len(data_collected[data_collected.invocations == 0])

fig, ax = plt.subplots(1, 1, figsize = (8, 6))

percentiles = [0.99, 0.95, 0.90, 0.80, 0.50]
font = {'color':  'black', 'weight': 'normal', 'size': 8}
for percentile in percentiles:
    ax.axhline(percentile, 0, 1.0, color = 'k', linestyle = 'dashed', lw = 0.5)
    ax.text(0, percentile + 0.002, f"{(int(percentile * 100))}th percentile", fontdict = font)

ax.plot(X, Y)
ax.set_xlabel(f'Load, invocations per minute.\nNot included {zero_invocations_count} cases of 0 invocations per minute.')

plt.savefig(os.path.join(args.figuresdir, 'cdf.png'), dpi = 600, bbox_inches='tight')
plt.close()
