import sys
import argparse
from cruncher.cruncher import Cruncher

# Sample command line execution:
# python3.6 cruncher-cl.py --confdir "cruncher_conf/experiment_metrics_step_up/"
# comment: on my laptop it needs to be invoked with python without version at the end

parser = argparse.ArgumentParser(description = 'Running the experiments for alternative configurations.')

parser.add_argument('--confdir', dest = 'config_dir',
                    action = 'store', default = None,
                    help = 'directory with the configuration files for the experiments')

parser.add_argument('--datadir', dest = 'data_dir',
                    action = 'store', default = None,
                    help = 'directory with the data to visualize')

args = parser.parse_args()

if args.config_dir is None:
    sys.exit('No configuration directory specified.')

c = Cruncher(args.config_dir)

if args.data_dir is None:
    c.run_experiment()
else:
    c.set_data_dir(args.data_dir)

c.visualize()
