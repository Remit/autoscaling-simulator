import sys
import argparse
from cruncher.cruncher import Cruncher

# Sample command line execution:
# python3.6 cruncher-cl.py --confdir "cruncher_conf/experiment_metrics_step_up/"

parser = argparse.ArgumentParser(description = 'Running the experiments for alternative configurations.')

parser.add_argument('--confdir', dest = 'config_dir',
                    action = 'store', default = None,
                    help = 'directory with the configuration files for the experiments')

args = parser.parse_args()

if args.config_dir is None:
    sys.exit('No configuration directory specified.')

c = Cruncher(args.config_dir)
c.run_experiment()
c.visualize()
