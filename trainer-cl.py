import sys
import argparse
from training_ground.trainer import Trainer

# Sample command line execution:
# python3.6 trainer-cl.py --confdir "training_conf/nonlinear-nn/"

parser = argparse.ArgumentParser(description = 'Learning the model to map the metrics and scaling aspects onto the quality of user experience.')

parser.add_argument('--confdir', dest = 'config_dir',
                    action = 'store', default = None,
                    help = 'directory with the configuration files for the training')

args = parser.parse_args()

if args.config_dir is None:
    sys.exit('No configuration directory specified.')

t = Trainer(args.config_dir)
t.start_training()
