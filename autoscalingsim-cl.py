import argparse
import os
import sys
from autoscalingsim import simulator
import pandas as pd
from stethoscope.analytical_engine import AnalysisFramework

# Q: why not in __main__? Problems with the relative paths in the package, details below.
# Reference for Python's import challenges: https://stackoverflow.com/questions/14132789/relative-imports-in-python-2-7/14132912#14132912
#
# Sample command line execution:
# python3.6 autoscalingsim-cl.py --step 10 --start "2020-09-17T10:00:00" --confdir "experiments/test" --simtime 1m
#
parser = argparse.ArgumentParser(description = 'Simulating autoscalers for the cloud applications and platforms.')

parser.add_argument('--step', dest = 'simulation_step_ms',
                    action = 'store', default = 10, type = int,
                    help = 'simulation step in milliseconds (default: 10)')
parser.add_argument('--start', dest = 'starting_time_str',
                    action = 'store', default = '1970-01-01 00:00:00',
                    help = 'simulated start in form of a date and time string YYYY-MM-DDThh:mm:ss (default: 1970-01-01T00:00:00)')
parser.add_argument('--simtime', dest = 'time_to_simulate_raw',
                    action = 'store', default = '1m',
                    help = 'amount of time to simulate (default 1m, 1 minute)')
parser.add_argument('--confdir', dest = 'config_dir',
                    action = 'store', default = None,
                    help = 'directory with the configuration files for the simulation')
parser.add_argument('--results', dest = 'results_dir',
                    action = 'store', default = None,
                    help = 'path to the directory where the results should be stored')
parser.add_argument('--plotsdir', dest = 'plots_dir',
                    action = 'store', default = None,
                    help = 'path to the directory where the plots should be stored')

args = parser.parse_args()

try:
    starting_time = pd.Timestamp(args.starting_time_str)

    if args.config_dir is None:
        sys.exit('No configuration directory specified.')

    if args.simulation_step_ms < 10:
        sys.exit('The simulation step is too small, should be equal or more than 10 ms')

    time_val = ''
    time_unit_name = ''
    for char in args.time_to_simulate_raw:
        if char.isnumeric():
            time_val += char
        elif char.isalpha():
            time_unit_name += char
        else:
            raise ValueError(f'Unrecognized symbol in --simtime argument: {char}')

    simulation_step = pd.Timedelta(args.simulation_step_ms, unit = 'ms')
    simulator = simulator.Simulator(simulation_step, starting_time,
                                    pd.Timedelta(int(time_val), unit = time_unit_name))

    simulator.add_simulation(args.config_dir, args.results_dir)
    simulator.start_simulation()

    if not args.plots_dir is None:
        if not os.path.exists(args.plots_dir):
            os.makedirs(args.plots_dir)

        af = AnalysisFramework(simulation_step, args.plots_dir)
        af.build_figures_for_single_simulation(simulator.simulations[os.path.basename(args.config_dir)], '')

except ValueError:
    sys.exit('Incorrect format for the simulated start, should be YYYY-MM-DDThh:mm:ss')
