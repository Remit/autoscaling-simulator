import argparse
import sys
from autoscalingsim import simulator
import pandas as pd

# Q: why not in __main__? Problems with the relative paths in the package, details below.
# Reference for Python's import challenges: https://stackoverflow.com/questions/14132789/relative-imports-in-python-2-7/14132912#14132912
#
# Sample command line execution:
# python3.6 autoscalingsim-cl.py --step 10 --start "2020-09-17T10:00:00" --confdir "experiments/test" --simdays 0.001
#
parser = argparse.ArgumentParser(description = 'Simulating autoscalers for the cloud applications and platforms.')

parser.add_argument('--step', dest = 'simulation_step_ms',
                    action = 'store', default = 10, type = int,
                    help = 'simulation step in milliseconds (default: 10)')
parser.add_argument('--start', dest = 'starting_time_str',
                    action = 'store', default = '1970-01-01 00:00:00',
                    help = 'simulated start in form of a date and time string YYYY-MM-DDThh:mm:ss (default: 1970-01-01T00:00:00)')
parser.add_argument('--simdays', dest = 'time_to_simulate_days',
                    action = 'store', default = 0.0005, type = float,
                    help = 'number of days to simulate (default: 0.0005 ~ 1 min)')
parser.add_argument('--confdir', dest = 'config_dir',
                    action = 'store', default = None,
                    help = 'directory with the configuration files for the simulation')
parser.add_argument('--results', dest = 'results_dir',
                    action = 'store', default = None,
                    help = 'path to the directory where the results should be stored')

args = parser.parse_args()

try:
    starting_time = pd.Timestamp(args.starting_time_str)

    if args.config_dir is None:
        sys.exit('No configuration directory specified.')

    if args.simulation_step_ms < 10:
        sys.exit('The simulation step is too small, should be equal or more than 10 ms')

    simulator = simulator.Simulator(pd.Timedelta(args.simulation_step_ms, unit = 'ms'),
                                    starting_time,
                                    args.time_to_simulate_days)

    simulator.add_simulation(args.config_dir,
                             args.results_dir)

    simulator.start_simulation()
except ValueError:
    sys.exit('Incorrect format for the simulated start, should be YYYY-MM-DDThh:mm:ss')

#print(list(simulator.simulations["test"].application_model.buffer_times_by_request['auth']))
