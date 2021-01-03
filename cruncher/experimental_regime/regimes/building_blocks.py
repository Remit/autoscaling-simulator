import os

from cruncher.experimental_regime.experimental_regime import ExperimentalRegime
from autoscalingsim.utils.error_check import ErrorChecker

@ExperimentalRegime.register('building_blocks')
class BuildingBlocksExperimentalRegime(ExperimentalRegime):

    def __init__(self, config_folder : str, regime_config : dict, simulator : 'Simulator',
                 repetitions_count_per_simulation : int, keep_evaluated_configs : bool = False):
        pass

    def run_experiment(self):
        pass
