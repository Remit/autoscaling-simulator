import os

from cruncher.experimental_regime.experimental_regime import ExperimentalRegime
from autoscalingsim.utils.error_check import ErrorChecker

@ExperimentalRegime.register('building_blocks')
class BuildingBlocksExperimentalRegime(ExperimentalRegime):

    def __init__(self, config_folder : str, simulator : 'Simulator', regime_config : dict):
        pass

    def run_experiment(self):
        pass
