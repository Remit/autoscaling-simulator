import json
import pandas as pd

from .regional_load_model.regional_load_model import RegionalLoadModel

from autoscalingsim.utils.error_check import ErrorChecker

class LoadModel:

    """ Combines regional workload generation models. Parses the configuration file. """

    def __init__(self, simulation_step : pd.Timedelta, filename : str):

        self.region_models = {}

        if filename is None:
            raise ValueError('Configuration file not provided for the WorkloadModel.')
        else:
            with open(filename) as f:
                config = json.load(f)
                load_kind = ErrorChecker.key_check_and_load('load_kind', config)
                regions_configs = ErrorChecker.key_check_and_load('regions_configs', config)

                for region_config in regions_configs:
                    region_name = ErrorChecker.key_check_and_load('region_name', region_config)
                    pattern = ErrorChecker.key_check_and_load('pattern', region_config, 'region_name', region_name)
                    load_configs = ErrorChecker.key_check_and_load('load_configs', region_config, 'region_name', region_name)
                    self.region_models[region_name] = RegionalLoadModel.get(load_kind)(region_name, pattern, load_configs, simulation_step)

    def generate_requests(self, timestamp : pd.Timestamp):

        """ Joins the lists of generated requests across all the regional load models """

        return [req for region_workload_model in self.region_models.values() for req in region_workload_model.generate_requests(timestamp)]

    def get_generated_load(self):

        return { region_name : region_load_model.get_stat() for region_name, region_load_model in self.region_models.items()}
