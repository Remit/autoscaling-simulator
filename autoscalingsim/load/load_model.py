import json
import pandas as pd

from .regional_load_model.regional_load_model import RegionalLoadModel

from autoscalingsim.utils.metric_source import MetricSource
from autoscalingsim.utils.error_check import ErrorChecker

class LoadModel(MetricSource):

    """ Combines regional workload generation models. Parses the configuration file. """

    def __init__(self, simulation_step : pd.Timedelta, simulation_start : pd.Timestamp, filename : str, reqs_processing_infos : dict):

        self.region_models = {}

        if filename is None:
            raise ValueError('Configuration file not provided for the WorkloadModel.')
        else:
            with open(filename) as f:

                try:
                    config = json.load(f)
                    load_kind = ErrorChecker.key_check_and_load('load_kind', config)
                    regions_configs = ErrorChecker.key_check_and_load('regions_configs', config)

                    for region_config in regions_configs:
                        region_name = ErrorChecker.key_check_and_load('region_name', region_config)
                        pattern = ErrorChecker.key_check_and_load('pattern', region_config, 'region_name', region_name)
                        load_configs = ErrorChecker.key_check_and_load('load_configs', region_config, 'region_name', region_name)
                        self.region_models[region_name] = RegionalLoadModel.get(load_kind)(region_name, pattern, load_configs, simulation_step, simulation_start, reqs_processing_infos)

                except json.JSONDecodeError:
                    raise ValueError(f'An invalid JSON when parsing for {self.__class__.__name__}')

    def generate_requests(self, timestamp : pd.Timestamp):

        """ Joins the lists of generated requests across all the regional load models """

        return [req for region_workload_model in self.region_models.values() for req in region_workload_model.generate_requests(timestamp)]

    def get_generated_load(self):

        return { region_name : region_load_model.get_stat() for region_name, region_load_model in self.region_models.items()}

    def get_metric_value(self, region_name : str, metric_name : str, req_type : str):

        return self.region_models[region_name].get_requests_count_per_unit_of_time(req_type)

    def get_aspect_value(self, region_name : str, aspect_name : str):

        raise NotImplementedError(f'Not supported for {self.__class__.__name__}')

    def get_resource_requirements(self, region_name : str):

        raise NotImplementedError(f'Not supported for {self.__class__.__name__}')

    def get_placement_parameter(self, region_name : str, parameter : str):

        raise NotImplementedError(f'Not supported for {self.__class__.__name__}')
