import json
import os
import numbers
import pandas as pd

from .metric.scalingmetric import MetricDescription
from .scaled.scaled_service_settings import ScaledServiceScalingSettings

from autoscalingsim.utils.error_check import ErrorChecker

class ScalingPolicyConfiguration:

    """
    Wraps the configuration of the scaling policy extracted from the configuration
    file.
    """

    def __init__(self, config_file : str):

        self.app_structure_scaling_config = None
        self.services_scaling_config = {}
        self.platform_scaling_config = None

        if not os.path.isfile(config_file):
            raise ValueError(f'No {self.__class__.__name__} configuration file found under the path {config_file}')

        with open(config_file) as f:

            config = json.load(f)

            policy_config = ErrorChecker.key_check_and_load('policy', config, self.__class__.__name__)
            app_config = ErrorChecker.key_check_and_load('application', config, self.__class__.__name__)
            platform_config = ErrorChecker.key_check_and_load('platform', config, self.__class__.__name__)

            # General policy settings
            sync_period_raw = ErrorChecker.key_check_and_load('sync_period', policy_config, self.__class__.__name__)
            sync_period_value = ErrorChecker.key_check_and_load('value', sync_period_raw, self.__class__.__name__)
            sync_period_unit = ErrorChecker.key_check_and_load('unit', sync_period_raw, self.__class__.__name__)
            self.sync_period = pd.Timedelta(sync_period_value, sync_period_unit)

            self.adjustment_goal = ErrorChecker.key_check_and_load('adjustment_goal', policy_config, self.__class__.__name__)
            self.optimizer_type = ErrorChecker.key_check_and_load('optimizer_type', policy_config, self.__class__.__name__)
            self.placement_hint = ErrorChecker.key_check_and_load('placement_hint', policy_config, self.__class__.__name__)
            self.combiner_settings = ErrorChecker.key_check_and_load('combiner', policy_config, self.__class__.__name__)

            structure_config = ErrorChecker.key_check_and_load('structure', app_config, self.__class__.__name__)
            services_config = ErrorChecker.key_check_and_load('services', app_config, self.__class__.__name__)

            # TODO: structure_config processing

            # Services settings
            for service_config in services_config:

                service_name = ErrorChecker.key_check_and_load('service_name', service_config, self.__class__.__name__)
                scaled_aspect_name = ErrorChecker.key_check_and_load('scaled_aspect_name', service_config, 'service', service_name)
                metric_descriptions = ErrorChecker.key_check_and_load('metrics_descriptions', service_config, 'service', service_name)
                metrics_descriptions = [ MetricDescription(service_name, scaled_aspect_name, md_conf) for md_conf in metric_descriptions ]

                self.services_scaling_config[service_name] = ScaledServiceScalingSettings(metrics_descriptions,
                                                                                          ErrorChecker.key_check_and_load('scaling_effect_aggregation_rule_name', service_config, 'service', service_name),
                                                                                          service_name, scaled_aspect_name)

                # TODO: platform_config processing


    def get_service_scaling_settings(self, service_name : str):

        if service_name in self.services_scaling_config:
            return self.services_scaling_config[service_name]
        elif 'default' in self.services_scaling_config:
            return self.services_scaling_config['default']
        else:
            raise ValueError(f'Scaling settings neither for "{service_name}" nor for "default" were found')
