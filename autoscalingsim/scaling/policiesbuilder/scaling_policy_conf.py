import json
import os
import numbers
import collections
import pandas as pd

from .metric.metric_description import MetricDescription
from .scaled.scaled_service_settings import ScaledServiceScalingSettings

from autoscalingsim.utils.error_check import ErrorChecker

class ScalingPolicyConfiguration:

    DEFAULT_SERVICE_NAME = 'default'

    def __init__(self, config_file : str):

        self._services_scaling_config = collections.defaultdict(ScaledServiceScalingSettings)
        self._app_structure_scaling_config = None

        if not os.path.isfile(config_file):
            raise ValueError(f'No {self.__class__.__name__} configuration file found under the path {config_file}')

        with open(config_file) as f:

            config = json.load(f)

            policy_config = ErrorChecker.key_check_and_load('policy', config, self.__class__.__name__)
            app_config = ErrorChecker.key_check_and_load('application', config, self.__class__.__name__)

            # General policy settings
            sync_period_raw = ErrorChecker.key_check_and_load('sync_period', policy_config, self.__class__.__name__)
            sync_period_value = ErrorChecker.key_check_and_load('value', sync_period_raw, self.__class__.__name__)
            sync_period_unit = ErrorChecker.key_check_and_load('unit', sync_period_raw, self.__class__.__name__)
            self._sync_period = pd.Timedelta(sync_period_value, sync_period_unit)

            structure_config = ErrorChecker.key_check_and_load('structure', app_config, self.__class__.__name__)
            related_service_to_consider = None
            if len(structure_config) > 0:
                related_service_to_consider = ErrorChecker.key_check_and_load('related_service_to_consider', structure_config, self.__class__.__name__)

            services_config = ErrorChecker.key_check_and_load('services', app_config, self.__class__.__name__)

            # Services settings
            for service_config in services_config:

                service_name = ErrorChecker.key_check_and_load('service_name', service_config, self.__class__.__name__)
                scaled_aspect_name = ErrorChecker.key_check_and_load('scaled_aspect_name', service_config, 'service', service_name)
                metric_descriptions = ErrorChecker.key_check_and_load('metrics_descriptions', service_config, 'service', service_name)
                metrics_descriptions = [ MetricDescription(service_name, scaled_aspect_name, md_conf, related_service_to_consider) for md_conf in metric_descriptions ]

                self._services_scaling_config[service_name] = ScaledServiceScalingSettings(metrics_descriptions,
                                                                                           ErrorChecker.key_check_and_load('scaling_effect_aggregation_rule_name', service_config, 'service', service_name),
                                                                                           service_name, scaled_aspect_name)


    def scaling_settings_for_service(self, service_name : str):

        if service_name in self._services_scaling_config:
            return self._services_scaling_config[service_name]
        else:
            return self._services_scaling_config[self.__class__.DEFAULT_SERVICE_NAME]

    @property
    def sync_period(self):

        return self._sync_period

    @property
    def services_scaling_config(self):

        return self._services_scaling_config.copy()
