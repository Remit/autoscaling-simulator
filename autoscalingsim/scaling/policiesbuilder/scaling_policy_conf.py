import json
import numbers
import pandas as pd

from .metric.scalingmetric import MetricDescription
from .metric.valuesfilter import ValuesFilter
from .metric.valuesaggregator import ValuesAggregator
from .metric.stabilizer import Stabilizer
from .metric.forecasting import MetricForecaster

from .scaled.scaled_service_settings import ScaledServiceScalingSettings

from ...utils.error_check import ErrorChecker

class ScalingPolicyConfiguration:

    """
    Wraps the configuration of the scaling policy extracted from the configuration
    file.
    """

    def __init__(self, config_file : str):

        self.app_structure_scaling_config = None
        self.services_scaling_config = {}
        self.platform_scaling_config = None

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
                service_key = 'service'
                service_name = ErrorChecker.key_check_and_load(service_key, service_config, self.__class__.__name__)
                scaled_service_name = ErrorChecker.key_check_and_load('scaled_service_name', service_config, service_key, service_name)
                scaled_aspect_name = ErrorChecker.key_check_and_load('scaled_aspect_name', service_config, service_key, service_name)

                metrics_descriptions = []
                metric_descriptions_json = ErrorChecker.key_check_and_load('metrics_descriptions', service_config, service_key, service_name)
                for metric_description_json in metric_descriptions_json:

                    metric_source_name = ErrorChecker.key_check_and_load('metric_source_name', metric_description_json, service_key, service_name)
                    metric_name = ErrorChecker.key_check_and_load('metric_name', metric_description_json, service_key, service_name)

                    # TODO: think of non-obligatory parameters that can be identified as none
                    values_filter_conf = ErrorChecker.key_check_and_load('values_filter_conf', metric_description_json, service_key, service_name)
                    values_aggregator_conf = ErrorChecker.key_check_and_load('values_aggregator_conf', metric_description_json, service_key, service_name)
                    stabilizer_conf = ErrorChecker.key_check_and_load('stabilizer_conf', metric_description_json, service_key, service_name)
                    forecaster_conf = ErrorChecker.key_check_and_load('forecaster_conf', metric_description_json, service_key, service_name)
                    capacity_adaptation_type = ErrorChecker.key_check_and_load('capacity_adaptation_type', metric_description_json, service_key, service_name)
                    timing_type = ErrorChecker.key_check_and_load('timing_type', metric_description_json, service_key, service_name)
                    target_value = ErrorChecker.key_check_and_load('target_value', metric_description_json, service_key, service_name)
                    priority = ErrorChecker.key_check_and_load('priority', metric_description_json, service_key, service_name)
                    initial_max_limit = ErrorChecker.key_check_and_load('initial_max_limit', metric_description_json, service_key, service_name)
                    initial_min_limit = ErrorChecker.key_check_and_load('initial_min_limit', metric_description_json, service_key, service_name)
                    initial_service_representation_in_metric = ErrorChecker.key_check_and_load('initial_service_representation_in_metric', metric_description_json, service_key, service_name)

                    metric_descr = MetricDescription(scaled_service_name,
                                                     scaled_aspect_name,
                                                     metric_source_name,
                                                     metric_name,
                                                     values_filter_conf,
                                                     values_aggregator_conf,
                                                     target_value,
                                                     stabilizer_conf,
                                                     timing_type,
                                                     forecaster_conf,
                                                     capacity_adaptation_type,
                                                     priority,
                                                     initial_max_limit,
                                                     initial_min_limit,
                                                     initial_service_representation_in_metric)

                    metrics_descriptions.append(metric_descr)

                scaling_effect_aggregation_rule_name = ErrorChecker.key_check_and_load('scaling_effect_aggregation_rule_name', service_config, service_key, service_name)
                self.services_scaling_config[service_name] = ScaledServiceScalingSettings(metrics_descriptions,
                                                                                          scaling_effect_aggregation_rule_name,
                                                                                          scaled_service_name,
                                                                                          scaled_aspect_name)

                # TODO: platform_config processing


    def get_service_scaling_settings(self, service_name : str):

        if service_name in self.services_scaling_config:
            return self.services_scaling_config[service_name]
        elif 'default' in self.services_scaling_config:
            return self.services_scaling_config['default']
        else:
            raise ValueError(f'Scaling settings neither for "{service_name}" nor for "default" were found')
