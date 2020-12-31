from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.utils.error_check import ErrorChecker

from .scalingmetric import ScalingMetricGroup, ScalingMetric
from .accessor_other_service_metric.accessor import AccessorToOtherService

class MetricGroupDescription:

    """
    "metrics_groups": [
        {
            "name": "group1",
            "priority": 1,
            "initial_max_limit": 15,
            "initial_min_limit": 1,
            "desired_aspect_value_calculator": { ... },
            "stabilizer_conf": { ... },
            "metrics": [
                {
                    "metric_source_name": "response_stats",
                    "metric_name": "buffer_time",
                    "submetric_name": "*",
                    "metric_type": "duration",
                    "metric_params": {
                      "duration_unit": "ms"
                    },
                    "values_filter_conf": { ... },
                    "values_aggregator_conf": { ... },
                    "forecaster_conf": { ... }
                }
            ]
            "default_metric_config" : {
                "values_filter_conf": { ... },
                "values_aggregator_conf": { ... },
                "forecaster_conf": { ... }
            }
        }, { ... }
    ]
    """

    def __init__(self, service_name : str, aspect_name : str, metrics_description_conf : dict):

        self.aspect_name = aspect_name

        service_key = 'service'
        self.name = ErrorChecker.key_check_and_load('name', metrics_description_conf, service_key, service_name)
        self.priority = ErrorChecker.key_check_and_load('priority', metrics_description_conf, service_key, service_name)
        self.initial_max_limit = ErrorChecker.key_check_and_load('initial_max_limit', metrics_description_conf, service_key, service_name)
        self.initial_min_limit = ErrorChecker.key_check_and_load('initial_min_limit', metrics_description_conf, service_key, service_name)
        self.desired_aspect_value_calculator_conf = ErrorChecker.key_check_and_load('desired_aspect_value_calculator_conf', metrics_description_conf, service_key, service_name)
        self.stabilizer_conf = ErrorChecker.key_check_and_load('stabilizer_conf', metrics_description_conf, service_key, service_name)

        self._metrics_descriptions = list()
        metrics_descriptions = ErrorChecker.key_check_and_load('metrics', metrics_description_conf, service_key, service_name, default = list())
        default_metric_config = ErrorChecker.key_check_and_load('default_metric_config', metrics_description_conf, service_key, service_name, default = dict())
        for metric_description_raw in metrics_descriptions:
            self._metrics_descriptions.append(MetricDescription(service_name, metric_description_raw, default_metric_config))

    def to_metric_group(self, service_name : str, region_name : str, state_reader : StateReader):

        metrics = [ metric_description.to_metric(service_name, region_name, state_reader) for metric_description in self._metrics_descriptions ]
        return ScalingMetricGroup(service_name, self.aspect_name, self.name, self.priority, region_name, state_reader, self.initial_min_limit, self.initial_max_limit,
                                  self.desired_aspect_value_calculator_conf, self.stabilizer_conf, metrics)

class MetricDescription:

    SERVICE_NAME_WILDCARD = 'default'
    SERVICE_SOURCE_WILDCARD = 'Service'

    """ Stores all the necessary information to create a scaling metric """

    def __init__(self, service_name : str, metric_description_conf : dict, default_metric_config : dict):

        self.service_name = service_name
        self.metric_source_name = ErrorChecker.key_check_and_load('metric_source_name', metric_description_conf)

        self.metric_name = ErrorChecker.key_check_and_load('metric_name', metric_description_conf)
        self.submetric_name = ErrorChecker.key_check_and_load('submetric_name', metric_description_conf, default = '')
        related_service_to_consider = ErrorChecker.key_check_and_load('related_service_to_consider', metric_description_conf, default = '')
        self.accessor_to_related_service_class = AccessorToOtherService.get(related_service_to_consider) if len(related_service_to_consider) > 0 else None

        self.values_filter_conf = ErrorChecker.key_check_and_load('values_filter_conf', metric_description_conf, default = default_metric_config.get('values_filter_conf', dict()))
        self.values_aggregator_conf = ErrorChecker.key_check_and_load('values_aggregator_conf', metric_description_conf, default = default_metric_config.get('values_aggregator_conf', dict()))
        self.forecaster_conf = ErrorChecker.key_check_and_load('forecaster_conf', metric_description_conf, default = default_metric_config.get('forecaster_conf', dict()))
        self.correlator_conf = ErrorChecker.key_check_and_load('correlator_conf', metric_description_conf, default = default_metric_config.get('correlator_conf', dict()))

    def to_metric(self, service_name : str, region_name : str, state_reader : StateReader):

        init_service_name = service_name if self.service_name == self.__class__.SERVICE_NAME_WILDCARD else self.service_name
        init_metric_source_name = service_name if self.metric_source_name == self.__class__.SERVICE_SOURCE_WILDCARD else self.metric_source_name

        if init_service_name == service_name:
            return ScalingMetric(init_service_name, region_name, init_metric_source_name, self.metric_name, self.submetric_name, state_reader,
                                 self.accessor_to_related_service_class, self.values_filter_conf, self.values_aggregator_conf, self.forecaster_conf, self.correlator_conf)
