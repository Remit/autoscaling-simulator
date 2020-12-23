import collections

from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.utils.metric_converter import MetricConverter
from autoscalingsim.utils.error_check import ErrorChecker
from autoscalingsim.utils.metric_units_registry import MetricUnitsRegistry

from .scalingmetric import ScalingMetricRegionalized
from .accessor_other_service_metric.accessor import AccessorToOtherService

class MetricSettingsPerRegion:

    def __init__(self, metric_converter : MetricConverter, metric_unit_type, values_filter_conf : dict,
                 values_aggregator_conf : dict, desired_aspect_value_calculator_conf : dict, correlator_conf : dict, stabilizer_conf : dict,
                 forecaster_conf : dict, priority : int, max_limit : float, min_limit : float):

        self.metric_converter = metric_converter
        self.metric_unit_type = metric_unit_type
        self.values_filter_conf = values_filter_conf
        self.values_aggregator_conf = values_aggregator_conf
        self.desired_aspect_value_calculator_conf = desired_aspect_value_calculator_conf
        self.correlator_conf = correlator_conf
        self.stabilizer_conf = stabilizer_conf
        self.forecaster_conf = forecaster_conf
        self.priority = priority
        self.max_limit = max_limit
        self.min_limit = min_limit

class MetricDescription:

    """ Stores all the necessary information to create a scaling metric """

    def __init__(self, service_name : str, aspect_name : str, metric_description_conf : dict):

        self.service_name = service_name
        self.aspect_name = aspect_name

        service_key = 'service'
        self.metric_source_name = ErrorChecker.key_check_and_load('metric_source_name', metric_description_conf, service_key, service_name)
        self.metric_name = ErrorChecker.key_check_and_load('metric_name', metric_description_conf, service_key, service_name)
        self.submetric_name = ErrorChecker.key_check_and_load('submetric_name', metric_description_conf, service_key, service_name, default = '')
        metric_type = ErrorChecker.key_check_and_load('metric_type', metric_description_conf, service_key, service_name, default = 'noop')
        metric_params = ErrorChecker.key_check_and_load('metric_params', metric_description_conf, service_key, service_name, default = {})
        self.metric_converter = MetricConverter.get(metric_type)(metric_params)
        self.metric_unit_type = MetricUnitsRegistry.get(metric_type)
        related_service_to_consider = ErrorChecker.key_check_and_load('related_service_to_consider', metric_description_conf, service_key, service_name, default = '')
        self.accessor_to_related_service_class = AccessorToOtherService.get(related_service_to_consider) if len(related_service_to_consider) > 0 else None

        self.values_filter_conf = ErrorChecker.key_check_and_load('values_filter_conf', metric_description_conf, service_key, service_name)
        self.values_aggregator_conf = ErrorChecker.key_check_and_load('values_aggregator_conf', metric_description_conf, service_key, service_name)
        self.stabilizer_conf = ErrorChecker.key_check_and_load('stabilizer_conf', metric_description_conf, service_key, service_name)
        self.forecaster_conf = ErrorChecker.key_check_and_load('forecaster_conf', metric_description_conf, service_key, service_name)
        self.desired_aspect_value_calculator_conf = ErrorChecker.key_check_and_load('desired_aspect_value_calculator_conf', metric_description_conf, service_key, service_name)
        self.correlator_conf = ErrorChecker.key_check_and_load('correlator_conf', metric_description_conf, service_key, service_name, default = dict())

        self.priority = ErrorChecker.key_check_and_load('priority', metric_description_conf, service_key, service_name)
        self.initial_max_limit = ErrorChecker.key_check_and_load('initial_max_limit', metric_description_conf, service_key, service_name)
        self.initial_min_limit = ErrorChecker.key_check_and_load('initial_min_limit', metric_description_conf, service_key, service_name)

        self.state_reader = None

    def to_regionalized_metric(self, regions : list, service_name : str = None,
                               metric_source_name : str = None, state_reader : StateReader = None):

        if service_name is None:
            service_name = self.service_name

        if metric_source_name is None:
            metric_source_name = self.metric_source_name

        if state_reader is None:
            state_reader = self.state_reader

        per_region_settings = MetricSettingsPerRegion( self.metric_converter, self.metric_unit_type, self.values_filter_conf,
                                                       self.values_aggregator_conf, self.desired_aspect_value_calculator_conf,
                                                       self.correlator_conf, self.stabilizer_conf, self.forecaster_conf,
                                                       self.priority, self.initial_max_limit, self.initial_min_limit )

        return ScalingMetricRegionalized(regions, service_name, self.aspect_name,
                                         metric_source_name, self.metric_name, self.submetric_name,
                                         per_region_settings, state_reader, self.accessor_to_related_service_class)
