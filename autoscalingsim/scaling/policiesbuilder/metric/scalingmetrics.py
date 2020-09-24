from .valuesfilter import *
from .valuesaggregator import *
from .stabilizer import *
from .forecasting import *
from .limiter import *

class MetricDescription:

    """
    Stores all the necessary information to create a scaling metric.
    """

    def __init__(self,
                 scaled_entity_name,
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
                 initial_entity_representation_in_metric):

        self.scaled_entity_name = scaled_entity_name
        self.scaled_aspect_name = scaled_aspect_name
        self.metric_source_name = metric_source_name
        self.metric_name = metric_name
        self.values_filter_conf = values_filter_conf
        self.values_aggregator_conf = values_aggregator_conf
        self.target_value = target_value
        self.stabilizer_conf = stabilizer_conf
        self.timing_type = timing_type
        self.forecaster_conf = forecaster_conf
        self.capacity_adaptation_type = capacity_adaptation_type
        self.priority = priority
        self.initial_max_limit = initial_max_limit
        self.initial_min_limit = initial_min_limit
        self.initial_entity_representation_in_metric = initial_entity_representation_in_metric

    def convert_to_metric(self):

        return ScalingMetric(self.scaled_entity_name,
                             self.scaled_aspect_name,
                             self.metric_source_name,
                             self.metric_name,
                             self.values_filter_conf,
                             self.values_aggregator_conf,
                             self.target_value,
                             self.stabilizer_conf,
                             self.timing_type,
                             self.forecaster_conf,
                             self.capacity_adaptation_type,
                             self.priority,
                             self.initial_max_limit,
                             self.initial_min_limit,
                             self.initial_entity_representation_in_metric)

class ScalingMetric:

    """
    Abstract description of a metric used to determine by how much should the
    associated entity be scaled. For instance, the ScalingMetric could be the
    CPU utilization. The associated ScaledEntity that contains the metric
    could be the node (= VM). Decouples scaling metric from the scaled aspect -
    the metric can come from entirely different source, e.g. external.
    """

    # First value in the list is the default
    reference_configs_dict = {
        'capacity_adaptation_type': ['discrete', 'continuous'],
        'timing_type': ['reactive', 'predictive']
    }

    def __init__(self,
                 scaled_entity_name,
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
                 max_limit,
                 min_limit,
                 entity_representation_in_metric):
        # Static state
        # Description of the entity to scale, includes:
        # - the name of the entity to scale, e.g. service name
        # - the name of the entity's aspect to scale, e.g. instance count
        self.scaled_entity_name = scaled_entity_name
        self.scaled_aspect_name = scaled_aspect_name

        # Description of the metric used for scaling, includes:
        # - the name of the metric source, e.g. service name or the name of some external entity
        # - the name of the metric in the metric source specified by the name above
        self.metric_source_name = metric_source_name
        self.metric_name = metric_name

        # Metric values preprocessing:
        # a filter that takes some of the metrics values depending
        # on the criteria in it, e.g. takes only values for last 5 seconds.
        # Should be callable.
        # TODO: consider filter chains
        self.values_filter = value_filter_registry[values_filter_conf['name']](values_filter_conf['config'])

        # an aggregator applicable to the time series metrics values
        # -- it aggregates metric values prior to the comparison
        # against the target. Should be callable.
        # TODO: consider values aggregators chains
        self.values_aggregator = value_aggregator_registry[values_aggregator_conf['name']](values_aggregator_conf['config'])

        # Scaling determinants:
        # integer value that determines the position of the metric in the
        # sequence of metrics used to scale the entity; the higher the priority
        # the closer is the metric to the beginning of the sequence, e.g. if
        # there are metrics CPU with the priority 15 and memory with the priority
        # -5, then the sequential aggregation thereof results in first computing
        # the scaling action based on the CPU utilization, and then using
        # these results as limits for the computation based on memory.
        self.priority = priority

        # a target value of the metric; comparison of the filtered and
        # aggregated value of the metric against the target may
        # result in the scaling depending on whether the predicate allows
        # this or that scaling action.
        self.target_value = target_value

        # used to stabilize scaling actions over time, e.g. if the scaling happens
        # many times over a short period of time, various start-up and termination
        # effects may severely impact the response time latency as well as other metrics
        self.stabilizer = value_stabilizer_registry[stabilizer_conf['name']](stabilizer_conf['config'])

        # either predictive or reactive; depending on the value
        # either the real metric value or its forecast is used.
        # The use of the "predictive" demands presence of the
        # forecaster.
        self.timing_type = timing_type
        self.forecaster = MetricForecaster(fhorizon_in_steps = forecaster_conf['fhorizon_in_steps'],
                                           forecasting_model_name = forecaster_conf['name'],
                                           forecasting_model_params = forecaster_conf['config'],
                                           resolution_ms = forecaster_conf['resolution_ms'],
                                           history_data_buffer_size = forecaster_conf['history_data_buffer_size'])

        # either continuous (for vertical scaling) or discrete (for
        # horizontal scaling)
        self.capacity_adaptation_type = capacity_adaptation_type

        # Dynamic state
        # reference to the metric manager that acts as an interface for all the values requests
        # from the side of the ScalingMetric, both for scaling metric and scaled aspect
        # Initialized and given to the ScalingMetric after its creation since it contains
        # the list of references to all the relevant entities/informers.
        self.metric_manager = None

        # current min-max limits on the post-scaling result for the
        # aspect of the scaled entity (count of scaled entities in case of horizontal scaling
        # or resource limits of scaled entities in case of vertical scaling)
        self.limiter = Limiter(min_limit, max_limit)

        # current representation of the entity in terms of metric, for instance
        # if the entity is the node and the metric is CPU utilization, and the capacity adaptation type is discrete,
        # then the representation may be 1 which means that 100 utilization translates
        # into 1 node of the given type. This property sits in dynamic category since
        # it might get changed during the predictive autoscaling over time with
        # new information becoming available about how the entities react on the change
        # in metric, e.g. if the metric is the requests per second and the entity is node,
        # there is no universal correspondence for every app and every request type.
        self.entity_representation_in_metric = entity_representation_in_metric

    def __call__(self):

        """
        Computes the desired state of the associated scaled entity (e.g. service)
        according to this particular metric.
        """

        # Extracts available metric values in a form of pandas DataFrame
        # with the datetime index. Can be a single value or a sequence of
        # values (in this case, some metric history is incorporated)
        metric_vals = self.entity_ref.get_metric_vals(self.metric_name)
        if self.timing_type == 'predictive':
            metric_vals = self.forecaster(cur_metric_vals)

        # Filtering raw metric values (e.g. by removing NA or some
        # abnormal values, or by smoothing the signal) and aggregating
        # these filtered values to produce the desired aggregated metric,
        # e.g. by averaging with the sliding window of a particular length
        filtered_metric_vals = self.values_filter(metric_vals)
        aggregated_metric_vals = self.values_aggregator(filtered_metric_vals)

        # Computing how does metric value related to the target --
        # the assumption is that the closer it is to the target value,
        # the better the current state of the application/infrastructure
        # reflects the needs of the scaled entity in terms of this metric.
        # The computed ratio is used to calaculate the desired amount of the
        # scaled aspect (e.g. CPU shares or service instances) by using
        # the representation of the metric in terms of the scaled entity and
        # the current amount of the scaled entity. Lastly, the computed
        # desired amount of the scaled aspect is stabilized to avoid
        # oscillations in it that may cause too much overhead when scaling.
        metric_ratio = aggregated_metric_vals / self.target_value
        # TODO: below
        desired_scaled_aspect = math.ceil(metric_ratio * self.entity_representation_in_metric * self.scaled_aspect_source)
        desired_scaled_aspect_stabilized = self.stabilizer(desired_scaled_aspect)

        # Limiting the produced values of the desired scaled aspect
        # such that it stays inside the given band. The limiting
        # post-processing is useful if there is a strict limit
        # on a particular scaled aspect (e.g. number of VM instances)
        # or if the adjustments to the desired scaled aspect must proceed
        # in a chained way using different metrics -> min/max limits
        # then serve as a communication channel.
        desired_scaled_aspect_stabilized_limited = self.limiter(desired_scaled_aspect_stabilized)

        return desired_scaled_aspect_stabilized_limited

    def update_limits(self,
                      new_min,
                      new_max):

        if not limiter is None:
            self.limiter.update_limits(new_min,
                                       new_max)

    @staticmethod
    def config_check(config_raw,
                     name_to_check):

        config_res = ScalingMetric.reference_configs_dict[name_to_check][0] # default
        if name_to_check in config_raw:
            if config_raw[name_to_check] in ScalingMetric.reference_configs_dict[name_to_check]:
                config_res = config_raw[name_to_check]
            else
                raise ValueError('Value {} of the config parameter {} is unknown for class {}'.format(config_raw[name_to_check],
                                                                                                      name_to_check,
                                                                                                      __class__.__name__))

        return config_res
