import pandas as pd

from .filtering.valuesfilter import ValuesFilter
from .aggregation.valuesaggregator import ValuesAggregator
from .stabilization.stabilizer import Stabilizer
from .forecasting.forecaster import MetricForecaster
from .limiter import Limiter

from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.desired_state.service_group.group_of_services_reg import GroupOfServicesRegionalized
from autoscalingsim.scaling.scaling_aspects import ScalingAspect
from autoscalingsim.utils.error_check import ErrorChecker

class MetricDescription:

    """
    Stores all the necessary information to create a scaling metric.
    """

    # First value in the list is the default
    reference_configs_dict = {
        'capacity_adaptation_type': ['discrete', 'continuous'],
        'timing_type': ['reactive', 'predictive']
    }

    def __init__(self,
                 service_name,
                 aspect_name,
                 metric_source_name,
                 metric_name,
                 metric_type,
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
                 initial_service_representation_in_metric):

        self.service_name = service_name
        self.aspect_name = aspect_name
        self.metric_source_name = metric_source_name
        self.metric_name = metric_name
        self.metric_type = metric_type
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
        self.initial_service_representation_in_metric = initial_service_representation_in_metric
        self.state_reader = None

    def convert_to_metric(self,
                          regions : list,
                          service_name : str = None,
                          metric_source_name : str = None,
                          state_reader : StateReader = None):

        if service_name is None:
            service_name = self.service_name

        if metric_source_name is None:
            metric_source_name = self.metric_source_name

        if state_reader is None:
            state_reader = self.state_reader

        return ScalingMetricRegionalized(regions,
                                         service_name,
                                         self.aspect_name,
                                         metric_source_name,
                                         self.metric_name,
                                         self.metric_type,
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
                                         self.initial_service_representation_in_metric,
                                         state_reader)

class ScalingMetricRegionalized:

    """
    Wraps a metric that spans over multiple regions.

    TODO:
    -- consider adding an opportunity to provide different metrics configurations per region
    """

    def __init__(self,
                 regions : list,
                 service_name : str,
                 aspect_name : str,
                 metric_source_name : str,
                 metric_name : str,
                 metric_type : type,
                 values_filter_conf : dict,
                 values_aggregator_conf : dict,
                 target_value : float,
                 stabilizer_conf : dict,
                 timing_type : str,
                 forecaster_conf : dict,
                 capacity_adaptation_type : str,
                 priority : int,
                 max_limit : float,
                 min_limit : float,
                 service_representation_in_metric : float,
                 state_reader : StateReader):

        # Description of the service to scale, includes:
        # - the name of the service to scale, e.g. service name
        # - the name of the service's aspect to scale, e.g. instance count
        self.service_name = service_name
        self.aspect_name = aspect_name

        # Description of the metric used for scaling, includes:
        # - the name of the metric source, e.g. service name or the name of some external service
        # - the name of the metric in the metric source specified by the name above
        self.metric_source_name = metric_source_name
        self.metric_name = metric_name

        self.state_reader = state_reader

        max_limit_aspect = ScalingAspect.get(self.aspect_name)(max_limit)
        min_limit_aspect = ScalingAspect.get(self.aspect_name)(min_limit)

        self.metrics_per_region = {}
        for region_name in regions:
            self.metrics_per_region[region_name] = ScalingMetric(region_name,
                                                                 metric_type,
                                                                 values_filter_conf,
                                                                 values_aggregator_conf,
                                                                 target_value,
                                                                 stabilizer_conf,
                                                                 timing_type,
                                                                 forecaster_conf,
                                                                 capacity_adaptation_type,
                                                                 priority,
                                                                 max_limit_aspect,
                                                                 min_limit_aspect,
                                                                 service_representation_in_metric)

    def __call__(self):

        """
        Computes desired aspect value according to the metric for each region.
        The returned dataframe is transformed into an aggregated timeline of
        regionalized services state. In each services state there is information
        only for a single service since the metric is associated with a single service.
        The aggregation across multiple services happends at the Scaling Manager.
        """

        regionalized_desired_ts_raw = {}
        for region_name, metric in self.metrics_per_region.items():

            metric_vals = self.state_reader.get_metric_value(self.service_name,
                                                             region_name,
                                                             self.metric_name)

            cur_aspect_val = self.state_reader.get_aspect_value(self.service_name,
                                                                region_name,
                                                                self.aspect_name)

            service_res_reqs = self.state_reader.get_resource_requirements(self.service_name,
                                                                          region_name)

            desired_scaled_aspect_val_pr = metric(metric_vals, cur_aspect_val)

            for timestamp, row_val in desired_scaled_aspect_val_pr.iterrows():
                aspects_dict = {}
                for aspect in row_val:
                    aspects_dict[aspect.name] = aspect
                if not timestamp in regionalized_desired_ts_raw:
                    regionalized_desired_ts_raw[timestamp] = {}
                regionalized_desired_ts_raw[timestamp][region_name] = {}
                regionalized_desired_ts_raw[timestamp][region_name][self.service_name] = aspects_dict

        timeline_of_regionalized_desired_states = {}
        for timestamp, regionalized_desired_val in regionalized_desired_ts_raw.items():
            timeline_of_regionalized_desired_states[timestamp] = GroupOfServicesRegionalized(regionalized_desired_val,
                                                                                            {self.service_name: service_res_reqs})

        return timeline_of_regionalized_desired_states

class ScalingMetric:

    """
    Abstract description of a metric used to determine by how much should the
    associated service be scaled. For instance, the ScalingMetric could be the
    CPU utilization. The associated Scaledservice that contains the metric
    could be the node (= VM). Decouples scaling metric from the scaled aspect -
    the metric can come from entirely different source, e.g. external.
    """

    def __init__(self,
                 region_name : str,
                 metric_type : type,
                 values_filter_conf : dict,
                 values_aggregator_conf : dict,
                 target_value : float,
                 stabilizer_conf : dict,
                 timing_type : str,
                 forecaster_conf : dict,
                 capacity_adaptation_type : str,
                 priority : int,
                 max_limit_aspect : ScalingAspect,
                 min_limit_aspect : ScalingAspect,
                 service_representation_in_metric : float):

        # Static state
        self.region_name = region_name
        self.metric_type = metric_type

        # Metric values preprocessing:
        # a filter that takes some of the metrics values depending
        # on the criteria in it, e.g. takes only values for last 5 seconds.
        # Should be callable.
        # TODO: consider filter chains
        vf_name = ErrorChecker.key_check_and_load('name', values_filter_conf, 'region', region_name)
        vf_conf = ErrorChecker.key_check_and_load('config', values_filter_conf, 'region', region_name)
        self.values_filter = ValuesFilter.get(vf_name)(vf_conf)

        # an aggregator applicable to the time series metrics values
        # -- it aggregates metric values prior to the comparison
        # against the target. Should be callable.
        # TODO: consider values aggregators chains
        va_name = ErrorChecker.key_check_and_load('name', values_aggregator_conf, 'region', region_name)
        va_conf = ErrorChecker.key_check_and_load('config', values_aggregator_conf, 'region', region_name)
        self.values_aggregator = ValuesAggregator.get(va_name)(va_conf, self.metric_type)

        # Scaling determinants:
        # integer value that determines the position of the metric in the
        # sequence of metrics used to scale the service; the higher the priority
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
        stab_name = ErrorChecker.key_check_and_load('name', stabilizer_conf, 'region', region_name)
        stab_conf = ErrorChecker.key_check_and_load('config', stabilizer_conf, 'region', region_name)
        self.stabilizer = Stabilizer.get(stab_name)(stab_conf)

        # either predictive or reactive; depending on the value
        # either the real metric value or its forecast is used.
        # The use of the "predictive" demands presence of the
        # forecaster.
        self.timing_type = timing_type
        self.forecaster = MetricForecaster(timing_type, forecaster_conf, self.metric_type)

        # either continuous (for vertical scaling) or discrete (for
        # horizontal scaling)
        self.capacity_adaptation_type = capacity_adaptation_type

        # Dynamic state

        # current min-max limits on the post-scaling result for the
        # aspect of the scaled service (count of scaled services in case of horizontal scaling
        # or resource limits of scaled services in case of vertical scaling)
        self.limiter = Limiter(min_limit_aspect, max_limit_aspect)

        # current representation of the service in terms of metric, for instance
        # if the service is the node and the metric is CPU utilization, and the capacity adaptation type is discrete,
        # then the representation may be 1 which means that 100 utilization translates
        # into 1 node of the given type. This property sits in dynamic category since
        # it might get changed during the predictive autoscaling over time with
        # new information becoming available about how the services react on the change
        # in metric, e.g. if the metric is the requests per second and the service is node,
        # there is no universal correspondence for every app and every request type.
        self.service_representation_in_metric = service_representation_in_metric

    def __call__(self, cur_metric_vals : pd.DataFrame, cur_aspect_val : ScalingAspect):

        """
        Computes the desired state of the associated scaled service (e.g. service)
        according to this particular metric.
        """

        if cur_metric_vals.shape[0] > 0:
            # Extracts available metric values in a form of pandas DataFrame
            # with the datetime index. Can be a single value or a sequence of
            # values (in this case, some metric history is incorporated)

            metric_vals = cur_metric_vals
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
            # reflects the needs of the scaled service in terms of this metric.
            # The computed ratio is used to calaculate the desired amount of the
            # scaled aspect (e.g. CPU shares or service instances) by using
            # the representation of the metric in terms of the scaled service and
            # the current amount of the scaled service. Lastly, the computed
            # desired amount of the scaled aspect is stabilized to avoid
            # oscillations in it that may cause too much overhead when scaling.
            metric_ratio = aggregated_metric_vals / self.target_value
            desired_scaled_aspect = cur_aspect_val * metric_ratio * self.service_representation_in_metric
            desired_scaled_aspect_stabilized = self.stabilizer(desired_scaled_aspect)

            #print(f'metric_ratio: {metric_ratio}')
            #print(f'desired_scaled_aspect: {desired_scaled_aspect}')
            #print(f'desired_scaled_aspect_stabilized: {desired_scaled_aspect_stabilized}')

            # Limiting the produced values of the desired scaled aspect
            # such that it stays inside the given band. The limiting
            # post-processing is useful if there is a strict limit
            # on a particular scaled aspect (e.g. number of VM instances)
            # or if the adjustments to the desired scaled aspect must proceed
            # in a chained way using different metrics -> min/max limits
            # then serve as a communication channel.
            desired_scaled_aspect_stabilized_limited = self.limiter(desired_scaled_aspect_stabilized) # TODO: check with scaling aspects

            return desired_scaled_aspect_stabilized_limited
        else:
            return pd.DataFrame()

    def update_limits(self, new_min, new_max):

        if not limiter is None:
            self.limiter.update_limits(new_min, new_max)
