import operator
import collections
import pandas as pd

from copy import deepcopy
from .service_metric import ServiceMetric, SumAggregator

from autoscalingsim.load.request import Request
from autoscalingsim.application.requests_buffer import RequestsBuffer
from autoscalingsim.application import buffer_utilization
from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage
from autoscalingsim.scaling.scaling_aspects import ScalingAspect
from autoscalingsim.desired_state.node_group.node_group import NodeGroup
from autoscalingsim.utils.requirements import ResourceRequirements
from autoscalingsim.utils.error_check import ErrorChecker

class ServiceState:

    """
    Represents service state in a particular region. The service state in a region
    is organized in multiple deployments, each of which binds a certain count of
    service instances to a particular node group.

    Attributes:
        service_name (str): stores the name of a service, used when initializing
            the new deployments.

        region_name (str): stores the name of the region, which the service
            owning this service state was deployed in.

        service_instance_resource_requirements (ResourceRequirements): keeps
            the resource requirements by the service instance without any
            additional requests executed by it.

        request_processing_infos (dict): stores the information relevant for
            processing the requests such as their resource usage, used when
            initializing the new deployments.

        averaging_interval (pd.Timedelta): an interval of time used for
            averaging the resource utilization data.

        sampling_interval (pd.Timedelta): a sampling interval used to
            manage the frequency of the resource utilization updates, i.e.
            the resource utilization is updated only once per this interval.

        upstream_buf (RequestsBuffer): a buffer for keeping the upstream
            requests before they can be processed by the service. Direction:
            from the user.

        downstream_buf (RequestsBuffer): a buffer for keeping the downstream
            requests (responses) before they can be processed by the service.
            Direction: to the user.

        deployments (dict): stores all the deployments access objects
            to the node groups that the instances of the current service
            are deployed on. A node group's unique id is used as a key.

        unschedulable (list): stores unique ids of the node groups that
            cannot be used in processing the incoming requests anymore, e.g.
            because they are scheduled for termination.

        service_utilizations (dict): stores the system resource utilization metrics
            (e.g. vCPU, memory) to be provided to the application. Since the
            node groups that originally accumulate these metrics can exist
            only for a limited time, this utilization information is updated
            upon removal of a deployment from the service state.

        service_metrics_and_sources (dict): stores and provides access to
            the service-level utilization metrics such as count of
            requests in one or both buffers.

    """

    def __init__(self,
                 service_name : str,
                 init_timestamp : pd.Timestamp,
                 region_name : str,
                 averaging_interval : pd.Timedelta,
                 buffers_config : dict,
                 sampling_interval : pd.Timedelta,
                 node_groups_registry : 'NodeGroupsRegistry'):

        self.service_name = service_name
        self.cur_timestamp = init_timestamp
        self.region_name = region_name
        self.averaging_interval = averaging_interval
        self.sampling_interval = sampling_interval
        self.node_groups_registry = node_groups_registry

        self.upstream_buf = None
        self.downstream_buf = None

        self.service_utilizations = {}

        buffer_capacity_by_request_type = {}
        buffer_capacity_by_request_type_raw = ErrorChecker.key_check_and_load('buffer_capacity_by_request_type', buffers_config, 'service', service_name)
        for buffer_capacity_config in buffer_capacity_by_request_type_raw:
            request_type = ErrorChecker.key_check_and_load('request_type', buffer_capacity_config, 'service', service_name)

            capacity = ErrorChecker.key_check_and_load('capacity', buffer_capacity_config, 'service', service_name)
            ErrorChecker.value_check('capacity', capacity, operator.ge, 0, [f'request_type {request_type}', f'service {service_name}'])

            buffer_capacity_by_request_type[request_type] = capacity

        queuing_discipline = ErrorChecker.key_check_and_load('discipline', buffers_config, 'service', service_name)

        self.upstream_buf = RequestsBuffer(self.service_name, buffer_capacity_by_request_type, queuing_discipline)
        self.downstream_buf = RequestsBuffer(self.service_name, buffer_capacity_by_request_type, queuing_discipline)

        self.service_metrics_and_sources = {
            'upstream_waiting_time': ServiceMetric(buffer_utilization.waiting_time_metric_name, [self.upstream_buf]),
            'downstream_waiting_time': ServiceMetric(buffer_utilization.waiting_time_metric_name, [self.downstream_buf]),
            'waiting_time': ServiceMetric(buffer_utilization.waiting_time_metric_name, [self.upstream_buf, self.downstream_buf], SumAggregator()),
            'upstream_waiting_requests_count': ServiceMetric(buffer_utilization.waiting_requests_count_metric_name, [self.upstream_buf]),
            'downstream_waiting_requests_count': ServiceMetric(buffer_utilization.waiting_requests_count_metric_name, [self.downstream_buf]),
            'waiting_requests_count': ServiceMetric(buffer_utilization.waiting_requests_count_metric_name, [self.upstream_buf, self.downstream_buf], SumAggregator())
        }

        self.node_groups_timelines = collections.defaultdict(lambda: {'start': None, 'end': None})

    def add_request(self, req : Request):

        self.upstream_buf.put(req) if req.upstream else self.downstream_buf.put(req)

    @property
    def processed(self):

        return self.node_groups_registry.processed_for_service(self.service_name, self.region_name)

    def step(self, cur_timestamp : pd.Timestamp, simulation_step : pd.Timedelta):

        """
        Makes a simulation step for this service state. Making a simulation step
        includes advancing the requests waiting in the buffers and putting
        them in the node group that has enough free system resources and
        enough spare service instances running. At the end of the step,
        service utilization of the system resources is updated.
        """

        self.cur_timestamp = cur_timestamp

        if self.node_groups_registry.is_deployed(self.service_name, self.region_name):
            self.upstream_buf.step()
            self.downstream_buf.step()

            for node_group in self.node_groups_registry.node_groups_for_service(self.service_name, self.region_name):
                node_group.step(simulation_step)
                resources_taken = node_group.system_resources_usage + node_group.system_resources_taken_by_all_requests()

                if not resources_taken.is_full:

                    advancing = True
                    # Assumption: first we try to process the downstream reqs to
                    # provide the response faster, but overall it is application-dependent
                    downstream_attempts_counter = 0
                    while advancing and (self.downstream_buf.size() > 0 or self.upstream_buf.size() > 0):
                        advancing = False

                        req = self.downstream_buf.attempt_fan_in()
                        if not req is None:
                            if node_group.can_schedule_request(req):
                                req = self.downstream_buf.fan_in()
                                node_group.start_processing(req)
                                advancing = True
                            else:
                                self.downstream_buf.shuffle()

                        else:
                            downstream_attempts_counter += 1
                            advancing = downstream_attempts_counter <= self.downstream_buf.size()
                            self.downstream_buf.shuffle()

                        req = self.upstream_buf.attempt_take()
                        if not req is None:
                            if node_group.can_schedule_request(req):
                                req = self.upstream_buf.take()
                                node_group.start_processing(req)
                                advancing = True
                            else:
                                self.upstream_buf.shuffle()

            # Post-step maintenance actions
            # Increase the cumulative time for all the reqs left in the buffers waiting
            self.upstream_buf.add_cumulative_time(simulation_step, self.service_name)
            self.downstream_buf.add_cumulative_time(simulation_step, self.service_name)

            # Update utilization metrics at the end of the step once per sampling interval
            if (cur_timestamp - pd.Timestamp(0)) % self.sampling_interval == pd.Timedelta(0):
                # Buffers
                self.upstream_buf.update_utilization(cur_timestamp, self.averaging_interval)
                self.downstream_buf.update_utilization(cur_timestamp, self.averaging_interval)

                # Service instances on node groups
                for node_group in self.node_groups_registry.node_groups_for_service(self.service_name, self.region_name):
                    resources_taken = node_group.system_resources_usage + node_group.system_resources_taken_by_requests(self.service_name)
                    node_group.update_utilization(self.service_name, resources_taken, cur_timestamp, self.averaging_interval)

    def force_remove_group(self, node_group : NodeGroup):

        if self.node_groups_timelines[node_group.id]['end'] is None:
            self.node_groups_timelines[node_group.id]['end'] = self.cur_timestamp

        self._check_out_system_resources_utilization_for_node_group(node_group)

        self.upstream_buf.detach_link(node_group.id)
        self.downstream_buf.detach_link(node_group.id)

    def update_placement(self, node_group : NodeGroup):

        if not node_group.id in self.node_groups_timelines:
            self.node_groups_timelines[node_group.id]['start'] = self.cur_timestamp

        self.upstream_buf.add_link(node_group.id, node_group.uplink)
        self.downstream_buf.add_link(node_group.id, node_group.downlink)

    def get_aspect_value(self, aspect_name : str):

        """
        Collects values for the given aspect, e.g. count, from all the node
        groups that the service instances of the current service are deployed in.
        The collected values are summed up and returned.
        """

        return self.node_groups_registry.aspect_value_for_service(aspect_name, self.service_name, self.region_name)

    def get_metric_value(self, metric_name : str,
                         interval : pd.Timedelta = pd.Timedelta(0, unit = 'ms')):

        """
        Collects and aggregates the service-related metrics, e.g. system resources
        utilization, requests arrival and processing stats.
        """

        service_metric_value = pd.DataFrame({'value': pd.Series([], dtype = 'float')}, index = pd.to_datetime([]))
        # Case of resource utilization metric
        if metric_name in SystemResourceUsage.system_resources:
            for node_group in self.node_groups_registry.node_groups_for_service(self.service_name, self.region_name):
                cur_util = node_group.utilization(self.service_name, metric_name, interval)
                service_metric_value = service_metric_value.add(cur_util, fill_value = 0)

            service_metric_value = service_metric_value.resample(self.averaging_interval).mean()
            service_metric_value /= self._derive_normalization_time_series(service_metric_value)

        elif metric_name in self.service_metrics_and_sources:
            service_metric_value = self.service_metrics_and_sources[metric_name].get_metric_value(interval)

        return service_metric_value

    def check_out_system_resources_utilization(self):

        """
        Fills up the service_utilizations field with up-to-date system
        resources utilization data from all the deployments, and returns it.
        """

        for node_group in self.node_groups_registry.node_groups_for_service(self.service_name, self.region_name):
            self._check_out_system_resources_utilization_for_node_group(node_group)

        for system_resource_name in self.service_utilizations.keys():
            self.service_utilizations[system_resource_name] = self.service_utilizations[system_resource_name].astype(float).resample(self.averaging_interval).mean()
            self.service_utilizations[system_resource_name] /= self._derive_normalization_time_series(self.service_utilizations[system_resource_name])

        return self.service_utilizations

    def _check_out_system_resources_utilization_for_node_group(self, node_group : 'NodeGroup'):

        """
        In contrast to getting the metric values on spot for scaling purposes,
        this method a) takes all the available utilization data, and b) does not
        normalize by the deployments count to show the full utilization reached
        over the lifetime of an application. The utilization data is useful
        along with the actual count of deployed node instances.
        """

        for system_resource_name in SystemResourceUsage.system_resources:
            if not system_resource_name in self.service_utilizations:
                self.service_utilizations[system_resource_name] = pd.DataFrame({'value': pd.Series([], dtype = 'float')}, index = pd.to_datetime([]))
            node_group_util = node_group.utilization(self.service_name, system_resource_name, pd.Timedelta(0, unit = 'ms'))#self.averaging_interval)

            if not node_group_util is None:
                self.service_utilizations[system_resource_name] = self.service_utilizations[system_resource_name].add(node_group_util, fill_value = 0)

    def _derive_normalization_time_series(self, metric_vals : pd.DataFrame):

        if not metric_vals.empty:

            metric_start = metric_vals.index.min()
            metric_end = metric_vals.index.max()

            node_groups_count = pd.DataFrame({'value': pd.Series([], dtype = 'float')}, index = pd.to_datetime([]))
            for node_group_existence_intervals in self.node_groups_timelines.values():
                ng_start = max(node_group_existence_intervals['start'], metric_start) if node_group_existence_intervals['start'] <= metric_end else None
                if node_group_existence_intervals['end'] is None:
                    ng_end = metric_end
                else:
                    ng_end = min(node_group_existence_intervals['end'], metric_end) if node_group_existence_intervals['end'] >= metric_start else None
                if not ng_start is None and not ng_end is None:
                    index_for_ng_counts = pd.date_range(ng_start, ng_end, freq = metric_vals.index.freq)
                    node_group_count_ts = pd.DataFrame({'value': [1.0] * len(index_for_ng_counts)}, index = index_for_ng_counts)
                    node_groups_count = node_groups_count.add(node_group_count_ts, fill_value = 0)

            node_groups_normalization_ts = pd.DataFrame({'value': [1.0] * metric_vals.shape[0]},
                                                        index = pd.date_range(metric_start, periods = metric_vals.shape[0], freq = metric_vals.index.freq))

            return pd.DataFrame({'value': node_groups_normalization_ts.merge(node_groups_count, how = 'outer', left_index = True, right_index = True).max(axis = 1)})

        else:

            return 1.0

    def _count_service_instances(self):

        return self.node_groups_registry.count_node_groups_for_service(self.service_name, self.region_name)
