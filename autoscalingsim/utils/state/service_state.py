import operator
import pandas as pd

from .deployment import Deployment
from .entity_state.scaling_aspects import ScalingAspect
from .container_state.container_group import HomogeneousContainerGroup

from ..requirements import ResourceRequirements
from ..error_check import ErrorChecker

from ...load.request import Request
from ...application.requests_buffer import RequestsBuffer
from ...infrastructure_platform.system_capacity import SystemCapacity

class ServiceMetric:

    """
    Interface to the service-level metrics.
    """

    def __init__(self,
                 metric_name : str,
                 source_refs : list,
                 aggregation_func_on_iterable = None):

        self.metric_name = metric_name
        self.source_refs = source_refs
        self.aggregation_func = aggregation_func_on_iterable

    def get_metric_value(self):

        results_lst = []
        for source_ref in self.source_refs:
            results_lst.append(source_ref.get_metric_value(self.metric_name))

        if self.aggregation_func is None:
            return results_lst[0]
        else:
            return self.aggregation_func(results_lst)

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

    """

    def __init__(self,
                 service_name : str,
                 init_timestamp : pd.Timestamp,
                 region_name : str,
                 averaging_interval : pd.Timedelta,
                 service_instance_resource_requirements : ResourceRequirements,
                 request_processing_infos : dict,
                 buffers_config : dict,
                 sampling_interval : pd.Timedelta):

        self.service_name = service_name
        self.region_name = region_name
        self.service_instance_resource_requirements = service_instance_resource_requirements
        self.request_processing_infos = request_processing_infos
        self.averaging_interval = averaging_interval
        self.sampling_interval = sampling_interval

        self.upstream_buf = None
        self.downstream_buf = None

        self.deployments = {}
        self.unschedulable = []

        self.service_utilizations = {}

        buffer_capacity_by_request_type = {}
        buffer_capacity_by_request_type_raw = ErrorChecker.key_check_and_load('buffer_capacity_by_request_type', buffers_config, 'service', service_name)
        for buffer_capacity_config in buffer_capacity_by_request_type_raw:
            request_type = ErrorChecker.key_check_and_load('request_type', buffer_capacity_config, 'service', service_name)

            capacity = ErrorChecker.key_check_and_load('capacity', buffer_capacity_config, 'service', service_name)
            ErrorChecker.value_check('capacity', capacity, operator.gt, 0, [f'request_type {request_type}', f'service {service_name}'])

            buffer_capacity_by_request_type[request_type] = capacity

        queuing_discipline = ErrorChecker.key_check_and_load('discipline', buffers_config, 'service', service_name)

        self.upstream_buf = RequestsBuffer(buffer_capacity_by_request_type, queuing_discipline)
        self.downstream_buf = RequestsBuffer(buffer_capacity_by_request_type, queuing_discipline)

        self.service_metrics_and_sources = {
            'upstream_waiting_time': ServiceMetric(RequestsBuffer.waiting_time_metric_name, [self.upstream_buf]),
            'downstream_waiting_time': ServiceMetric(RequestsBuffer.waiting_time_metric_name, [self.downstream_buf]),
            'waiting_time': ServiceMetric(RequestsBuffer.waiting_time_metric_name, [self.upstream_buf, self.downstream_buf], sum),
            'upstream_waiting_requests_count': ServiceMetric(RequestsBuffer.waiting_requests_count_metric_name, [self.upstream_buf]),
            'downstream_waiting_requests_count': ServiceMetric(RequestsBuffer.waiting_requests_count_metric_name, [self.downstream_buf]),
            'waiting_requests_count': ServiceMetric(RequestsBuffer.waiting_requests_count_metric_name, [self.upstream_buf, self.downstream_buf], sum)
        }

    def add_request(self, req : Request):

        """
        Puts a request in one of the requests buffers depending on
        whether it moves upstream (request) or downstrem (response).
        """

        if req.upstream:
            self.upstream_buf.put(req)
        else:
            self.downstream_buf.put(req)

    def get_processed(self):

        """
        Returns the requests that were processed by any instance of the
        current service. The requests are collected across all the deployments.
        """

        processed_requests = []
        for deployment in self.deployments.values():
            processed_requests.extend(deployment.get_processed_for_service())

        return processed_requests

    def step(self,
             cur_timestamp : pd.Timestamp,
             simulation_step : pd.Timedelta):

        """
        Makes a simulation step on this service state. Making a simulation step
        includes advancing the requests waiting in the buffers and putting
        them in the node group that has enough free system resources and
        enough spare service instances running. At the end of the step,
        service utilization of the system resources is updated.
        """

        time_budget = simulation_step

        if len(self.deployments) > 0:
            self.upstream_buf.step(simulation_step)
            self.downstream_buf.step(simulation_step)

            for deployment in self.deployments.values():
                if not deployment.node_group.id in self.unschedulable:
                    time_budget = min(time_budget, deployment.step(time_budget))
                    deployment_capacity_taken = deployment.system_resources_reserved() \
                                                 + deployment.system_resources_taken_by_all_requests()

                    if not deployment_capacity_taken.is_exhausted():

                        while time_budget > pd.Timedelta(0, unit = 'ms'):
                            advancing = True

                            # Assumption: first we try to process the downstream reqs to
                            # provide the response faster, but overall it is application-dependent
                            while advancing and (self.downstream_buf.size() > 0 or self.upstream_buf.size() > 0):
                                advancing = False

                                req = self.downstream_buf.attempt_fan_in()
                                if not req is None:

                                    if deployment.can_schedule_request(req):
                                        req = self.downstream_buf.fan_in()
                                        deployment.start_processing(req)
                                        advancing = True
                                    else:
                                        self.downstream_buf.shuffle()

                                req = self.upstream_buf.attempt_take()
                                if not req is None:

                                    if deployment.can_schedule_request(req):
                                        req = self.upstream_buf.take()
                                        deployment.start_processing(req)
                                        advancing = True
                                    else:
                                        self.upstream_buf.shuffle()

                                time_budget -= simulation_step

                            if (self.downstream_buf.size() == 0) and (self.upstream_buf.size() == 0):
                                time_budget -= simulation_step

            # Post-step maintenance actions
            # Increase the cumulative time for all the reqs left in the buffers waiting
            self.upstream_buf.add_cumulative_time(simulation_step, self.service_name)
            self.downstream_buf.add_cumulative_time(simulation_step, self.service_name)

            # Update resource utilization at the end of the step once per sampling interval
            if (cur_timestamp - pd.Timestamp(0)) % self.sampling_interval == pd.Timedelta(0):
                for deployment in self.deployments.values():
                    deployment_capacity_taken = deployment.system_resources_reserved() \
                                                 + deployment.system_resources_taken_by_requests()
                    deployment.update_utilization(deployment_capacity_taken, cur_timestamp, self.averaging_interval)

    def prepare_group_for_removal(self, node_group_id : int):

        """
        Adds the provided node group ID to the list of groups
        that should not be used in scheduling the requests.
        """

        self.unschedulable.append(node_group_id)

    def force_remove_group(self, node_group_id : int):

        """
        Removes the deployment that the provided node group is used in.
        The record about this node group not being available for requests
        scheduling is also removed.
        """

        if node_group_id in self.unschedulable:
            self.unschedulable.remove(node_group_id)

        if node_group_id in self.deployments:
            self._check_out_utilization_for_deployment(self.deployments[node_group_id])
            del self.deployments[node_group_id]

    def update_placement(self, node_group : HomogeneousContainerGroup):

        """
        Creates a new deployment for the service instances with the new
        node group. The node group might be shared among multiple services.
        """

        self.deployments[node_group.id] = Deployment(self.service_name, node_group, self.request_processing_infos)

        node_group.link.set_request_processing_infos(self.request_processing_infos)
        self.upstream_buf.set_link(node_group.link)
        self.downstream_buf.set_link(node_group.link)

    def get_aspect_value(self, aspect_name : str):

        """
        Collects values for the given aspect, e.g. count, from all the node
        groups that the service instances of the current service are deployed in.
        The collected values are summed up and returned.
        """

        aspect_values_collected = []
        for deployment in self.deployments.values():
            aspect_values_collected.append(deployment.get_aspect_value(aspect_name))

        return sum(aspect_values_collected)

    def get_metric_value(self,
                         metric_name : str,
                         #cur_timestamp : pd.Timestamp,
                         interval : pd.Timedelta = pd.Timedelta(0, unit = 'ms')):

        """
        Collects and aggregates the service-related metrics, e.g. system resources
        utilization, requests arrival and processing stats.
        """

        service_metric_value = pd.DataFrame(columns = ['datetime', 'value']).set_index('datetime')
        # Case of resource utilization metric
        if metric_name in SystemCapacity.layout:
            for deployment in self.deployments.values():
                cur_deployment_util = deployment.get_utilization(metric_name, interval)

                # Aligning the time series
                common_index = cur_deployment_util.index.union(service_metric_value.index).astype(cur_deployment_util.index.dtype)
                cur_deployment_util = cur_deployment_util.reindex(common_index, fill_value = 0)
                service_metric_value = service_metric_value.reindex(common_index, fill_value = 0)
                service_metric_value += cur_deployment_util

            service_metric_value /= len(self.deployments) # normalization
        elif metric_name in self.service_metrics_and_sources:
            self.service_metrics_and_sources[metric_name].get_metric_value()
            # TODO: think of using cur_timestamp? propagate it

        return service_metric_value

    def check_out_utilization(self):

        """
        Fills up the service_utilizations field with up-to-date system
        resources utilization data from all the deployments, and returns it.
        """

        for deployment in self.deployments.values():
            self._check_out_utilization_for_deployment(deployment)

        return self.service_utilizations

    def _check_out_utilization_for_deployment(self, deployment : Deployment):

        """
        In contrast to getting the metric values on spot for scaling purposes,
        this method a) takes all the available utilization data, and b) does not
        normalize by the deployments count to show the full utilization reached
        over the lifetime of an application. The utilization data is useful
        along with the actual count of deployed node instances.
        """

        for system_resource_name in SystemCapacity.layout:
            if not system_resource_name in self.service_utilizations:
                self.service_utilizations[system_resource_name] = pd.DataFrame(columns = ['datetime', 'value']).set_index('datetime')
            deployment_util = deployment.get_utilization(system_resource_name) # take all the available data for the given resource

            # Aligning the time series
            common_index = deployment_util.index.union(self.service_utilizations[system_resource_name].index)
            deployment_util = deployment_util.reindex(common_index, fill_value = 0)
            self.service_utilizations[system_resource_name] = self.service_utilizations[system_resource_name].reindex(common_index, fill_value = 0)
            self.service_utilizations[system_resource_name] += deployment_util
