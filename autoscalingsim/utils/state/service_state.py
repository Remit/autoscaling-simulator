import pandas as pd
import operator

from .state import ScaledEntityState
from .entity_state.entity_group import EntityGroup
from .entity_state.scaling_aspects import ScalingAspect
from .container_state.container_group import HomogeneousContainerGroup

from ..requirements import ResourceRequirements
from ..error_check import ErrorChecker

from ...infrastructure_platform.node import NodeInfo
from ...infrastructure_platform.link import NodeGroupLink
from ...infrastructure_platform.system_capacity import SystemCapacity
from ...load.request import Request
from ...application.requests_buffer import RequestsBuffer

class Deployment:

    def __init__(self,
                 service_name : str,
                 node_group : HomogeneousContainerGroup):

        self.node_group = node_group

        deployed_service_count = self.node_group.entities_state.get_entity_count(service_name)
        deployed_service_instance_resource_reqs = self.node_group.entities_state.get_entity_resource_requirements(service_name)
        cap_taken = self.node_group.container_info.resource_requirements_to_capacity(deployed_service_instance_resource_reqs)
        self.system_capacity_reserved = cap_taken * deployed_service_count

    def step(self,
             time_budget : pd.Timedelta):

        return self.node_group.step(time_budget)

    def compute_capacity_taken_by_requests(self,
                                           service_name : str,
                                           request_processing_infos : dict):

        return self.node_group.compute_capacity_taken_by_requests(service_name,
                                                                  request_processing_infos)

    def resource_requirements_to_capacity(self,
                                          res_reqs : ResourceRequirements):

        return self.node_group.resource_requirements_to_capacity(res_reqs)

    def start_processing(self,
                         req : Request):

        self.node_group.start_processing(req)

    def get_processed_for_service(self,
                                  service_name : str):

        return self.node_group.get_processed_for_service(service_name)

    def update_utilization(self,
                           service_name : str,
                           capacity_taken : SystemCapacity,
                           timestamp : pd.Timestamp,
                           averaging_interval : pd.Timedelta):

        self.node_group.update_utilization(service_name,
                                           capacity_taken,
                                           timestamp,
                                           averaging_interval)

    def get_utilization(self,
                        service_name : str,
                        resource_name : str,
                        interval : pd.Timedelta = pd.Timedelta(0, unit = 'ms')):

        return self.node_group.get_utilization(service_name, resource_name, interval)

class ServiceState:

    """
    Wraps service state for a particular region. The information present in the state
    is relevant for the scaling.
    """

    def __init__(self,
                 service_name : str,
                 init_timestamp : pd.Timestamp,
                 region_name : str,
                 averaging_interval : pd.Timedelta,
                 resource_requirements : ResourceRequirements,
                 request_processing_infos : dict,
                 buffers_config : dict,
                 sampling_interval : pd.Timedelta):

        self.service_name = service_name
        self.region_name = region_name
        self.entity_group = EntityGroup(service_name,
                                        resource_requirements)
        self.request_processing_infos = request_processing_infos
        self.averaging_interval = averaging_interval
        self.sampling_interval = sampling_interval

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

        self.deployments = {} # Container groups that this service is currently deployed on
        self.unschedulable = [] # Container gtoup IDs for groups that no new request should be scheduled on

        self.service_utilizations = {} # Utilization by different type of resource

    def get_resource_requirements(self):

        return self.entity_group.get_resource_requirements()

    def update_aspect(self,
                      aspect : ScalingAspect):

        """
        Updates the scaling aspect value. This value is stored in the entity
        group, e.g. the count of instances or the resource limit.
        """

        self.entity_group.update_aspect(aspect)

    def update_placement(self,
                         node_group : HomogeneousContainerGroup):

        self.deployments[node_group.id] = Deployment(self.service_name, node_group)

        node_group.link.set_request_processing_infos(self.request_processing_infos)
        self.upstream_buf.set_link(node_group.link)
        self.downstream_buf.set_link(node_group.link)

    def prepare_group_for_removal(self,
                                  container_group_id : int):

        """
        Adds the provided ID of the container group to the list of groups
        that are soon to be removed. This should force the simulator
        not to schedule any requests on such groups.
        """

        self.unschedulable.append(container_group_id)

    def check_out_utilization(self):

        for deployment in self.deployments.values():
            self._check_out_utilization_for_deployment(deployment)

        return self.service_utilizations

    def _check_out_utilization_for_deployment(self,
                                              deployment : Deployment):

        """
        In contrast to getting the metric values on spot for scaling purposes,
        here we a) take all the available utilization data, and b) do not normalize
        by the deployments count to show the full utilization reached over the
        lifetime of the application. It is especially useful along with the
        information on the actual number of deployed instances.
        """

        for system_resource_name in SystemCapacity.layout:
            if not system_resource_name in self.service_utilizations:
                self.service_utilizations[system_resource_name] = pd.DataFrame(columns = ['datetime', 'value']).set_index('datetime')
            deployment_util = deployment.get_utilization(self.service_name, system_resource_name) # take all the available data for the given resource
            # Aligning the time series
            common_index = deployment_util.index.union(self.service_utilizations[system_resource_name].index)
            deployment_util = deployment_util.reindex(common_index, fill_value = 0)
            self.service_utilizations[system_resource_name] = self.service_utilizations[system_resource_name].reindex(common_index, fill_value = 0)
            self.service_utilizations[system_resource_name] += deployment_util

    def force_remove_group(self,
                           container_group_id : int):

        if container_group_id in self.unschedulable:
            self.unschedulable.remove(container_group_id)

        if container_group_id in self.deployments:
            self._check_out_utilization_for_deployment(self.deployments[container_group_id])
            del self.deployments[container_group_id]

    def get_placement_parameter(self,
                                parameter : str):

        if self.placed_on_node is None:
            return None
        else:
            try:
                return self.placed_on_node.__getattribute__(parameter)
            except AttributeError:
                raise ValueError(f'Unknown parameter type {parameter}')

    def get_aspect_value(self,
                         aspect_name : str):

        return self.entity_group.get_aspect_value(aspect_name)

    # TODO: think of other kinds of metrics e.g. req count
    def get_metric_value(self,
                         metric_name : str,
                         interval : pd.Timedelta = pd.Timedelta(0, unit = 'ms')):

        service_metric_value = pd.DataFrame(columns = ['datetime', 'value']).set_index('datetime')
        # Case of resource utilization metric
        if metric_name in SystemCapacity.layout:
            for deployment in self.deployments.values():
                cur_deployment_util = deployment.get_utilization(self.service_name, metric_name, interval)
                # Aligning the time series
                common_index = cur_deployment_util.index.union(service_metric_value.index).astype(cur_deployment_util.index.dtype)
                cur_deployment_util = cur_deployment_util.reindex(common_index, fill_value = 0)
                service_metric_value = service_metric_value.reindex(common_index, fill_value = 0)
                service_metric_value += cur_deployment_util

            service_metric_value /= len(self.deployments) # normalization

        return service_metric_value

    def get_processed(self):

        processed_requests = []
        for deployment in self.deployments.values():
            processed_requests.extend(deployment.get_processed_for_service(self.service_name))

        return processed_requests

    def add_request(self,
                    req : Request):

        if req.upstream:
            self.upstream_buf.put(req)
        else:
            self.downstream_buf.put(req)

    def step(self,
             cur_timestamp : pd.Timestamp,
             simulation_step : pd.Timedelta):

        time_budget = simulation_step

        if len(self.deployments) > 0:
            self.downstream_buf.step(simulation_step)
            self.upstream_buf.step(simulation_step)

            for deployment in self.deployments.values():
                if not deployment.node_group.id in self.unschedulable:
                    time_budget = min(time_budget, deployment.step(time_budget))
                    deployment_capacity_taken = deployment.system_capacity_reserved + deployment.compute_capacity_taken_by_requests(self.service_name,
                                                                                                                                    self.request_processing_infos)

                    if not deployment_capacity_taken.is_exhausted():
                        while(time_budget > pd.Timedelta(0, unit = 'ms')):

                            # Assumption: first we try to process the downstream reqs to
                            # provide the response faster, but overall it is application-dependent
                            while ((self.downstream_buf.size() > 0) or (self.upstream_buf.size() > 0)) and not deployment_capacity_taken.is_exhausted():
                                req = self.downstream_buf.attempt_fan_in()
                                if not req is None:
                                    cap_taken = deployment.resource_requirements_to_capacity(self.request_processing_infos[req.request_type].resource_requirements)
                                    if not (deployment_capacity_taken + cap_taken).is_exhausted():
                                        req = self.downstream_buf.fan_in()
                                        deployment.start_processing(req)
                                    else:
                                        self.downstream_buf.shuffle()

                                    deployment_capacity_taken += cap_taken

                                req = self.upstream_buf.attempt_take()
                                if not req is None:
                                    cap_taken = deployment.resource_requirements_to_capacity(self.request_processing_infos[req.request_type].resource_requirements)
                                    if not (deployment_capacity_taken + cap_taken).is_exhausted():
                                        req = self.upstream_buf.take()
                                        deployment.start_processing(req)
                                    else:
                                        self.upstream_buf.shuffle()

                                    deployment_capacity_taken += cap_taken

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
                    deployment_capacity_taken = deployment.system_capacity_reserved + deployment.compute_capacity_taken_by_requests(self.service_name,
                                                                                                                                    self.request_processing_infos)
                    deployment.update_utilization(self.service_name,
                                                  deployment_capacity_taken,
                                                  cur_timestamp,
                                                  self.averaging_interval)

class ServiceStateRegionalized(ScaledEntityState):

    """
    Contains information relevant to conduct the scaling. The state should be
    updated at each simulation step and provided to the ServiceScalingPolicyHierarchy
    s.t. the scaling decision could be taken. The information stored in the
    ServiceState is diverse and satisfies any type of scaling policy that
    could be used, be it utilization-based or workload-based policy, reactive
    or predictive, etc.

    TODO:
        add properties for workload-based scaling + predictive
    """

    def __init__(self,
                 service_name : str,
                 init_timestamp : pd.Timestamp,
                 service_regions : list,
                 averaging_interval_ms,
                 resource_requirements : ResourceRequirements,
                 request_processing_infos : dict,
                 buffers_config : dict,
                 sampling_interval : pd.Timestamp):

        self.region_states = {}
        self.service_name = service_name
        for region_name in service_regions:
            self.region_states[region_name] = ServiceState(service_name,
                                                           init_timestamp,
                                                           region_name,
                                                           averaging_interval_ms,
                                                           resource_requirements,
                                                           request_processing_infos,
                                                           buffers_config,
                                                           sampling_interval)

    def add_request(self,
                    req : Request):

        if not req.region_name in self.region_states:
            raise ValueError(f'Received request with an unknown region name: {req.region_name}')

        self.region_states[req.region_name].add_request(req)

    def step(self,
             cur_timestamp : pd.Timestamp,
             simulation_step : pd.Timedelta):

        for service_state in self.region_states.values():
            service_state.step(cur_timestamp, simulation_step)

    def check_out_utilization(self):

        """
        Collects the utilization for the current state across all the regions that
        it is deployed into.
        """

        utilization_per_region = {}
        for region_name, region_state in self.region_states.items():
            utilization_per_region[region_name] = region_state.check_out_utilization()

        return utilization_per_region

    def get_resource_requirements(self,
                                  region_name : str):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].get_resource_requirements()

    def get_placement_parameter(self,
                                region_name : str,
                                parameter : str):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].get_placement_parameter(parameter)

    def update_metric(self,
                      region_name : str,
                      metric_name : str,
                      timestamp : pd.Timestamp,
                      value : float):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        self.region_states[region_name].update_metric(metric_name,
                                                      timestamp,
                                                      value)

    def update_aspect(self,
                      region_name : str,
                      aspect : ScalingAspect):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        self.region_states[region_name].update_aspect(aspect)

    def update_placement(self,
                         region_name : str,
                         node_group : HomogeneousContainerGroup):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        self.region_states[region_name].update_placement(node_group)

    def get_aspect_value(self,
                         region_name : str,
                         aspect_name : str):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].get_aspect_value(aspect_name)

    def get_metric_value(self,
                         region_name : str,
                         metric_name : str):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].get_metric_value(metric_name)

    def get_processed(self):

        """
        Binds together all the processed requests and returns them.
        """

        processed_requests = []
        for service_state in self.region_states.values():
            processed_requests.extend(service_state.get_processed())

        return processed_requests

    def prepare_groups_for_removal(self,
                                   region_name : str,
                                   node_group_ids : list):

        if region_name in self.region_states:
            for node_group_id in node_group_ids:
                self.region_states[region_name].prepare_group_for_removal(node_group_id)

    def force_remove_groups(self,
                            region_name : str,
                            node_group_ids : list):

        if region_name in self.region_states:
            for node_group_id in node_group_ids:
                self.region_states[region_name].force_remove_group(node_group_id)
