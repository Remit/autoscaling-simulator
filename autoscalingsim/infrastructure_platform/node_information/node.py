import pandas as pd

from ..system_resource_usage import SystemResourceUsage

from ...scaling.policiesbuilder.scaled.scaled_container import ScaledContainer

from ...utils.size import Size
from ...utils.requirements import ResourceRequirements
from ...utils.state.entity_state.entity_group import EntitiesState

class NodeInfo(ScaledContainer):

    """
    Holds static information about the node type, e.g. virtual machine.
    NodeInfo realizes interface of the ScaledEntityContainer class to
    fulfill the expectations of the adjusting algorithms when scaling.
    """

    def __init__(self,
                 provider : str,
                 node_type : str,
                 vCPU : int,
                 memory : Size,
                 disk : Size,
                 network_bandwidth : Size,
                 price_p_h : float = 0.0,
                 cpu_credits_h : int = 0,
                 latency : pd.Timedelta = pd.Timedelta(0, unit = 'ms'),
                 requests_acceleration_factor : float = 1.0,
                 labels : list = []):

        self.provider = provider
        self.node_type = node_type
        self.vCPU = vCPU
        self.memory = memory
        self.disk = disk
        self.network_bandwidth = network_bandwidth
        self.price_p_h = price_p_h
        self.cpu_credits_h = cpu_credits_h
        self.latency = latency
        self.requests_acceleration_factor = requests_acceleration_factor
        self.labels = labels

    def __repr__(self):

        return f'{self.__class__.__name__}(provider = {self.provider}, \
                                           node_type = {self.node_type}, \
                                           vCPU = {self.vCPU}, \
                                           memory = {repr(self.memory)}, \
                                           disk = {repr(self.disk)}, \
                                           network_bandwidth = {repr(self.network_bandwidth)}, \
                                           price_p_h = {self.price_p_h}, \
                                           cpu_credits_h = {self.cpu_credits_h}, \
                                           latency = {self.latency}, \
                                           requests_acceleration_factor = {self.requests_acceleration_factor}, \
                                           labels = {self.labels})'

    def system_resources_to_take_from_requirements(self, res_requirements : ResourceRequirements) -> SystemResourceUsage:

        """
        Computes system resource usage that will be taken on this type of node
        if the resource requirements provided in the parameter are to be fulfilled.
        For instance, the resource requirements can be provided for a request.
        """

        return SystemResourceUsage(self, 1, res_requirements.to_dict())

    def entities_require_system_resources(self, entities_state : EntitiesState,
                                          instances_count : int = 1) -> tuple:

        """
        Calculates system resource usage by the entities if they
        are to be accommodated on the node. If no state is provided, each
        entity is assumed to have a single instance. Otherwise, the instance
        count is taken from the state. In addition, the method returns whether
        the entities can at all be accommodated by this node type.
        """

        if not isinstance(entities_state, EntitiesState):
            raise TypeError(f'Unexpected type provided to compute the required capacity: {type(entities_state)}')

        requirements_by_entity = entities_state.get_entities_requirements()
        counts_by_entity = entities_state.get_entities_counts()

        joint_resource_requirements = ResourceRequirements.new_empty_resource_requirements()
        for entity_name, requirements in requirements_by_entity.items():
            factor = counts_by_entity[entity_name]
            joint_resource_requirements += factor * requirements

        for label in joint_resource_requirements.labels:
            if not label in self.labels:
                return (False, 0.0)

        system_resource_usage = SystemResourceUsage(self, instances_count, joint_resource_requirements.to_dict())
        allocated = not system_resource_usage.is_full()

        return (allocated, system_resource_usage)

    def get_unique_id(self) -> str:

        return self.provider + self.node_type

    def get_name(self) -> str:

        return self.node_type

    def get_provider(self) -> str:

        return self.provider

    def get_max_usage(self) -> dict:

        return {'vCPU': self.vCPU, 'memory': self.memory,
                'disk': self.disk, 'network_bandwidth': self.network_bandwidth}

    def get_cost_per_unit_time(self) -> int:

        return self.price_p_h

    def get_performance(self) -> float:

        return 0
