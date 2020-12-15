import pandas as pd

from .system_resource_usage import SystemResourceUsage

from autoscalingsim.utils.size import Size
from autoscalingsim.utils.price import PricePerUnitTime
from autoscalingsim.utils.credits import CreditsPerUnitTime
from autoscalingsim.utils.requirements import ResourceRequirements
from autoscalingsim.desired_state.service_group.group_of_services import GroupOfServices

class NodeInfo:

    """
    Holds static information about the node type, e.g. virtual machine.
    NodeInfo realizes interface of the ScaledContainer class to
    fulfill the expectations of the adjusting algorithms when scaling.
    """

    def __init__(self,
                 provider : str,
                 node_type : str,
                 vCPU : int,
                 memory : Size,
                 disk : Size,
                 network_bandwidth : Size,
                 price_per_unit_time : PricePerUnitTime = PricePerUnitTime(0),
                 cpu_credits_per_unit_time : CreditsPerUnitTime = CreditsPerUnitTime('vCPU', 0),
                 latency : pd.Timedelta = pd.Timedelta(0, unit = 'ms'),
                 requests_acceleration_factor : float = 1.0,
                 labels : list = []):

        self._provider = provider
        self._node_type = node_type
        self._vCPU = vCPU
        self._memory = memory
        self._disk = disk
        self._network_bandwidth = network_bandwidth
        self._price_per_unit_time = price_per_unit_time
        self._cpu_credits_per_unit_time = cpu_credits_per_unit_time
        self._latency = latency
        self._requests_acceleration_factor = requests_acceleration_factor
        self._labels = labels

    def system_resources_to_take_from_requirements(self, res_requirements : ResourceRequirements) -> SystemResourceUsage:

        """
        Computes system resource usage that will be taken on this type of node
        if the resource requirements provided in the parameter are to be fulfilled.
        For instance, the resource requirements can be provided for a request.
        """

        return SystemResourceUsage(self, 1, res_requirements.to_dict())

    def services_require_system_resources(self, services_state : GroupOfServices,
                                          instances_count : int = 1) -> tuple:

        """
        Calculates system resource usage by the services if they
        are to be accommodated on the node. If no state is provided, each
        service is assumed to have a single instance. Otherwise, the instance
        count is taken from the state. In addition, the method returns whether
        the services can at all be accommodated by this node type.
        """

        if not isinstance(services_state, GroupOfServices):
            raise TypeError(f'Unexpected type provided to compute the required capacity: {type(services_state)}')

        requirements_by_service = services_state.services_requirements
        counts_by_service = services_state.services_counts

        joint_resource_requirements = ResourceRequirements.new_empty_resource_requirements()
        for service_name, requirements in requirements_by_service.items():
            factor = counts_by_service[service_name]
            joint_resource_requirements += factor * requirements

        for label in joint_resource_requirements.labels:
            if not label in self._labels:
                return (False, 0.0)

        system_resource_usage = SystemResourceUsage(self, instances_count, joint_resource_requirements.to_dict())
        allocated = not system_resource_usage.is_full

        return (allocated, system_resource_usage)

    @property
    def unique_id(self) -> str:

        return self._provider + self._node_type

    @property
    def node_type(self) -> str:

        return self._node_type

    @property
    def latency(self) -> pd.Timedelta:

        return self._latency

    @property
    def network_bandwidth(self) -> Size:

        return self._network_bandwidth

    @property
    def provider(self) -> str:

        return self._provider

    @property
    def price_per_unit_time(self) -> PricePerUnitTime:

        return self._price_per_unit_time

    @property
    def max_usage(self) -> dict:

        return {'vCPU': self._vCPU, 'memory': self._memory,
                'disk': self._disk, 'network_bandwidth': self._network_bandwidth}

    def __repr__(self):

        return f'{self.__class__.__name__}(provider = {self._provider}, \
                                           node_type = {self._node_type}, \
                                           vCPU = {self._vCPU}, \
                                           memory = {repr(self._memory)}, \
                                           disk = {repr(self._disk)}, \
                                           network_bandwidth = {repr(self._network_bandwidth)}, \
                                           price_per_unit_time = {self._price_per_unit_time}, \
                                           cpu_credits_per_unit_time = {self._cpu_credits_per_unit_time}, \
                                           latency = {self._latency}, \
                                           requests_acceleration_factor = {self._requests_acceleration_factor}, \
                                           labels = {self._labels})'
