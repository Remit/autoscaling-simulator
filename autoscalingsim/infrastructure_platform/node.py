import pandas as pd

from .system_capacity import SystemCapacity

from ..scaling.policiesbuilder.scaled.scaled_container import ScaledContainer
from ..utils.requirements import ResourceRequirements
from ..utils.state.entity_state.entity_group import EntitiesState

class NodeInfo(ScaledContainer):
    """
    Holds the static information about the node used to deploy the application, e.g. virtual machine.
    NodeInfo is derived from the ScaledEntityContainer class to provide an
    expected interface to the adjusters of the platform to the scaled services.

    TODO:
        consider more universal resource names and what performance can be shared?
    """
    def __init__(self,
                 provider : str,
                 node_type : str,
                 vCPU : int,
                 memory : int,
                 disk : int,
                 network_bandwidth_MBps : float,
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
        self.network_bandwidth_MBps = network_bandwidth_MBps
        self.price_p_h = price_p_h
        self.cpu_credits_h = cpu_credits_h
        self.latency = latency
        self.requests_acceleration_factor = requests_acceleration_factor
        self.labels = labels

    def get_unique_id(self):
        return self.provider + self.node_type

    def get_name(self):
        return self.node_type

    def get_capacity(self):

        capacity_dict = {'vCPU': self.vCPU,
                         'memory': self.memory,
                         'disk': self.disk,
                         'network_bandwidth_MBps': self.network_bandwidth_MBps}

        return capacity_dict

    def get_cost_per_unit_time(self):

        return self.price_p_h

    def get_performance(self):

        return 0

    def resource_requirements_to_capacity(self,
                                          res_requirements : ResourceRequirements):

        """
        Computes system capacity that will be taken on this type of node
        if the resource requirements provided in the parameter are to be accomodated.
        For instance, the resource requirements can be provided for a specific request.
        """

        return SystemCapacity(self, 1, res_requirements.to_dict())

    def entities_require_capacity(self,
                                  entities_state : EntitiesState) -> tuple:

        """
        Calculates how much capacity would be taken by the entities if they
        are to be accommodated on the node. If no state is provided, each
        entity is assumed to have a single instance. Otherwise, the instance
        count is taken from the state. In addition, the method returns whether
        the entities can at all be accommodated on the node.
        """

        if not isinstance(entities_state, EntitiesState):
            raise TypeError(f'Unexpected type provided to compute the required capacity: {type(entities_state)}')

        requirements_by_entity = entities_state.get_entities_requirements()
        counts_by_entity = entities_state.get_entities_counts()

        joint_resource_requirements = ResourceRequirements.new_empty_resource_requirements()
        for entity_name, requirements in requirements_by_entity.items():
            factor = 1
            if not entities_state is None:
                factor = counts_by_entity[entity_name]

            joint_resource_requirements += factor * requirements

        for label in joint_resource_requirements.labels:
            if not label in self.labels:
                return (False, 0.0)

        capacity_taken = SystemCapacity(self, 1,
                                        joint_resource_requirements.to_dict())

        allocated = not capacity_taken.is_exhausted()

        return (allocated, capacity_taken)
