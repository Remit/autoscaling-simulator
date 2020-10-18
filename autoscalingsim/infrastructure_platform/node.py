from .system_capacity import SystemCapacity

from ..scaling.policiesbuilder.scaled.scaled_container import ScaledContainer

class NodeInfo(ScaledContainer):
    """
    Holds the static information about the node used to deploy the application, e.g. virtual machine.
    NodeInfo is derived from the ScaledEntityContainer class to provide an
    expected interface to the adjusters of the platform to the scaled services.

    TODO:
        consider more universal resource names and what performance can be shared?
    """
    def __init__(self,
                 provider,
                 node_type,
                 vCPU,
                 memory,
                 network_bandwidth_MBps,
                 price_p_h = 0.0,
                 cpu_credits_h = 0,
                 latency_ms = 0,
                 requests_acceleration_factor = 1.0,
                 labels = []):

        self.provider = provider
        self.node_type = node_type
        self.vCPU = vCPU
        self.memory = memory
        self.network_bandwidth_MBps = network_bandwidth_MBps
        self.price_p_h = price_p_h
        self.cpu_credits_h = cpu_credits_h
        self.latency_ms = latency_ms
        self.requests_acceleration_factor = requests_acceleration_factor
        self.labels = labels

    def get_name(self):
        return self.node_type

    def get_capacity(self):

        capacity_dict = {'vCPU': self.vCPU,
                         'memory': self.memory}

        return capacity_dict

    def get_cost_per_unit_time(self):

        return self.price_p_h

    def get_performance(self):

        return 0

    def fits(self,
             requirements_by_entity):

        """
        Checks whether the node of given type can acommodate the requirements
        of the entities considered for the placement on such node.
        """

        fits, _ = self.takes_capacity(requirements_by_entity)
        return fits

    def takes_capacity(self,
                       requirements_by_entity : dict, # TODO: adapt to resource Requirements abstraction
                       entities_state : EntitiesState = None):

        labels_required = []
        vCPU_required = 0
        memory_required = 0

        for entity_name, requirements in requirements_by_entity.items():
            labels_reqs = ErrorChecker.key_check_and_load('labels', requirements, self.node_type)
            vCPU_reqs = ErrorChecker.key_check_and_load('vCPU', requirements, self.node_type)
            memory_reqs = ErrorChecker.key_check_and_load('memory', requirements, self.node_type)

            factor = 1
            if not entities_state is None:
                factor = entities_state.count(entity_name)

            labels_required.extend(labels_reqs)
            vCPU_required += (vCPU_reqs * factor)
            memory_required += (memory_reqs * factor)

        for label_required in labels_required:
            if not label_required in self.labels:
                return (False, 0.0)

        capacity_taken = SystemCapacity(self.node_type,
                                        vCPU_required / self.vCPU,
                                        memory_required / self.memory)
        allocated = not capacity_taken.isexhausted()

        return (allocated, capacity_taken)
