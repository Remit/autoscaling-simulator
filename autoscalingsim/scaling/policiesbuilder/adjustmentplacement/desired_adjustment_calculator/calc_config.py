from autoscalingsim.scaling.state_reader import StateReader

class DesiredChangeCalculatorConfig:

    def __init__(self, placement_hint : str, optimizer_type : str,
                 node_for_scaled_services_types : dict, state_reader : StateReader):

        self.placement_hint = placement_hint
        self.optimizer_type = optimizer_type
        self.node_for_scaled_services_types = node_for_scaled_services_types
        self.state_reader = state_reader
