class SimulationResultsForPlotting:

    def __init__(self, infrastructure_cost : dict, response_times : dict, buffer_times : dict, network_times : dict, load : dict,
                 utilization : dict, desired_node_count : dict, actual_node_count : dict):

        self.infrastructure_cost = infrastructure_cost.copy()
        self.response_times = response_times.copy()
        self.buffer_times = buffer_times.copy()
        self.network_times = network_times.copy()
        self.load = load.copy()
        self.utilization = utilization.copy()
        self.desired_node_count = desired_node_count.copy()
        self.actual_node_count = actual_node_count.copy()
