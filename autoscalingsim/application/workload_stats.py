class WorkloadStatsRegional:

    """
    Workload statistics collected for a particular region.
    """

    def __init__(self):

        self.response_times_by_request = {}
        self.network_times_by_request = {}
        self.buffer_times_by_request = {}

    def add_request(self,
                    req : Request):

        # Response received by the user
        if not req.request_type in self.response_times_by_request:
            self.response_times_by_request[req.request_type] = []
        self.response_times_by_request[req.request_type].append(req.cumulative_time)

        # Time spent transferring between the nodes
        if not req.request_type in self.network_times_by_request:
            self.network_times_by_request[req.request_type] = []
        self.network_times_by_request[req.request_type].append(req.network_time)

        # Time spent waiting in the buffers
        if len(req.buffer_time) > 0:
            if not req.request_type in self.buffer_times_by_request:
                self.buffer_times_by_request[req.request_type] = []
            self.buffer_times_by_request[req.request_type].append(req.buffer_time)

class WorkloadStats:

    """
    Container for the stats of the processed requests.
    """

    def __init__(self):
        self.regional_stats = {}

    def add_request(self,
                    req : Request):

        if not req.region_name in self.regional_stats:
            self.regional_stats[req.region_name] = WorkloadStatsRegional()

        self.regional_stats[req.region_name].add_request(req)

    def get_response_times_by_request(self):

        response_times = {}
        for region_name, reg_stats in self.regional_stats.items():
            response_times[region_name] = reg_stats.response_times_by_request

        return response_times

    def get_network_times_by_request(self):

        network_times = {}
        for region_name, reg_stats in self.regional_stats.items():
            network_times[region_name] = reg_stats.network_times_by_request

        return network_times

    def get_buffer_times_by_request(self):

        buffer_times = {}
        for region_name, reg_stats in self.regional_stats.items():
            buffer_times[region_name] = reg_stats.buffer_times_by_request

        return buffer_times
