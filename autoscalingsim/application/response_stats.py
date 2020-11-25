from autoscalingsim.load.request import Request

class ResponseStatsRegional:

    """
    Load statistics collected for a particular region.
    """

    def __init__(self):

        self.response_times_by_request = {}
        self.network_times_by_request = {}
        self.buffer_times_by_request = {}

    def add_request(self, req : Request):

        # Response received by the user
        if not req.request_type in self.response_times_by_request:
            self.response_times_by_request[req.request_type] = []
        self.response_times_by_request[req.request_type].append(req.cumulative_time.microseconds / 1000)

        # Time spent transferring between the nodes
        if not req.request_type in self.network_times_by_request:
            self.network_times_by_request[req.request_type] = []
        self.network_times_by_request[req.request_type].append(req.network_time.microseconds / 1000)

        # Time spent waiting in the buffers
        if len(req.buffer_time) > 0:
            if not req.request_type in self.buffer_times_by_request:
                self.buffer_times_by_request[req.request_type] = {}

            for service_name, buffer_time_val in req.buffer_time.items():
                if not service_name in self.buffer_times_by_request[req.request_type]:
                    self.buffer_times_by_request[req.request_type][service_name] = []
                self.buffer_times_by_request[req.request_type][service_name].append(buffer_time_val.microseconds / 1000)

class ResponseStats:

    def __init__(self):
        self.regional_stats = {}

    def add_request(self, req : Request):

        if not req.region_name in self.regional_stats:
            self.regional_stats[req.region_name] = ResponseStatsRegional()

        self.regional_stats[req.region_name].add_request(req)

    def get_response_times_by_request(self):

        return { region_name : reg_stats.response_times_by_request for region_name, reg_stats in self.regional_stats.items() }

    def get_network_times_by_request(self):

        return { region_name : reg_stats.network_times_by_request for region_name, reg_stats in self.regional_stats.items() }

    def get_buffer_times_by_request(self):

        return { region_name : reg_stats.buffer_times_by_request for region_name, reg_stats in self.regional_stats.items() }
