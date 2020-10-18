from collections import deque

from ..workload.request import Request, RequestProcessingInfo

class LinkBuffer:
    """
    Combines link and the buffer. The requests in the link advance when the step method is called.
    If the processing time left (i.e. waiting on the link) is over, the request proceeds to the
    buffer, where it can be extracted from for the further processing in a service.
    If the used throughput reached the throughput limit, the request is lost. The same happens
    if upon the transition to the buffer, the buffer is full for the given request type.

    Properties:

        As buffer:
            capacity_by_request_type (dict):  holds capacity of the buffer by request type, in terms of
                                              requests currently placed in the buffer

            [STUB] policy:                    policy used for moving requests in the buffer, e.g. FIFO/LIFO

            ********************************************************************************************************

            requests (collections.deque):     holds current requests in the buffer

            reqs_cnt (dict):                  holds current request count by the request type, used to rapidly check
                                              if more requests of the given type can be accomodated in the buffer

        As link:
            latency_ms (int):                 latency of the link in milliseconds, taken from the config

            throughput_mbps (int):            throughput of the link in Megabytes per sec, taken from the config

            request_processing_infos (dict):  holds requests processing information to compute the used
                                              throughput etc.

            ********************************************************************************************************

            requests_in_transfer (list):      holds requests that are currently "transferred" by this link

            used_throughput_MBps (int):       throughput currently used on this link by the "transferred" reqs

    Methods:

        As buffer:
            append_left (req):                puts the request req at the beginning of the buffer to give other
                                              requests opportunity to be processed if the current request waits
                                              for other replies to get processed (fan-in)

            pop:                              takes the last added request out of the buffer for processing (LIFO)

            pop_left:                         takes the first added request out of the buffer for processing (FIFO)

            size:                             returns size of the buffer

            add_cumulative_time (delta):      adds time delta to every request in the buffer

            remove_by_id (request_id):        removes all the requests with request id request_id from the buffer

        As link:
            put (req):                        puts a new request req on a link, i.e. in requests_in_transfer,
                                              if there is enough spare throughput; otherwise drops the request

            step (simulation_step_ms):        makes a discrete simulation time step of the length simulation_step_ms
                                              to advance the requests held on the link, i.e. in requests_in_transfer,
                                              and to put them into the buffer if possible (capacity left). In case of
                                              no spare capacity in the buffer, the request is also dropped.

            _req_occupied_MBps (req):         private method that computes the throughput used by the request req

    TODO:
        implementing scaling of the links? e.g. according to the added instances of services.
        implement wrapping of the lower-level details into policies like LIFO, FIFO, etc. hide append, pop etc.

    """
    def __init__(self,
                 capacity_by_request_type,
                 request_processing_infos,
                 policy = "FIFO"):

        # Static state
        # Buffer:
        self.capacity_by_request_type = capacity_by_request_type
        self.policy = policy

        # Link:
        self.latency_ms = pd.Timedelta(0, unit = 'ms')
        self.throughput_MBps = 0
        self.request_processing_infos = request_processing_infos

        # Dynamic state
        # Buffer:
        self.requests = deque([])
        self.reqs_cnt = {}
        for request_type in capacity_by_request_type.keys():
            self.reqs_cnt[request_type] = 0

        # Link:
        self.requests_in_transfer = []
        self.used_throughput_MBps = 0

    def update_settings(latency : pd.Timedelta,
                        network_bandwidth_MBps : int):

        self.latency = latency
        self.throughput_MBps = network_bandwidth_MBps

    def step(self, simulation_step_ms):
        """ Processing requests to bring them from the link into the buffer """
        for req in self.requests_in_transfer:
            #min_time_to_subtract_ms = min(req.processing_left_ms, simulation_step_ms)
            #req.processing_left_ms -= min_time_to_subtract_ms
            #if req.processing_left_ms <= 0:
            req.cumulative_time_ms += simulation_step_ms
            capacity = self.capacity_by_request_type[req.request_type]

            req.waiting_on_link_left_ms -= simulation_step_ms
            req.network_time_ms += simulation_step_ms
            if req.waiting_on_link_left_ms <= 0:
                if (capacity > self.reqs_cnt[req.request_type]) and (req.cumulative_time_ms < self.request_processing_infos[req.request_type].timeout_ms):
                    self.requests.append(req)
                    self.used_throughput_MBps -= self._req_occupied_MBps(req)
                    self.reqs_cnt[req.request_type] += 1

                self.requests_in_transfer.remove(req)

    def attempt_pop(self):

        if self.size() > 0:
            return self.requests[-1]
        else:
            return None

    def attempt_fan_in(self):

        if self.size() > 0:
            req = self.requests[-1]
            # Processing fan-in case
            if req.replies_expected > 1:
                req_id_ref = req.request_id
                reqs_present = 1
                for req_lookup in self.requests[:-1]:
                    if req_lookup.request_id == req_id_ref:
                        reqs_present += 1

                if reqs_present == req.replies_expected:
                    return req

            else:
                return req
        else:
            return None

    def shift(self):

        """
        Shifts requests queue to give other requests a chance to get processed.
        """

        req = self.pop()
        self.append_left(req)

    def fan_in(self,
               req : Request):

        """
        Finalizes fan-in action for the given request by removing the
        associated responses from the requests queue.
        """

        req = self.pop()
        self.remove_by_id(req.request_id)
        req.replies_expected = 1

    def put(self, req):
        req_size_b_MBps = self._req_occupied_MBps(req)

        if self.throughput_MBps - self.used_throughput_MBps >= req_size_b_MBps:
            self.used_throughput_MBps += req_size_b_MBps
            req.waiting_on_link_left_ms = self.latency_ms
            self.requests_in_transfer.append(req)
        else:
            del req

    def append_left(self, req):
        self.requests.appendLeft(req)

    def pop(self):
        req = None
        if len(self.requests) > 0:
            req = self.requests.pop()
            self.reqs_cnt[req.request_type] -= 1

        return req

    def pop_left(self):
        req = None
        if len(self.requests) > 0:
            req = self.requests.popLeft()
            self.reqs_cnt[req.request_type] -= 1

        return req

    def size(self):
        return len(self.requests)

    def add_cumulative_time(self,
                            delta,
                            service_name):

        for req in self.requests:
            req.cumulative_time_ms += delta
            if not service_name in req.buffer_time_ms:
                req.buffer_time_ms[service_name] = delta
            else:
                req.buffer_time_ms[service_name] += delta

    def remove_by_id(self, request_id):
        for req in reversed(self.requests):
            if req.request_id == request_id:
                self.requests.remove(req)

    def _req_occupied_MBps(self, req):
        req_size_b = 0
        if req.upstream:
            req_size_b = self.request_processing_infos[req.request_type].request_size_b
        else:
            req_size_b = self.request_processing_infos[req.request_type].response_size_b
        req_size_b_mb = req_size_b / (1024 * 1024)
        req_size_b_MBps = req_size_b_mb * (self.latency_ms / 1000) # taking channel for that long
        return req_size_b_MBps
