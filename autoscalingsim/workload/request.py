import uuid

class Request:
    def __init__(self,
                 request_type,
                 request_id = None):
        # Static state
        self.request_type = request_type
        if request_id is None:
            self.request_id = uuid.uuid1()
        else:
            self.request_id = request_id

        # Dynamic state
        self.processing_left_ms = 0
        self.waiting_on_link_left_ms = 0
        self.cumulative_time_ms = 0
        self.upstream = True
        self.replies_expected = 1 # to implement the fan-in on the level of service

    def set_downstream(self):
        self.upstream = False

class RequestProcessingInfo:
    def __init__(self,
                 request_type,
                 entry_service,
                 processing_times,
                 timeout_ms,
                 request_size_b,
                 response_size_b,
                 request_operation_type):

        self.request_type = request_type
        self.entry_service = entry_service
        self.processing_times = processing_times
        self.timeout_ms = timeout_ms
        self.request_size_b = request_size_b
        self.response_size_b = response_size_b
        self.request_operation_type = request_operation_type
