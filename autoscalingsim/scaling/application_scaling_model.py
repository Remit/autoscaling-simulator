from datetime import timedelta

class ServiceScalingInfo:
    """
    """
    def __init__(self,
                 boot_up_delta):

        self.boot_up_ms = boot_up_delta

class ApplicationScalingModel:
    """
    """
    def __init__(self,
                 decision_making_time_ms = 0,
                 service_scaling_infos_raw = []):

        self.decision_making_time_ms = decision_making_time_ms
        self.service_scaling_infos = {}

        for service_scaling_info_raw in service_scaling_infos_raw:
            ssi = ServiceScalingInfo(service_scaling_info_raw["boot_up_ms"] * timedelta(microseconds = 1000))

            self.service_scaling_infos[service_scaling_info_raw["name"]] = ssi

    def get_service_scaling_params(self,
                                   service_name):
        ssi = None
        if service_name in self.service_scaling_infos:
            ssi = self.service_scaling_infos[service_name]

        return ssi
