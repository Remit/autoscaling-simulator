class ScalingModel:
    """
    Defines the scaling behaviour that does not depend upon the scaling policy, i.e. represents
    unmanaged scaling characteristics such as booting times for VMs or start-up times for service
    instances. Encompasses two parts, one related to the Platform Model, the other related to the
    Services in the Application Model.
    """
    def __init__(self,
                 simulation_step_ms,
                 config_filename = None):

        # Static state
        self.platform_scaling_model = PlatformScalingModel(simulation_step_ms)
        self.application_scaling_model = None

        if config_filename is None:
            raise ValueError('Configuration file not provided for the ApplicationModel.')
        else:
            with open(config_filename) as f:
                config = json.load(f)

                # 1. Filling into the platform scaling information
                # Defaults
                provider = "on-premise"
                decision_making_time_ms = 0
                link_added_throughput_coef_per_vm = 1
                nodes_scaling_infos_raw = []

                for platform_i in config["platform"]:

                    if "provider" in platform_i.keys():
                        provider = platform_i["provider"]
                    if "decision_making_time_ms" in platform_i.keys():
                        decision_making_time_ms = platform_i["decision_making_time_ms"]
                    if "link_added_throughput_coef_per_vm" in platform_i.keys():
                        link_added_throughput_coef_per_vm = platform_i["link_added_throughput_coef_per_vm"]
                    if "nodes" in platform_i.keys():
                        nodes_scaling_infos_raw = platform_i["nodes"]

                    self.platform_scaling_model.add_provider(provider,
                                                             decision_making_time_ms,
                                                             link_added_throughput_coef_per_vm,
                                                             nodes_scaling_infos_raw)

                # 2. Filling into the application scaling information
                # Defaults
                decision_making_time_ms = 0
                service_scaling_infos_raw = []

                if "decision_making_time_ms" in config["application"].keys():
                    decision_making_time_ms = config["application"]["decision_making_time_ms"]
                if "services" in config["application"].keys():
                    service_scaling_infos_raw = config["application"]["services"]

                self.application_scaling_model = ApplicationScalingModel(decision_making_time_ms,
                                                                         service_scaling_infos_raw)
