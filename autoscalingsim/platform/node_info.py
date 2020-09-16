class NodeInfo:
    """
    Holds the static information about the node used to deploy the application, e.g. virtual machine.
    """
    def __init__(self,
                 node_type,
                 vCPU,
                 memory,
                 network_bandwidth_MBps,
                 price_p_h = 0.0,
                 cpu_credits_h = 0,
                 latency_ms = 0):
        
        self.node_type = node_type
        self.vCPU = vCPU
        self.memory = memory
        self.network_bandwidth_MBps = network_bandwidth_MBps
        self.price_p_h = price_p_h
        self.cpu_credits_h = cpu_credits_h
        self.latency_ms = latency_ms
