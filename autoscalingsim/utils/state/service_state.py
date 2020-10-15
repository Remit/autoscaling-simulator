import pandas as pd

from .state import State

class ServiceState(State):
    """
    Contains information relevant to conduct the scaling. The state should be
    updated at each simulation step and provided to the ServiceScalingPolicyHierarchy
    s.t. the scaling decision could be taken. The information stored in the
    ServiceState is diverse and satisfies any type of scaling policy that
    could be used, be it utilization-based or workload-based policy, reactive
    or predictive, etc.

    TODO:
        add properties for workload-based scaling + predictive
    """

    resource_utilization_types = [
        'cpu_utilization',
        'mem_utilization',
        'disk_utilization'
    ]

    def __init__(self,
                 init_timestamp,
                 init_service_instances,
                 init_resource_requirements,
                 averaging_interval_ms,
                 init_keepalive_ms = -1):

        # Untimed
        self.requirements = init_resource_requirements
        self.count = init_service_instances
        # The number of threads that the platform has allocated on different nodes
        # for the instances of this service
        self.platform_threads_available = 4 # TODO: 0, it's a temp fix
        # the negative value of keepalive is used to keep the timed params indefinitely
        self.keepalive = pd.Timedelta(init_keepalive_ms, unit = 'ms')

        # Timed
        self.tmp_state = State.TempState(init_timestamp,
                                         averaging_interval_ms,
                                         ServiceState.resource_utilization_types)

        default_ts_init = {'datetime': [init_timestamp], 'value': [0.0]}

        self.cpu_utilization = pd.DataFrame(default_ts_init)
        self.cpu_utilization = self.cpu_utilization.set_index('datetime')

        self.mem_utilization = pd.DataFrame(default_ts_init)
        self.mem_utilization = self.mem_utilization.set_index('datetime')

        self.disk_utilization = pd.DataFrame(default_ts_init)
        self.disk_utilization = self.disk_utilization.set_index('datetime')

    def get_val(self,
                attribute_name):

        """
        Currently, the only method defined in the parent abstract class (interface),
        i.e. the contract that State establishes with others using it is that
        of a uniform attribute getting access.
        """

        if not hasattr(self, attribute_name):
            raise ValueError('Attribute {} not found in {}'.format(attribute_name, self.__class__.__name__))

        return self.__getattribute__(attribute_name)

    def update_val(self,
                   attribute_name,
                   attribute_val):

        """
        Updates an untimed attribute (its past values are not interesting).
        Should be called by an entity that computes the new value of the attribute, i.e.
        it incorporates the formalized knowledge of how to compute it.
        For instance, ScalingAspectManager is responsible for the updates
        that happen during the scaling with the aspects, e.g. number of service
        instances grows or shrinks; a scaling aspect is a variety of an updatable attribute.
        """

        if (not hasattr(self, attribute_name)) or attribute_name in ServiceState.resource_utilization_types:
            raise ValueError('Untimed attribute {} not found in {}'.format(aspect_name, self.__class__.__name__))

        self.__setattr__(aspect_name, aspect_val)

    def update_metric(self,
                      metric_name,
                      cur_ts,
                      cur_val):

        """
        Updates a metric with help of the temporary state that bufferizes some observations
        that are aggregated based on a moving average technique and returned as the actual
        values stored in the ServiceState.
        """

        if not hasattr(self, metric_name):
            raise ValueError('Metric {} not found in {}'.format(metric_name, self.__class__.__name__))

        old_metric_val = self.__getattribute__(metric_name)

        if isinstance(old_metric_val, pd.DataFrame):
            if not isinstance(cur_ts, pd.Timestamp):
                raise ValueError('Timestamp of unexpected type')

            oldest_to_keep_ts = cur_ts - self.keepalive

            # Discarding old observations
            if oldest_to_keep_ts < cur_ts:
                old_metric_val = old_metric_val[old_metric_val.index > oldest_to_keep_ts]

            val_to_upd = self.tmp_state.update_and_get(metric_name,
                                                       cur_ts,
                                                       cur_val)

            val_to_upd = old_metric_val.append(val_to_upd)
            self.__setattr__(metric_name, val_to_upd)
        else:
            raise ValueError('Unexpected metric type {}'.format(type(old_metric_val)))
