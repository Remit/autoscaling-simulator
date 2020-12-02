from autoscalingsim.deltarepr.service_instances_group_delta import ServiceInstancesGroupDelta, ServiceInstancesGroupDeltaWildcard

class GroupOfServicesDelta:

    @classmethod
    def from_deltas(cls : type, deltas : dict, in_change : bool = True, virtual : bool = False):

        services_group_delta = cls({}, in_change, virtual)
        services_group_delta.deltas = deltas

        return services_group_delta

    def __init__(self, aspects_vals_per_entity : dict = {},
                 in_change : bool = True, virtual : bool = False, services_reqs : dict = {}):

        self.deltas = { service_name : ServiceInstancesGroupDelta(service_name, aspects_vals, services_reqs[service_name]) \
                                        if service_name in services_reqs else ServiceInstancesGroupDeltaWildcard(service_name, aspects_vals) \
                                        for service_name, aspects_vals in aspects_vals_per_entity.copy().items() }

        # Signifies whether the delta should be considered during the enforcing or not.
        # The aim of 'virtual' property is to keep the connection between the deltas after the enforcement.
        self.virtual = virtual
        self.in_change = in_change

    def enforce(self, services_lst : list):

        """
        Enforces the change represented by this delta for entities provided in the list.
        Results two deltas. The first is enforced, and the second contains the unenforced
        remainder to consider later on (e.g. later enforcement time).
        """

        enforced_deltas = { service_name : delta for service_name, delta in self.deltas.items() if service_name in services_lst }
        not_enforced_deltas = { service_name : delta for service_name, delta in self.deltas.items() if not service_name in services_lst }

        return (self.__class__.from_deltas(enforced_deltas, in_change = False),
                self.__class__.from_deltas(not_enforced_deltas, in_change = True))

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other_delta : 'GroupOfServicesDelta', sign : int):

        if not isinstance(other_delta, GroupOfServicesDelta):
            raise TypeError(f'The operand to be added is not of the expected type {self.__class__.__name__}, got {other_delta.__class__.__name__}')

        if self.in_change != other_delta.in_change:
            raise ValueError('Operands differ by the in_change status')

        new_deltas = self.deltas.copy()
        for service_name in other_delta.deltas:
            if service_name in new_deltas:
                if sign == -1:
                    new_deltas[service_name] -= other_delta.deltas[service_name]
                elif sign == 1:
                    new_deltas[service_name] += other_delta.deltas[service_name]
            else:
                new_deltas[service_name] = other_delta.deltas[service_name]

        return self.__class__.from_deltas({ service_name : delta for service_name, delta in new_deltas.items() if delta.to_raw_change()['count'] != 0 },
                                          self.in_change, self.virtual)

    def add(self, other_delta : ServiceInstancesGroupDelta):

        if not isinstance(other_delta, ServiceInstancesGroupDelta):
            raise TypeError(f'An attempt to add an object of unknown type {other_delta.__class__.__name__} to the list of deltas in {self.__class__.__name__}')

        self.deltas[other_delta.service_name] = other_delta

    def set_count_sign(self, sign : int):

        for delta in self.deltas.values():
            delta.set_count_sign(sign)

    @property
    def services(self):

        return list(self.deltas.keys())

    # was get_service_group_delta
    def get_delta_for_service(self, service_name : str):

        if not service_name in self.deltas:
            raise ValueError(f'No entity group delta for entity name {service_name} found')

        return self.deltas[service_name]

    # was extract_raw_scaling_aspects_changes
    def get_raw_scaling_aspects_changes(self):

        return { service_name : service_delta.to_raw_change() for service_name, service_delta in self.deltas.items() }

    def to_services_raw_count_change(self):

        return { service_name : delta.to_raw_change()['count'] for service_name, delta in self.deltas.items() }

    def to_services_state(self):

        return GroupOfServices({ service_name : delta.to_service_group() for service_name, delta in self.deltas.items() })

    def copy(self):

        return self.__class__.from_deltas(self.deltas.copy(), self.in_change, self.virtual)

    def __repr__(self):

        return f'{self.__class__.__name__}( deltas = {repr(self.deltas)}, in_change = {self.in_change}, virtual = {self.virtual})'
