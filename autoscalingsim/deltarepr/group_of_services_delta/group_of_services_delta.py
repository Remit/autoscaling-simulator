from autoscalingsim.deltarepr.service_instances_group_delta import ServiceInstancesGroupDelta, ServiceInstancesGroupDeltaWildcard

class GroupOfServicesDelta:

    @classmethod
    def from_deltas(cls : type, deltas : dict, in_change : bool = True):

        services_group_delta = cls({}, in_change)
        services_group_delta.deltas = deltas

        return services_group_delta

    def __init__(self, aspects_vals_per_entity : dict = {},
                 in_change : bool = True, services_reqs : dict = {}):

        self.deltas = {}
        for service_name, aspects_vals in aspects_vals_per_entity.items():
            if service_name in services_reqs:
                self.deltas[service_name] = ServiceInstancesGroupDelta(service_name, aspects_vals, services_reqs[service_name])
            else:
                self.deltas[service_name] = ServiceInstancesGroupDeltaWildcard(service_name, aspects_vals)

        self.in_change = in_change

    def enforce(self, services_lst : list):

        """
        Enforces the change represented by this delta for the services provided in the list.
        Results in two deltas. The first is enforced, and the second contains the unenforced
        remainder to consider later on (e.g. later enforcement time).
        """

        enforced_deltas = dict(filter(lambda rec: rec[0] in services_lst, self.deltas.items()))
        not_enforced_deltas = dict(filter(lambda rec: rec[0] not in services_lst, self.deltas.items()))

        return (self.__class__.from_deltas(enforced_deltas, in_change = False),
                self.__class__.from_deltas(not_enforced_deltas, in_change = True))

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other : 'GroupOfServicesDelta', sign : int):

        if not isinstance(other, GroupOfServicesDelta):
            raise TypeError(f'The operand to be added is not of the expected type {self.__class__.__name__}, got {other.__class__.__name__}')

        if self.in_change != other.in_change:
            raise ValueError('Operands differ by their in_change statuses')

        new_deltas = self.deltas.copy()
        for service_name in other.deltas:
            if service_name in new_deltas:
                if sign == -1:
                    new_deltas[service_name] -= other.deltas[service_name]
                elif sign == 1:
                    new_deltas[service_name] += other.deltas[service_name]
            else:
                new_deltas[service_name] = other.deltas[service_name]

        return self.__class__.from_deltas(dict(filter(lambda rec: not rec[1].is_empty, new_deltas.items())), self.in_change)

    #def insert(self, other : ServiceInstancesGroupDelta):

    #    if not isinstance(other, ServiceInstancesGroupDelta):
    #        raise TypeError(f'An attempt to add an object of unknown type {other.__class__.__name__} to the list of deltas in {self.__class__.__name__}')

    #    self.deltas[other.service_name] = other

    def set_count_sign(self, sign : int):

        for delta in self.deltas.values():
            delta.set_count_sign(sign)

    def copy(self):

        return self.__class__.from_deltas(self.deltas.copy(), self.in_change)

    @property
    def services(self):

        return list(self.deltas.keys())

    def delta_for_service(self, service_name : str):

        if not service_name in self.deltas:
            raise ValueError(f'No service group delta for the name {service_name} found')

        return self.deltas[service_name].copy()

    def to_raw_scaling_aspects_changes(self):

        return { service_name : service_delta.to_raw_change() for service_name, service_delta in self.deltas.items() }

    def to_raw_count_change(self):

        return { service_name : delta.to_raw_change()['count'] for service_name, delta in self.deltas.items() }

    def to_services_state(self):

        return GroupOfServices({ service_name : delta.to_service_group() for service_name, delta in self.deltas.items() })

    def __repr__(self):

        return f'{self.__class__.__name__}( deltas = {repr(self.deltas)}, in_change = {self.in_change})'
