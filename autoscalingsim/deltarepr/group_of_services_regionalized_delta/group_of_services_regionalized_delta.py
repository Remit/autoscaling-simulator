class GroupOfServicesRegionalizedDelta:

    @classmethod
    def from_group(cls : type, services_groups_per_region : dict):

        return cls({ region_name : group_of_services.to_delta() \
                    for region_name, group_of_services in services_groups_per_region.items() })

    def __init__(self, deltas : dict):

        self.deltas = deltas

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other_delta : 'GroupOfServicesRegionalizedDelta', sign : int):

        if not isinstance(other_delta, self.__class__):
            raise TypeError(f'The operand to be added is not of the expected type {self.__class__.__name__}: instead got {other_delta.__class__.__name__}')

        new_delta = self.copy()
        for region_name in other_delta.deltas:
            if region_name in new_delta.deltas:
                if sign == -1:
                    new_delta.deltas[region_name] -= other_delta.deltas[region_name]
                elif sign == 1:
                    new_delta.deltas[region_name] += other_delta.deltas[region_name]
            else:
                new_delta.deltas[region_name] = other_delta.deltas[region_name]

        return new_delta

    def copy(self):

        return self.__class__(self.deltas)

    def to_raw_scaling_aspects_changes(self):

        return { region_name : region_delta.to_raw_scaling_aspects_changes() for region_name, region_delta in self.deltas.items() }
