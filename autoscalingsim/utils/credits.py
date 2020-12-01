import pandas as pd

class CreditsPerUnitTime:

    def __init__(self, resource_name : str, value : float,
                 time_unit : pd.Timedelta = pd.Timedelta(1, unit = 'h')):

        self.resource_name = resource_name
        self.value = value
        self.time_unit = time_unit

    def __mul__(self, other):

        if isinstance(other, pd.Timedelta):
            time_ratio = other / self.time_unit
            return self.value * time_ratio

        elif isinstance(other, numebers.Number):
            return self.__class__(self.value, self.time_unit)

        else:
            raise TypeError(f'Unexpected type {other.__class__.__name__}')

    def __rmul__(self, other):

        return self.__mul__(other)
