import pandas as pd
from .size import Size

class MetricUnitsRegistry:

    """ """

    _Registry = {
        'Timedelta': pd.Timedelta,
        'Size': Size
    }

    @classmethod
    def get(cls, name : str):

        return cls._Registry[name] if name in cls._Registry else float
