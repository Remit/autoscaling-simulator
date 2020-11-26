import pandas as pd
from .size import Size

class MetricUnitsRegistry:

    _Registry = {
        'duration': pd.Timedelta,
        'size': Size
    }

    @classmethod
    def get(cls, name : str):

        return cls._Registry[name] if name in cls._Registry else float
