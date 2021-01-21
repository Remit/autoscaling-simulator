import pandas as pd

from autoscalingsim.utils.metric.metric_category import MetricCategory
from autoscalingsim.utils.error_check import ErrorChecker

class Size(MetricCategory):

    sizes_bytes = {
        'B'  : 1,
        'KB' : 1024,
        'MB' : 1048576,
        'GB' : 1073741824,
        'TB' : 1099511627776,
        'PB' : 1125899906842624,
        'EB' : 1152921504606846976,
        'ZB' : 1180591620717411303424 }

    default_unit = 'B'

    @classmethod
    def to_metric(cls, config : dict):

        val = ErrorChecker.key_check_and_load('value', config)
        unit = ErrorChecker.key_check_and_load('unit', config)

        return cls(val, unit = unit)

    @classmethod
    def to_target_value(cls, config : dict):

        val = ErrorChecker.key_check_and_load('value', config)
        if val < 0 or val > 1:
            raise ValueError('Target value should be specified as a relative number between 0.0 and 1.0')

        return val

    @classmethod
    def to_scaling_representation(cls, val : float):

        return val

    @classmethod
    def convert_df(cls, df : pd.DataFrame):

        return df

    def __init__(self, value : float = 0.0, unit : str = 'B'):

        if not unit in self.__class__.sizes_bytes and not unit.upper() in self.__class__.sizes_bytes:
            raise ValueError(f'Unknown unit {unit}')

        #if value < 0: value = 0

        normalizer = 1 if unit in self.__class__.sizes_bytes else 8 # handles the case of *bit unit
        self._value = (value / normalizer) * self.__class__.sizes_bytes[unit.upper()]

    def to_bytes(self): return self.to_unit('B')

    def to_kilobytes(self): return self.to_unit('kB')

    def to_megabytes(self): return self.to_unit('MB')

    def to_gigabytes(self): return self.to_unit('GB')

    def to_terabytes(self): return self.to_unit('TB')

    def to_petabytes(self): return self.to_unit('PB')

    def to_exabytes(self): return self.to_unit('EB')

    def to_zetabytes(self): return self.to_unit('ZB')

    def to_unit(self, unit : str) -> float:

        return self._value / self.__class__.sizes_bytes[unit]
