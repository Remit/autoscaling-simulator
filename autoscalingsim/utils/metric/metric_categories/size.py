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

    @classmethod
    def to_metric(cls, config : dict):

        val = ErrorChecker.key_check_and_load('value', config)
        unit = ErrorChecker.key_check_and_load('unit', config)

        return cls(val, unit = unit)

    def __init__(self, value : float = 0.0, unit : str = 'B'):

        if not unit in self.__class__.sizes_bytes and not unit.upper() in self.__class__.sizes_bytes:
            raise ValueError(f'Unknown unit {unit}')

        #if value < 0: value = 0

        normalizer = 1 if unit in self.__class__.sizes_bytes else 8 # handles the case of *bit unit
        self._value = (value / normalizer) * self.__class__.sizes_bytes[unit.upper()]

    def to_bytes(self): return self._to_unit('B')

    def to_kilobytes(self): return self._to_unit('kB')

    def to_megabytes(self): return self._to_unit('MB')

    def to_gigabytes(self): return self._to_unit('GB')

    def to_terabytes(self): return self._to_unit('TB')

    def to_petabytes(self): return self._to_unit('PB')

    def to_exabytes(self): return self._to_unit('EB')

    def to_zetabytes(self): return self._to_unit('ZB')

    def _to_unit(self, unit : str) -> float:

        return self._value / self.__class__.sizes_bytes[unit]
