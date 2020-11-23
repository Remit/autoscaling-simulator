import pandas as pd

from ..stabilizer import Stabilizer

from ......utils.error_check import ErrorChecker

@Stabilizer.register('maxStabilizer')
class MaxStabilizer(Stabilizer):

    """
    Stabilizes the oscillations in the scaled aspect by substituting the values
    in the time window for the max of the max value encountered in it and of the max
    value found in the previous time window. Tends to overprovision the capacity.
    """
    def __init__(self, config : dict):

        resolution_raw = ErrorChecker.key_check_and_load('resolution', config, self.__class__.__name__)
        resolution_value = ErrorChecker.key_check_and_load('value', resolution_raw, self.__class__.__name__)
        resolution_unit = ErrorChecker.key_check_and_load('unit', resolution_raw, self.__class__.__name__)
        self.resolution = pd.Timedelta(resolution_value, unit = resolution_unit)

    def __call__(self, values : pd.DataFrame):

        return values.resample(self.resolution).max().bfill()
