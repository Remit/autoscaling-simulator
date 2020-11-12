import pandas as pd

from ..regional_load_model import RegionalLoadModel

@RegionalLoadModel.register('constant')
class ConstantLoadModel(RegionalLoadModel):

    """
    Implementation of the load model that generates the same amount of load
    over the time.
    """

    def __init__(self,
                 region_name : str,
                 pattern : dict,
                 load_configs : dict,
                 simulation_step : pd.Timedelta):

        pass

    def generate_requests(self,
                          timestamp : pd.Timestamp):
        pass
