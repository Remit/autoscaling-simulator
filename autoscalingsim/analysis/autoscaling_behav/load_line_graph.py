import os
import pandas as pd

from matplotlib import pyplot as plt

from .. import plotting_constants

class LoadLineGraph:

    FILENAME = 'ts_line_load.png'

    @classmethod
    def plot(cls : type,
             load_regionalized : dict,
             resolution : pd.Timedelta = pd.Timedelta(1000, unit = 'ms'),
             figures_dir = None):

        """
        Line graph (x axis - time) of the desired/current node count,
        separately for each node type
        """

        for region_name, load_ts_per_request_type in load_regionalized.items():
            plt.figure()
            for req_type, load_ts in load_ts_per_request_type.items():

                load_ts.index = pd.to_datetime(load_ts.index)
                load_ts.value = pd.to_numeric(load_ts.value)
                resampled_load = load_ts.resample(resolution).sum()

                _ = plt.plot(resampled_load, label = req_type)

                unit = resolution // pd.Timedelta(1000, unit = 'ms')
                plt.ylabel(f'load, requests per {unit} s')
                plt.legend(loc = "lower right")
                plt.xticks(rotation = 70)

            if not figures_dir is None:
                figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
            else:
                plt.title(f'Generated load over time in region {region_name}')
                plt.show()
