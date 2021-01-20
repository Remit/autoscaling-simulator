import os
import math
import collections
import pandas as pd

from matplotlib import pyplot as plt
import matplotlib.ticker as ticker
from collections.abc import Iterable

import stethoscope.plotting_constants as plotting_constants

def _roundup(x):

    return int(math.ceil(x / 100) * 100)

class UtilizationLineGraph:

    FILENAME = 'ts_line_utilization.png'

    @classmethod
    def plot(cls : type,
             utilization_per_service : dict,
             resolution : pd.Timedelta = pd.Timedelta(1000, unit = 'ms'),
             figures_dir = None):

        """
        Line graph (x axis - time) of the service system resources utilization,
        separately for each service-resource pair
        """

        utilization_regionalized = collections.defaultdict(lambda: collections.defaultdict(dict))
        for service_name, utilization_per_region in utilization_per_service.items():
            for region_name, utilization_per_resource in utilization_per_region.items():
                utilization_regionalized[region_name][service_name] = utilization_per_resource

        for region_name, utilization_per_service in utilization_regionalized.items():
            if len(utilization_per_service) > 0:
                fig, axs = plt.subplots(nrows = plotting_constants.SYSTEM_RESOURCES_CNT,
                                        ncols = len(utilization_per_service),
                                        figsize = (len(utilization_per_service) * plotting_constants.SQUARE_PLOT_SIDE_INCH,
                                                   plotting_constants.SYSTEM_RESOURCES_CNT * plotting_constants.SQUARE_PLOT_SIDE_INCH))
                font = {'color': 'black', 'weight': 'bold', 'size': 12}
                if not isinstance(axs, Iterable):
                    axs = [axs]
                if len(utilization_per_service) == 1:
                    axs = [axs]

                i = 0
                for service_name, utilization_per_resource in utilization_per_service.items():

                    j = 0
                    for resource_name, utilization_ts in utilization_per_resource.items():

                        utilization_ts.index = pd.to_datetime(utilization_ts.index)
                        utilization_ts.value = pd.to_numeric(utilization_ts.value)
                        resampled_utilization = utilization_ts.resample(resolution).mean() * 100
                        max_y_value = max(100, _roundup(resampled_utilization.value.max()))

                        major_ticks_interval_raw = math.ceil(max_y_value / (plotting_constants.SQUARE_PLOT_SIDE_INCH * plotting_constants.MAJOR_TICKS_PER_INCH))
                        major_ticks_interval = round(major_ticks_interval_raw, -max(len(str(major_ticks_interval_raw)) - 1, 0))
                        minor_ticks_interval = major_ticks_interval // plotting_constants.MINOR_TICKS_PER_MAJOR_TICK_INTERVAL

                        axs[j][i].plot(resampled_utilization, label = resource_name)

                        unit = resolution // pd.Timedelta(1000, unit = 'ms')
                        axs[j][i].set_ylabel(f'{resource_name} util.,\n% per {unit} s')
                        axs[j][i].set_title(f'Service {service_name[:plotting_constants.VARIABLE_NAMES_SIZE_LIMIT]}...', y = 1.2, fontdict = font)
                        axs[j][i].set_ylim(0, max_y_value)
                        axs[j][i].yaxis.set_major_locator(ticker.MultipleLocator(major_ticks_interval))
                        axs[j][i].yaxis.set_minor_locator(ticker.MultipleLocator(minor_ticks_interval))
                        axs[j][i].set_yticks([y for y in axs[j][i].get_yticks() if (y >= 0) and (y < max_y_value)] + [max_y_value])
                        plt.setp(axs[j][i].get_xticklabels(), rotation = 70, ha = "right", rotation_mode = "anchor")

                        j += 1

                    i += 1

                fig.tight_layout()

                if not figures_dir is None:
                    figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                    plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
                else:
                    plt.title(f'Resource utilization over time in region {region_name}')
                    plt.show()

                plt.close()
