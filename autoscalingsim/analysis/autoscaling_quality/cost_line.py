import os
import collections
import pandas as pd

from matplotlib import pyplot as plt
import matplotlib.ticker as ticker

import autoscalingsim.analysis.plotting_constants as plotting_constants

class CostLineGraph:

    FILENAME = 'ts_line_cost.png'

    @classmethod
    def plot(cls : type,
             infrastructure_cost_per_provider : dict,
             resolution : pd.Timedelta(5000, unit = 'ms'),
             figures_dir = None):

        for provider_name, cost_per_region in infrastructure_cost_per_provider.items():
            for region_name, cost_in_time in cost_per_region.items():
                fig = plt.figure()
                ax = fig.add_subplot(1, 1, 1)
                ax.plot(cost_in_time.resample(resolution).mean())
                ax.set_title(f'Region {region_name} ({provider_name})')
                ax.set_ylabel('Total Cost, USD')
                ax.tick_params('x', labelrotation = 70)

                if not figures_dir is None:
                    figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(provider_name + '-' + region_name, cls.FILENAME))
                    plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
                else:
                    plt.suptitle(f'Cost of infrastructure in region {region_name} ({provider_name})', y = 1.05)
                    plt.show()
