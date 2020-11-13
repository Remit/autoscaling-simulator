import os
import pandas as pd

from matplotlib import pyplot as plt

from .. import plotting_constants

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

        utilization_regionalized = {}
        for service_name, utilization_per_region in utilization_per_service.items():
            for region_name, utilization_per_resource in utilization_per_region.items():
                if not region_name in utilization_regionalized:
                    utilization_regionalized[region_name] = {}
                utilization_regionalized[region_name][service_name] = utilization_per_resource

        for region_name, utilization_per_service in utilization_regionalized.items():
            if len(utilization_per_service) > 0:
                fig, axes = plt.subplots(nrows = plotting_constants.SYSTEM_RESOURCES_CNT,
                                         ncols = len(utilization_per_service),
                                         figsize = (plotting_constants.SYSTEM_RESOURCES_CNT * 4, len(utilization_per_service) * 4))
                font = {'color':  'black',
                        'weight': 'bold',
                        'size': 12}
                i = 0
                for service_name, utilization_per_resource in utilization_per_service.items():

                    j = 0
                    for resource_name, utilization_ts in utilization_per_resource.items():

                        utilization_ts.index = pd.to_datetime(utilization_ts.index)
                        utilization_ts.value = pd.to_numeric(utilization_ts.value)
                        resampled_utilization = utilization_ts.resample(resolution).mean()

                        axes[j][i].plot(resampled_utilization, label = resource_name)

                        unit = resolution // pd.Timedelta(1000, unit = 'ms')
                        axes[j][i].set_ylabel(f'{resource_name} utilization, % per {unit} s')
                        axes[j][i].legend(loc = "lower right")
                        axes[j][i].set_title(f'Service {service_name}',
                                             y = 1.2,
                                             fontdict = font)
                        plt.setp(axes[j][i].get_xticklabels(), rotation = 70, ha = "right", rotation_mode = "anchor")

                        j += 1

                    i += 1

                fig.tight_layout()

                if not figures_dir is None:
                    figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                    plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
                else:
                    plt.title(f'Resource utilization over time in region {region_name}')
                    plt.show()
