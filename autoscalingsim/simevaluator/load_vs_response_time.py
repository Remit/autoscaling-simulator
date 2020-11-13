import os
import pandas as pd

from matplotlib import pyplot as plt

from ..analysis import plotting_constants

class LoadVsResponseTimeGraph:

    FILENAME = 'line_load_vs_response_time.png'

    @classmethod
    def plot(cls : type,
             experiments_load : dict,
             experiments_response_times : dict,
             figures_dir = None):

        """
        Boxplot graph of the response time vs load in different experiments.
        X axis - load. Y axis - response times.
        """


        for region_name, load_per_req_type in experiments_load.items():
            if len(load_per_req_type) > 0:
                fig, axes = plt.subplots(nrows=1, ncols=len(load_per_req_type))
                i = 0
                font = {'color':  'black',
                        'weight': 'bold',
                        'size': 12}

                for req_type, load_per_experiment in load_per_req_type.items():
                    data_dict = {}
                    for experiment_id, load in load_per_experiment.items():
                        response_times = []
                        if region_name in experiments_response_times:
                            if req_type in experiments_response_times[region_name]:
                                response_times = experiments_response_times[region_name][req_type].get(experiment_id, [])

                        data_dict[int(load)] = response_times

                    axes[i].boxplot(data_dict.values())
                    axes[i].set_title(f'Request type {req_type}',
                                      y = 1.2,
                                      fontdict = font)
                    axes[i].set_xticklabels(data_dict.keys())
                    axes[i].set_xlabel(f'Load, rps')
                    axes[i].set_ylabel('Response time, ms')
                    i += 1

                fig.tight_layout()

                if not figures_dir is None:
                    figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                    plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
                else:
                    plt.suptitle(f'Load versus response time in {region_name}', y = 1.05)
                    plt.show()
