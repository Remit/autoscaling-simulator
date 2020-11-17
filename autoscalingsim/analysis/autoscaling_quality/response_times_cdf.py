import os

import pandas as pd
import numpy as np

from matplotlib import pyplot as plt

from .. import plotting_constants

class ResponseTimesCDF:

    FILENAME = 'cdf_response_times.png'

    @classmethod
    def plot(cls : type,
             response_times_regionalized : dict,
             simulation_step : pd.Timedelta,
             figures_dir = None):

        """
        Builds CDF of the requests by the response times, separate line for
        each request type.
        """

        for region_name, response_times_per_request_type in response_times_regionalized.items():
            simulation_step_ms = simulation_step.microseconds // 1000
            plt.figure()
            max_response_time = max([max(response_times_of_req) for response_times_of_req in response_times_per_request_type.values()])
            cdf_xlim = int(max_response_time + simulation_step_ms)
            x_axis = range(0, cdf_xlim, simulation_step_ms)

            cdfs_per_req_type = {}
            for req_type, response_times in response_times_per_request_type.items():
                reqs_count_binned = [0] * len(x_axis)

                for response_time in response_times:
                    reqs_count_binned[int(response_time // simulation_step_ms)] += 1

                cdfs_per_req_type[req_type] = np.cumsum(reqs_count_binned) / sum(reqs_count_binned)

            for req_type, cdf_vals in cdfs_per_req_type.items():
                _ = plt.plot(x_axis, cdf_vals, label = req_type)

            percentiles = [0.99, 0.95, 0.90, 0.80, 0.50]
            font = {'color':  'black', 'weight': 'normal', 'size': 8}
            for percentile in percentiles:
                plt.hlines(percentile, min(x_axis), max(x_axis),
                           colors='k', linestyles='dashed', lw = 0.5)
                plt.text(0, percentile + 0.001,
                         f"{(int(percentile * 100))}th percentile",
                         fontdict = font)

            plt.xlabel('Response time, ms')
            plt.legend(loc = "lower right")

            if not figures_dir is None:
                figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
            else:
                plt.title(f'CDF of requests by response time in region {region_name}')
                plt.show()
