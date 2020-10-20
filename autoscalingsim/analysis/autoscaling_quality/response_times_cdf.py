import os

import pandas as pd

from matplotlib import pyplot as plt

class ResponseTimesCDF:

    FILENAME = 'cdf_response_times.png'

    @staticmethod
    def plot(simulation_step : pd.Timedelta,
             response_times_per_request_type,
             figures_dir = None):
        """
        Builds CDF of the requests by the response times, separate line for
        each request type.
        """

        simulation_step_ms = int(simulation_step.microseconds / 1000)
        max_response_time = max([max(response_times_of_req) for response_times_of_req in response_times_per_request_type.values()])
        cdf_xlim = max_response_time + 1 * simulation_step_ms + 1
        x_axis = range(0, cdf_xlim, simulation_step_ms)

        cdfs_per_req_type = {}
        for req_type, response_times in response_times_per_request_type.items():
            reqs_count_binned = [0] * len(x_axis)

            for response_time in response_times:
                reqs_count_binned[response_time // simulation_step] += 1

            cdfs_per_req_type[req_type] = np.cumsum(reqs_count_binned) / sum(reqs_count_binned)

        for req_type, cdf_vals in cdfs_per_req_type.items():
            plt.plot(x_axis, cdf_vals, label = req_type)

        percentiles = [0.99, 0.95, 0.90, 0.80, 0.50]
        font = {'color':  'black',
                'weight': 'normal',
                'size': 8}
        for percentile in percentiles:
            plt.hlines(percentile, min(x_axis), max(x_axis),
                       colors='k', linestyles='dashed', lw = 0.5)
            plt.text(0, percentile + 0.001,
                     "{}th percentile".format(int(percentile * 100)),
                     fontdict = font)

        plt.xlabel('Response time, ms')
        plt.legend(loc = "lower right")

        if not figures_dir is None:
            figure_path = os.path.join(figures_dir, ResponseTimesCDF.FILENAME)
            plt.savefig(figure_path)
        else:
            plt.title('CDF of requests by response time')
            plt.show()
