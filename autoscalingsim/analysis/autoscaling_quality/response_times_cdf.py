import os
import collections

import pandas as pd
import numpy as np

from matplotlib import pyplot as plt

import autoscalingsim.analysis.plotting_constants as plotting_constants

class ResponseTimesCDF:

    FILENAME = 'cdf_response_times.png'

    @classmethod
    def comparative_plot(cls: type, simulations_by_name : dict, simulation_step : pd.Timedelta, figures_dir : str = None):

        regions = list()
        response_times_regionalized_aggregated = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(list)))
        for simulation_name, simulation_instances in simulations_by_name.items():

            for simulation in simulation_instances:
                response_times_regionalized = simulation.application_model.response_stats.get_response_times_by_request()
                for region_name, response_times_per_request_type in response_times_regionalized.items():
                    for req_type, response_times in response_times_per_request_type.items():
                        response_times_regionalized_aggregated[simulation_name][region_name][req_type].extend(response_times)

                    regions.append(region_name)

        for region_name in regions:
            fig = plt.figure()
            ax = fig.add_subplot(1, 1, 1)

            resp_times_maximums = list()
            for simulation_name, response_times_regionalized_per_sim in response_times_regionalized_aggregated.items():
                resp_times_maximums.append(cls._internal_plot(ax, response_times_regionalized_per_sim[region_name], simulation_step, simulation_name))

            cls._internal_post_processing(ax, region_name, max(resp_times_maximums), figures_dir)

    @classmethod
    def plot(cls : type, response_times_regionalized : dict, simulation_step : pd.Timedelta, figures_dir : str = None):

        for region_name, response_times_per_request_type in response_times_regionalized.items():
            fig = plt.figure()
            ax = fig.add_subplot(1, 1, 1)
            resp_times_maximum = cls._internal_plot(ax, response_times_per_request_type, simulation_step)
            cls._internal_post_processing(ax, region_name, resp_times_maximum, figures_dir)

    @classmethod
    def _internal_plot(cls : type, ax, response_times_per_request_type : dict, simulation_step : pd.Timedelta, additional_label : str = None):

        present_request_types_cnt = len([ True for resp_times in response_times_per_request_type.values() if len(resp_times) > 0 ])

        if present_request_types_cnt > 0:
            simulation_step_ms = simulation_step.microseconds // 1000
            max_response_times_by_req_type = [max(response_times_of_req) for response_times_of_req in response_times_per_request_type.values() if len(response_times_of_req) > 0 ]
            max_response_time = max(max_response_times_by_req_type) if len(max_response_times_by_req_type) > 0 else 0
            cdf_xlim = int(max_response_time + simulation_step_ms)
            x_axis = range(0, cdf_xlim, simulation_step_ms)

            cdfs_per_req_type = {}
            for req_type, response_times in response_times_per_request_type.items():
                reqs_count_binned = [0] * len(x_axis)

                for response_time in response_times:
                    reqs_count_binned[int(response_time // simulation_step_ms)] += 1

                cdfs_per_req_type[req_type] = np.cumsum(reqs_count_binned) / sum(reqs_count_binned)

            for req_type, cdf_vals in cdfs_per_req_type.items():
                lbl = f'{additional_label}: {req_type}' if not additional_label is None else req_type
                ax.plot(x_axis, cdf_vals, label = lbl)

            return max(x_axis)

        else:
            return 0

    @classmethod
    def _internal_post_processing(cls : type, ax, region_name : str, resp_times_maximum : float, figures_dir : str = None):

        percentiles = [0.99, 0.95, 0.90, 0.80, 0.50]
        font = {'color':  'black', 'weight': 'normal', 'size': 8}
        for percentile in percentiles:
            ax.hlines(percentile, 0, resp_times_maximum, colors='k', linestyles='dashed', lw = 0.5)
            ax.text(0, percentile + 0.001, f"{(int(percentile * 100))}th percentile", fontdict = font)

        plt.xlabel('Response time, ms')
        plt.legend(loc = "lower right")

        if not figures_dir is None:
            figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
            plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
        else:
            plt.title(f'CDF of requests by response time in region {region_name}')
            plt.show()
