import os
import collections

import pandas as pd
import numpy as np

from matplotlib import pyplot as plt

import stethoscope.plotting_constants as plotting_constants

class ResponseTimesCDF:

    FILENAME = 'cdf_response_times.png'

    @classmethod
    def comparative_plot_normalized(cls: type, simulations_by_name : dict, simulation_step : pd.Timedelta, figures_dir : str = None, names_converter = None):

        fulfilled_cnt_regionalized = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(int)))
        for simulation_name, simulation_instances in simulations_by_name.items():
            for simulation in simulation_instances:
                for region_name, response_times_per_request_type in simulation.response_times.items():
                    for req_type, response_times in response_times_per_request_type.items():
                        fulfilled_cnt_regionalized[region_name][req_type][simulation_name] += len(response_times)

        fulfilled_cnt_regionalized_maximums = collections.defaultdict(lambda: collections.defaultdict(int))
        for region_name, fulfilled_cnt_per_request_type in fulfilled_cnt_regionalized.items():
            for req_type, fulfilled_cnt_per_simulation in fulfilled_cnt_per_request_type.items():
                fulfilled_cnt_regionalized_maximums[region_name][req_type] = max(fulfilled_cnt_per_simulation.values())

        normalization_coefs = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(float)))
        for region_name, fulfilled_cnt_per_request_type in fulfilled_cnt_regionalized.items():
            for req_type, fulfilled_cnt_per_simulation in fulfilled_cnt_per_request_type.items():
                for sim_name, fulfilled_cnt in fulfilled_cnt_per_simulation.items():
                    normalization_coefs[sim_name][region_name][req_type] = fulfilled_cnt / fulfilled_cnt_regionalized_maximums[region_name][req_type]

        cls.comparative_plot(simulations_by_name, simulation_step, figures_dir, names_converter, normalization_coefs)

    @classmethod
    def comparative_plot(cls: type, simulations_by_name : dict, simulation_step : pd.Timedelta, figures_dir : str = None, names_converter = None, normalization_coefs = None):

        regions = list()
        response_times_regionalized_aggregated = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(list)))
        for simulation_name, simulation_instances in simulations_by_name.items():

            for simulation in simulation_instances:
                for region_name, response_times_per_request_type in simulation.response_times.items():
                    for req_type, response_times in response_times_per_request_type.items():
                        response_times_regionalized_aggregated[simulation_name][region_name][req_type].extend(response_times)

                    regions.append(region_name)

        for region_name in regions:
            fig = plt.figure()
            ax = fig.add_subplot(1, 1, 1)

            for simulation_name, response_times_regionalized_per_sim in response_times_regionalized_aggregated.items():
                simulation_name_as_label = names_converter(simulation_name)
                normalization_coef = normalization_coefs[simulation_name][region_name] if not normalization_coefs is None else None
                cls._internal_plot(ax, response_times_regionalized_per_sim[region_name], simulation_step, simulation_name_as_label, normalization_coef)

            cls._internal_post_processing(ax, region_name, figures_dir, len(response_times_regionalized_aggregated), normalization_coefs)

    @classmethod
    def plot(cls : type, response_times_regionalized : dict, simulation_step : pd.Timedelta, figures_dir : str = None):

        for region_name, response_times_per_request_type in response_times_regionalized.items():
            fig = plt.figure()
            ax = fig.add_subplot(1, 1, 1)
            cls._internal_plot(ax, response_times_per_request_type, simulation_step)
            cls._internal_post_processing(ax, region_name, figures_dir)

    @classmethod
    def _internal_plot(cls : type, ax, response_times_per_request_type : dict, simulation_step : pd.Timedelta, additional_label : str = None, normalization_coef : dict = None):

        present_request_types_cnt = len([ True for resp_times in response_times_per_request_type.values() if len(resp_times) > 0 ])

        if present_request_types_cnt > 0:
            simulation_step_ms = simulation_step.microseconds // 1000
            max_response_times_by_req_type = [max(response_times_of_req) for response_times_of_req in response_times_per_request_type.values() if len(response_times_of_req) > 0 ]
            max_response_time = max(max_response_times_by_req_type) if len(max_response_times_by_req_type) > 0 else 0
            cdf_xlim = int(max_response_time + simulation_step_ms)
            x_axis_step = max(simulation_step_ms, cdf_xlim // 100)
            x_axis = range(0, cdf_xlim, x_axis_step)

            cdfs_per_req_type = {}
            for req_type, response_times in response_times_per_request_type.items():
                reqs_count_binned = [0] * len(x_axis)

                for response_time in response_times:
                    reqs_count_binned[int(response_time // x_axis_step)] += 1

                cdfs_per_req_type[req_type] = np.cumsum(reqs_count_binned) / sum(reqs_count_binned)
                if not normalization_coef is None:
                    cdfs_per_req_type[req_type] *= normalization_coef[req_type]

            for req_type, cdf_vals in cdfs_per_req_type.items():
                lbl = f'{additional_label}:\n{req_type}' if not additional_label is None else req_type
                ax.plot(x_axis, cdf_vals, label = lbl)

    @classmethod
    def _internal_post_processing(cls : type, ax, region_name : str, figures_dir : str = None, ncol : int = 1, normalization_coefs : dict = None):

        percentiles = [0.99, 0.95, 0.90, 0.80, 0.50]
        font = {'color':  'black', 'weight': 'normal', 'size': 8}
        for percentile in percentiles:
            ax.axhline(percentile, 0, 1.0, color = 'k', linestyle = 'dashed', lw = 0.5)
            ax.text(0, percentile + 0.002, f"{(int(percentile * 100))}th percentile", fontdict = font)

        plt.xlabel('Response time, ms')
        plt.legend(loc = 'upper center', ncol = min(ncol, 2), bbox_to_anchor = (0.5, -0.15))

        if not figures_dir is None:
            fname = plotting_constants.filename_format.format(region_name, cls.FILENAME)
            if not normalization_coefs is None:
                fname = f'normalized-{fname}'
            figure_path = os.path.join(figures_dir, fname)
            plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
        else:
            plt.title(f'CDF of requests by response time in region {region_name}')
            plt.show()

        plt.close()
