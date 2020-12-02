import os
import collections
import numpy as np
import matplotlib.ticker as ticker
from matplotlib import pyplot as plt

import autoscalingsim.analysis.plotting_constants as plotting_constants

class DistributionRequestsTimesBarchart:

    FILENAME = 'bars_req_time_distribution_by_cat.png'

    @classmethod
    def plot(cls : type,
             response_times_regionalized : dict,
             buffer_times_regionalized : dict,
             network_times_regionalized : dict,
             aggregation_fn = np.mean,
             bar_width : float = 0.15,
             figures_dir : str = None):

        """
        Barchart of the processing vs waiting vs network time
        for the fulfilled requests, a bar for each request type
        """

        if not callable(aggregation_fn):
            raise ValueError('The aggregation function object is not callable.')

        for region_name, response_times_per_request_type in response_times_regionalized.items():
            present_request_types_cnt = len([ True for resp_times in response_times_per_request_type.values() if len(resp_times) > 0 ])

            if present_request_types_cnt > 0:
                fig, ax = plt.subplots(nrows = 1, ncols = 1, figsize = (3 * present_request_types_cnt, 3))
                buffer_times_by_request = buffer_times_regionalized[region_name]
                network_times_by_request = network_times_regionalized[region_name]

                aggregated_processing_time_per_req_type = []
                aggregated_buf_waiting_time_per_req_type = []
                aggregated_network_time_per_req_type = []

                req_types = list(response_times_per_request_type.keys())
                max_response_time = 0
                for req_type in req_types:

                    req_type_response_times = response_times_per_request_type[req_type]

                    req_type_network_times = [0.0]
                    if req_type in network_times_by_request:
                        req_type_network_times = network_times_by_request[req_type]

                    aggregated_network_time_per_req_type.append(aggregation_fn(req_type_network_times))

                    req_type_buf_waiting_times = [0.0]
                    if req_type in buffer_times_by_request:
                        req_type_buf_waiting_times = []
                        if isinstance(buffer_times_by_request[req_type], collections.Mapping):
                            for buf_wait_time_per_service in buffer_times_by_request[req_type].values():
                                req_type_buf_waiting_times.extend(list(buf_wait_time_per_service))

                    aggregated_buf_waiting_time_per_req_type.append(aggregation_fn(req_type_buf_waiting_times))

                    agg_proc_time = aggregation_fn(req_type_response_times) - (aggregated_buf_waiting_time_per_req_type[-1] + aggregated_network_time_per_req_type[-1])
                    aggregated_processing_time_per_req_type.append(agg_proc_time)

                    max_response_time = max(max_response_time, max(req_type_response_times))

                ax.bar(req_types, aggregated_processing_time_per_req_type,
                       bar_width, label='Processing')
                ax.bar(req_types, aggregated_network_time_per_req_type,
                       bar_width, bottom = aggregated_processing_time_per_req_type,
                       label='Transferring')

                ax.bar(req_types, aggregated_buf_waiting_time_per_req_type,
                       bar_width, bottom = np.array(aggregated_processing_time_per_req_type) \
                                            + np.array(aggregated_network_time_per_req_type),
                       label='Waiting')

                ax.set_ylabel('Duration, ms')
                ax.set_ylim(0, (max_response_time + 10))
                ax.legend(loc = 'lower center', bbox_to_anchor=(0.5, -0.3), ncol = 3)

                ax.yaxis.set_major_locator(ticker.MultipleLocator(50))
                ax.yaxis.set_minor_locator(ticker.MultipleLocator(10))

                fig.tight_layout()

                if not figures_dir is None:
                    figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                    plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
                else:
                    plt.suptitle(f'Distribution of the request time in the application,\
                    \naggregated with the {aggregation_fn.__name__} function in region {region_name}')
                    plt.show()
