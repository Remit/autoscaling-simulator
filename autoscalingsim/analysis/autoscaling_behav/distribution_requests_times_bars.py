import os
import numpy as np

from matplotlib import pyplot as plt

import ..plotting_constants

class DistributionRequestsTimesBarchart:

    FILENAME = 'bars_req_time_distribution_by_cat.png'

    @staticmethod
    def plot(response_times_regionalized : dict,
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
            buffer_times_by_request = buffer_times_regionalized[region_name]
            network_times_by_request = network_times_regionalized[region_name]

            aggregated_processing_time_per_req_type = []
            aggregated_buf_waiting_time_per_req_type = []
            aggregated_network_time_per_req_type = []

            req_types = list(response_times_per_request_type.keys())
            for req_type in req_types:

                req_type_response_times = response_times_per_request_type[req_type]

                req_type_network_times = [0.0]
                if req_type in network_times_by_request:
                    req_type_network_times = network_times_by_request[req_type]
                aggregated_network_time_per_req_type.append(aggregation_fn(req_type_network_times))

                req_type_buf_waiting_times = [0.0]
                if req_type in buffer_times_by_request:
                    req_type_buf_waiting_times = [list(buf_wait_time.values())[0] for buf_wait_time in buffer_times_by_request[req_type]]
                aggregated_buf_waiting_time_per_req_type.append(aggregation_fn(req_type_buf_waiting_times))

                agg_proc_time = aggregation_fn(req_type_response_times) - (aggregated_buf_waiting_time_per_req_type[-1] + aggregated_network_time_per_req_type[-1])
                aggregated_processing_time_per_req_type.append(agg_proc_time)

            plt.bar(req_types, aggregated_processing_time_per_req_type,
                    bar_width, label='Processing')
            plt.bar(req_types, aggregated_network_time_per_req_type,
                    bar_width, bottom = aggregated_processing_time_per_req_type,
                    label='Transferring')

            plt.bar(req_types, aggregated_buf_waiting_time_per_req_type,
                    bar_width, bottom = np.array(aggregated_processing_time_per_req_type) \
                                        + np.array(aggregated_network_time_per_req_type),
                    label='Waiting')

            plt.ylabel('Duration, ms')
            plt.legend(loc = 'upper right', bbox_to_anchor=(0.9, -0.1), ncol = 3)

            if not figures_dir is None:
                figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, DistributionRequestsTimesBarchart.FILENAME))
                plt.savefig(figure_path)
            else:
                plt.suptitle('Distribution of the request time in the application,\
                \naggregated with the {} function in region {}'.format(aggregation_fn.__name__, region_name))
                plt.show()
