import os
import math
import matplotlib.ticker as ticker

from matplotlib import pyplot as plt
from collections.abc import Iterable

import autoscalingsim.analysis.plotting_constants as plotting_constants

class ResponseTimesHistogram:

    FILENAME = 'hist_response_times.png'

    @classmethod
    def plot(cls : type,
             response_times_regionalized : dict,
             bins_size_ms : int = 10,
             figures_dir = None):

        """
        Builds histogram of requests by the response time.
        """

        for region_name, response_times_per_request_type in response_times_regionalized.items():

            present_request_types_cnt = len([ True for resp_times in response_times_per_request_type.values() if len(resp_times) > 0 ])

            if present_request_types_cnt > 0:
                plt.figure()
                max_response_times_by_req_type = [max(response_times_of_req) for response_times_of_req in response_times_per_request_type.values() if len(response_times_of_req) > 0]
                max_response_time = max(max_response_times_by_req_type) if len(max_response_times_by_req_type) > 0 else 0
                bins_cnt = max(math.ceil(max_response_time / bins_size_ms), 1)

                plots_count = len(response_times_per_request_type)
                rows_cnt = 1
                cols_cnt = plots_count
                if plots_count > plotting_constants.MAX_PLOTS_CNT_ROW:
                    rows_cnt = math.ceil(plots_count / plotting_constants.MAX_PLOTS_CNT_ROW)
                    cols_cnt = plotting_constants.MAX_PLOTS_CNT_ROW

                fig, axs = plt.subplots(rows_cnt, cols_cnt, sharey = True, tight_layout = True)

                # Ref: https://stackoverflow.com/questions/6963035/pyplot-axes-labels-for-subplots/36542971#36542971
                fig.add_subplot(111, frameon = False)
                plt.tick_params(labelcolor='none', top=False, bottom=False, left=False, right=False)

                if not isinstance(axs, Iterable):
                    axs = [axs]

                i = 0
                for req_type, response_times in response_times_per_request_type.items():
                    # TODO: '<' not supported between instances of 'dict' and 'float'
                    axs[i].hist(response_times, bins = bins_cnt)
                    axs[i].title.set_text(req_type)
                    axs[i].xaxis.set_major_formatter(ticker.StrMethodFormatter("{x:.0f}"))
                    i += 1

                plt.xlabel('Response time, ms')
                plt.ylabel('Completed requests')

                if not figures_dir is None:
                    figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                    plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
                else:
                    plt.suptitle(f'Distribution of requests by response time in region {region_name}', y = 1.05)
                    plt.show()

                plt.close()
