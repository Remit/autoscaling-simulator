import os

from matplotlib import pyplot as plt

from .. import plotting_constants

class FulfilledDroppedBarchart:

    FILENAME = 'bars_fulfilled_failed.png'

    @classmethod
    def plot(cls : type,
             response_times_regionalized : dict,
             workload_regionalized : dict,
             bar_width : float = 0.15,
             figures_dir = None):

        """
        Builds a barchart of fulfilled requests vs dropped,
        a bar for each request type.
        """

        for region_name, workload_ts_per_request_type in workload_regionalized.items():
            plt.figure()
            if region_name in response_times_regionalized:
                response_times_per_request_type = response_times_regionalized[region_name]
                req_types = list(workload_ts_per_request_type.keys())
                succeeded_reqs = []
                failed_reqs = []
                max_req_cnt = 0
                for req_type, workload_timeline in workload_ts_per_request_type.items():
                    responses_cnt = 0
                    if req_type in response_times_per_request_type:
                        responses_cnt = len(response_times_per_request_type[req_type])

                    succeeded_reqs.append(responses_cnt)

                    requests_cnt = 0
                    for _, cnt in workload_timeline:
                        requests_cnt += cnt

                    failed_reqs_cnt = requests_cnt - responses_cnt
                    failed_reqs.append(failed_reqs_cnt)

                    max_req_cnt = max([max_req_cnt, requests_cnt])

                plt.bar(req_types, succeeded_reqs,
                        bar_width, label='Fulfilled')
                plt.bar(req_types, failed_reqs,
                        bar_width, bottom = succeeded_reqs, label='Failed')

                plt.ylabel('Requests count')
                plt.ylim(top = int(max_req_cnt * 1.05))
                plt.legend()

                if not figures_dir is None:
                    figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                    plt.savefig(figure_path)
                else:
                    plt.suptitle(f'Fulfilled and failed requests in region {region_name}')
                    plt.show()
