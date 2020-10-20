import os

from matplotlib import pyplot as plt

class GeneratedRequestsByType:

    FILENAME = 'bars_total_gen_reqs.png'

    @staticmethod
    def _generated_requests_by_type_barchart(workload_ts_per_request_type,
                                             bar_width = 0.15,
                                             figures_dir = None):
        """
        Barchart of the overall amount of generated requests by type.
        """

        req_types = list(workload_ts_per_request_type.keys())
        reqs_cnts = {}
        max_req_cnt = 0
        for req_type, workload_timeline in workload_ts_per_request_type.items():

            requests_cnt = 0
            for _, cnt in workload_timeline:
                requests_cnt += cnt

            reqs_cnts[req_type] = requests_cnt

            max_req_cnt = max([max_req_cnt, requests_cnt])

        plt.bar(list(reqs_cnts.keys()), list(reqs_cnts.values()),
                bar_width)

        plt.ylabel('Requests count')
        plt.ylim(top = int(max_req_cnt * 1.05))

        if not figures_dir is None:
            figure_path = os.path.join(figures_dir, GeneratedRequestsByType.FILENAME)
            plt.savefig(figure_path)
        else:
            plt.suptitle('Total generated requests by type')
            plt.show()
