import os

from matplotlib import pyplot as plt

from .. import plotting_constants

class GeneratedRequestsByType:

    FILENAME = 'bars_total_gen_reqs.png'

    @classmethod
    def plot(cls : type,
             load_regionalized : dict,
             bar_width : float = 0.15,
             figures_dir = None):
        """
        Barchart of the overall amount of generated requests by type.
        """

        for region_name, load_ts_per_request_type in load_regionalized.items():
            plt.figure()
            reqs_cnts = {}
            max_req_cnt = 0
            for req_type, load_timeline in load_ts_per_request_type.items():
                reqs_cnts[req_type] = sum(load_timeline.value)
                max_req_cnt = max([max_req_cnt, reqs_cnts[req_type]])

            plt.bar(list(reqs_cnts.keys()), list(reqs_cnts.values()), bar_width)

            plt.ylabel('Requests count')
            plt.ylim(top = int(max_req_cnt * 1.05))

            if not figures_dir is None:
                figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
            else:
                plt.suptitle(f'Total generated requests by type in region {region_name}')
                plt.show()
