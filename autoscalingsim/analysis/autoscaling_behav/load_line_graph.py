import os
import pandas as pd

from matplotlib import pyplot as plt

from .. import plotting_constants

class LoadLineGraph:

    FILENAME = 'ts_line_load.png'

    @classmethod
    def plot(cls : type,
             load_regionalized : dict,
             resolution : pd.Timedelta = pd.Timedelta(1000, unit = 'ms'),
             figures_dir = None):

        """
        Line graph (x axis - time) of the desired/current node count,
        separately for each node type
        """

        for region_name, load_ts_per_request_type in load_regionalized.items():
            plt.figure()
            for req_type, load_ts_raw in load_ts_per_request_type.items():

                load_ts_times = []
                load_ts_req_counts = []
                new_frame_start = load_ts_raw[0][0] + resolution
                cur_req_cnt = 0
                last_added = False
                for load_obs in load_ts_raw:
                    last_added = False

                    cur_ts = load_obs[0]
                    reqs_cnt = load_obs[1]

                    if cur_ts > new_frame_start:
                        load_ts_times.append(new_frame_start)
                        load_ts_req_counts.append(cur_req_cnt)
                        cur_req_cnt = 0
                        new_frame_start = cur_ts + resolution
                        last_added = True

                    cur_req_cnt += reqs_cnt

                if not last_added:
                    load_ts_times.append(new_frame_start)
                    load_ts_req_counts.append(cur_req_cnt)

                df_load = pd.DataFrame(data = {'time': load_ts_times,
                                               'requests': load_ts_req_counts})
                df_load = df_load.set_index('time')
                _ = plt.plot(df_load, label = req_type)

                unit = resolution // pd.Timedelta(1000, unit = 'ms')
                plt.ylabel(f'load, requests per {unit} s')
                plt.legend(loc = "lower right")
                plt.xticks(rotation = 70)

            if not figures_dir is None:
                figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
            else:
                plt.title(f'Generated load over time in region {region_name}')
                plt.show()
