import pandas as pd

from matplotlib import pyplot as plt

import ..plotting_constants

class WorkloadLineGraph:

    FILENAME = 'ts_line_workload.png'

    @staticmethod
    def plot(workload_regionalized : dict,
             resolution_ms : int = 1000,
             figures_dir = None):

        """
        Line graph (x axis - time) of the desired/current node count,
        separately for each node type
        """

        for region_name, workload_ts_per_request_type in workload_regionalized.items():
            for req_type, workload_ts_raw in workload_ts_per_request_type.items():

                workload_ts_times_ms = []
                workload_ts_req_counts = []
                new_frame_start_ms = workload_ts_raw[0][0] + resolution_ms
                cur_req_cnt = 0
                last_added = False
                for workload_obs in workload_ts_raw:
                    last_added = False

                    cur_ts_ms = workload_obs[0]
                    reqs_cnt = workload_obs[1]

                    if cur_ts_ms > new_frame_start_ms:
                        workload_ts_times_ms.append(new_frame_start_ms)
                        workload_ts_req_counts.append(cur_req_cnt)
                        cur_req_cnt = 0
                        new_frame_start_ms = cur_ts_ms + resolution_ms
                        last_added = True

                    cur_req_cnt += reqs_cnt

                if not last_added:
                    workload_ts_times_ms.append(new_frame_start_ms)
                    workload_ts_req_counts.append(cur_req_cnt)

                workload_ts_time = [datetime.fromtimestamp(workload_ts_time_ms // 1000) for workload_ts_time_ms in workload_ts_times_ms]

                df_workload = pd.DataFrame(data = {'time': workload_ts_time,
                                                   'requests': workload_ts_req_counts})
                df_workload = df_workload.set_index('time')
                plt.plot(df_workload, label = req_type)

                plt.ylabel('Workload, requests per {} s'.format(resolution_ms // 1000))
                plt.legend(loc = "lower right")
                plt.xticks(rotation = 70)

            if not figures_dir is None:
                figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, WorkloadLineGraph.FILENAME))
                plt.savefig(figure_path)
            else:
                plt.title('Generated workload over time in region {}'.format(region_name))
                plt.show()
