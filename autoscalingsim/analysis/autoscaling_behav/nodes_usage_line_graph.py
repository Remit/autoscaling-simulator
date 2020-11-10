import os
import math
import pandas as pd

from matplotlib import pyplot as plt

from .. import plotting_constants

class NodesUsageLineGraph:

    FILENAME = 'ts_line_nodes.png'

    @classmethod
    def plot(cls : type,
             desired_node_count_regionalized : dict,
             actual_node_count_regionalized : dict,
             resolution_ms : int = 5000,
             figures_dir = None):

        """
        Line graph (x axis - time) of the desired/current node count,
        separately for each node type
        """

        for region_name, desired_node_count in desired_node_count_regionalized.items():
            plt.figure()
            actual_node_count = actual_node_count_regionalized[region_name]
            node_types = list(desired_node_count.keys())
            plots_count = len(node_types)
            rows_cnt = 1
            cols_cnt = plots_count
            if plots_count > plotting_constants.MAX_PLOTS_CNT_ROW:
                rows_cnt = math.ceil(plots_count / plotting_constants.MAX_PLOTS_CNT_ROW)

            fig, axs = plt.subplots(rows_cnt, cols_cnt,
                                    sharey = True, tight_layout = True)

            # Ref: https://stackoverflow.com/questions/6963035/pyplot-axes-labels-for-subplots/36542971#36542971
            fig.add_subplot(111, frameon = False)
            plt.tick_params(labelcolor='none', top=False, bottom=False, left=False, right=False)

            max_desired_count = 1
            for node_type in node_types:

                desired_ts = desired_node_count[node_type]['timestamps']
                desired_count = desired_node_count[node_type]['count']
                max_desired_count = max(max_desired_count, max(desired_count))

                actual_ts = []
                actual_count = []
                if node_type in actual_node_count:
                    actual_ts = actual_node_count[node_type]['timestamps']
                    actual_count = actual_node_count[node_type]['count']

                df_desired = pd.DataFrame(data = {'time': desired_ts,
                                                  'nodes': desired_count})
                df_actual = pd.DataFrame(data = {'time': actual_ts,
                                                 'nodes': actual_count})
                df_desired = df_desired.set_index('time')
                df_actual = df_actual.set_index('time')

                axs.title.set_text(f'Node type {node_type}')
                _ = axs.plot(df_desired, label = "Desired count")
                _ = axs.plot(df_actual, label = "Actual count")

                #fig.canvas.draw()
                #axs.set_xticklabels([txt.get_text() for txt in axs.get_xticklabels()], rotation = 70)

            axs.set_ylabel('Node count')
            axs.set_ylim(top = math.ceil(1.2 * max_desired_count))

            handles, labels = axs.get_legend_handles_labels()
            legend = axs.legend(handles, labels, loc = 'upper center', ncol = 2)

            fig.canvas.draw()
            axs.set_xticklabels([txt.get_text() for txt in axs.get_xticklabels()], rotation = 70)

            if not figures_dir is None:
                figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
            else:
                plt.suptitle(f'Desired and actual number of nodes per node type in region {region_name}', y = 1.05)
                plt.show()
