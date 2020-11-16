import os
import math
import pandas as pd

from matplotlib import pyplot as plt
from collections.abc import Iterable

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
                                    sharey = True, tight_layout = True,
                                    figsize = (cols_cnt * 4, rows_cnt * 4))

            # Ref: https://stackoverflow.com/questions/6963035/pyplot-axes-labels-for-subplots/36542971#36542971
            #fig.add_subplot(111, frameon = False)
            #plt.tick_params(labelcolor='none', top=False, bottom=False, left=False, right=False)

            max_desired_count = 1
            i = 0
            if not isinstance(axs, Iterable):
                axs = [axs]
                
            for ax in axs:

                node_type = node_types[i]
                desired_ts = desired_node_count[node_type]['timestamps']
                desired_count = desired_node_count[node_type]['count']
                max_desired_count = max(max_desired_count, max(desired_count))

                actual_ts = []
                actual_count = []
                if node_type in actual_node_count:
                    actual_ts = actual_node_count[node_type]['timestamps']
                    actual_count = actual_node_count[node_type]['count']

                df_desired = pd.DataFrame(data = {'datetime': desired_ts, 'nodes': desired_count}).set_index('datetime')
                df_actual = pd.DataFrame(data = {'datetime': actual_ts, 'nodes': actual_count}).set_index('datetime')
                #common_index = df_desired.index.union(df_actual.index)
                #ticklabels = df.index.strftime('%Y-%m-%d')

                ax.set_title(f'Node type {node_type}')
                ax.plot(df_desired, label = "Desired count")
                ax.plot(df_actual, label = "Actual count")

                #fig.canvas.draw()
                #axs.set_xticklabels([txt.get_text() for txt in axs.get_xticklabels()], rotation = 70)

                ax.set_ylabel('Node count')
                ax.set_ylim(top = math.ceil(1.2 * max_desired_count))

                #handles, labels = axs.get_legend_handles_labels()
                ax.legend(loc = 'upper center', ncol = 2) # legend =
                #ax.set_xticklabels(common_index, rotation = 70)
                ax.tick_params('x', labelrotation = 70)

                i += 1

                #fig.canvas.draw()
                #axs.set_xticklabels([txt.get_text() for txt in axs.get_xticklabels()], rotation = 70)

            if not figures_dir is None:
                figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
            else:
                plt.suptitle(f'Desired and actual number of nodes per node type in region {region_name}', y = 1.05)
                plt.show()
