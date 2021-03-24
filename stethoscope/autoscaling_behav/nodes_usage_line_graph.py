import os
import math
import pandas as pd
import numpy as np

from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator
from collections.abc import Iterable

import stethoscope.plotting_constants as plotting_constants

from autoscalingsim.infrastructure_platform.platform_model import PlatformModel

class NodesUsageLineGraph:

    FILENAME = 'ts_line_nodes.png'

    @classmethod
    def plot(cls : type,
             desired_node_count_per_provider : dict,
             actual_node_count_per_provider : dict,
             resolution_ms : int = 5000,
             figures_dir = None):

        """
        Line graph (x axis - time) of the desired/current node count,
        separately for each node type
        """

        for provider_name, desired_node_count_per_region in desired_node_count_per_provider.items():
            for region_name, desired_node_count in desired_node_count_per_region.items():
                plt.figure()
                actual_node_count = list()
                if provider_name in actual_node_count_per_provider:
                    if region_name in actual_node_count_per_provider[provider_name]:
                        actual_node_count = actual_node_count_per_provider[provider_name][region_name]
                node_types = list(desired_node_count.keys())
                plots_count = len(node_types)
                rows_cnt = 1
                cols_cnt = plots_count
                if plots_count > plotting_constants.MAX_PLOTS_CNT_ROW:
                    rows_cnt = math.ceil(plots_count / plotting_constants.MAX_PLOTS_CNT_ROW)
                    cols_cnt = plotting_constants.MAX_PLOTS_CNT_ROW

                fig, axs = plt.subplots(rows_cnt, cols_cnt, sharey = True, tight_layout = True,
                                        figsize = (cols_cnt * 4, rows_cnt * 4))

                max_desired_count = 1

                if not isinstance(axs, Iterable):
                    axs = np.asarray([axs])

                axs_f = axs.flatten()[:len(node_types)]
                for node_type, ax in zip(node_types, axs_f):
                    
                    desired_ts = desired_node_count[node_type][PlatformModel.timestamps_key]
                    desired_count = desired_node_count[node_type][PlatformModel.node_count_key]
                    max_desired_count = max(max_desired_count, max(desired_count))

                    actual_ts = []
                    actual_count = []
                    if node_type in actual_node_count:
                        actual_ts = actual_node_count[node_type][PlatformModel.timestamps_key]
                        actual_count = actual_node_count[node_type][PlatformModel.node_count_key]

                    df_desired = pd.DataFrame(data = {'datetime': desired_ts, 'nodes': desired_count}).set_index('datetime')
                    df_actual = pd.DataFrame(data = {'datetime': actual_ts, 'nodes': actual_count}).set_index('datetime')

                    ax.plot(df_desired, label = "Desired count")
                    ax.plot(df_actual, label = "Actual count")

                    ax.set_title(f'Node type {node_type} ({provider_name})')
                    ax.set_ylabel('Node count')
                    ax.set_ylim(top = math.ceil(1.2 * max_desired_count))
                    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
                    ax.legend(loc = 'upper center', ncol = 2)
                    ax.tick_params('x', labelrotation = 70)

                if not figures_dir is None:
                    figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                    plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
                else:
                    plt.suptitle(f'Desired and actual number of nodes per node type in region {region_name}', y = 1.05)
                    plt.show()

                plt.close()
