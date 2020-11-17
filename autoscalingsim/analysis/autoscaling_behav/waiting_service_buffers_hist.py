import os
import math
import matplotlib.gridspec as gridspec

from matplotlib import pyplot as plt

from .. import plotting_constants

class WaitingServiceBuffersHistogram:

    FILENAME = 'hist_buf_waiting_time.png'

    @classmethod
    def plot(cls : type,
             buffer_times_regionalized : dict,
             bins_size_ms : int = 10,
             figures_dir = None):

        """
        Builds a set of histograms for the waiting times in buffers.
        """

        for region_name, buffer_times_by_request in buffer_times_regionalized.items():
            if len(buffer_times_by_request) > 0:
                outer_rows_cnt = len(buffer_times_by_request)
                outer_cols_cnt = 1
                fig = plt.figure()
                fig.add_subplot(111, frameon = False)
                plt.tick_params(labelcolor='none', top=False, bottom=False, left=False, right=False)
                outer = gridspec.GridSpec(outer_rows_cnt, outer_cols_cnt,
                                          wspace=0.2, hspace=1.3)
                font = {'color':  'black', 'weight': 'bold', 'size': 12}

                i = 0
                for req_type, buffers_waiting_times in buffer_times_by_request.items():

                    ax_out = plt.Subplot(fig, outer[i])
                    ax_out.set_title(f'Request type {req_type}', y = 1.4, fontdict = font)

                    ax_out.set_xlabel('Time spent waiting in the buffer, ms')
                    ax_out.set_ylabel('Waiting requests')
                    ax_out.xaxis.labelpad = 25
                    ax_out.yaxis.labelpad = 15
                    ax_out.set_xticks([])

                    fig.add_subplot(ax_out)
                    max_waiting_time = max([max(sublist) for sublist in list(buffers_waiting_times.values())])
                    bins_cnt = math.ceil(max_waiting_time / bins_size_ms)

                    plots_count = len(buffers_waiting_times)
                    rows_cnt = 1
                    cols_cnt = plots_count
                    if plots_count > plotting_constants.MAX_PLOTS_CNT_ROW:
                        rows_cnt = math.ceil(plots_count / plotting_constants.MAX_PLOTS_CNT_ROW)

                    # Plotting for req type
                    inner = gridspec.GridSpecFromSubplotSpec(rows_cnt,
                                                             cols_cnt,
                                                             subplot_spec = outer[i],
                                                             wspace = 0.5,
                                                             hspace = 0.1)

                    j = 0
                    for service_name, service_buffer_waiting_times in buffers_waiting_times.items():
                        ax = plt.Subplot(fig, inner[j], sharey = ax_out)
                        ax.hist(service_buffer_waiting_times, bins = bins_cnt)
                        ax.title.set_text(f'{service_name}\nbuffers')
                        if not ax.is_last_row():
                            plt.setp(ax.get_xticklabels(), visible=False)
                        plt.setp(ax.get_yticklabels(), visible=False)
                        fig.add_subplot(ax)
                        j += 1

                    i += 1

                if not figures_dir is None:
                    figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                    plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
                else:
                    plt.suptitle(f'Distribution of requests by buffer waiting time in {region_name}', y = 1.05)
                    plt.show()
