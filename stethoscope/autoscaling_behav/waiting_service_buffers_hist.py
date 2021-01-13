import os
import math
import numpy as np
import collections
import matplotlib.gridspec as gridspec
import matplotlib.ticker as ticker

from matplotlib import pyplot as plt

import stethoscope.plotting_constants as plotting_constants

class WaitingServiceBuffersHistogram:

    FILENAME = 'hist_buf_waiting_time.png'

    @classmethod
    def plot(cls : type,
             buffer_times_regionalized : dict,
             figures_dir = None):

        """
        Builds a set of histograms for the waiting times in buffers.
        """

        for region_name, buffer_times_by_request in buffer_times_regionalized.items():
            if len(buffer_times_by_request) > 0:
                outer_rows_cnt = len([True for buffers_waiting_times in buffer_times_by_request.values() if len(buffers_waiting_times) > 0])
                outer_cols_cnt = 1

                if outer_rows_cnt > 0:
                    services_order = {}
                    plot_id = 0
                    global_max_waiting_time = 0
                    for req_type, buffers_waiting_times in buffer_times_by_request.items():
                        if isinstance(buffers_waiting_times, collections.Mapping):
                            for service_name, service_buffer_waiting_times in buffers_waiting_times.items():
                                global_max_waiting_time = max(global_max_waiting_time, max(service_buffer_waiting_times))
                                if (not service_name in services_order) and len(service_buffer_waiting_times) > 0:
                                    services_order[service_name] = plot_id
                                    plot_id += 1

                    max_cnt_of_services = len(services_order)#max([len(buffers_waiting_times) for buffers_waiting_times in buffer_times_by_request.values()])

                    inner_rows_cnt = 1
                    inner_cols_cnt = max_cnt_of_services
                    if max_cnt_of_services > plotting_constants.MAX_PLOTS_CNT_ROW:
                        inner_rows_cnt = math.ceil(max_cnt_of_services / plotting_constants.MAX_PLOTS_CNT_ROW)
                        inner_cols_cnt = plotting_constants.MAX_PLOTS_CNT_ROW

                    if inner_cols_cnt > 0:
                        fig = plt.figure(figsize = (plotting_constants.SQUARE_PLOT_SIDE_INCH * inner_cols_cnt, plotting_constants.SQUARE_PLOT_SIDE_INCH * inner_rows_cnt * outer_rows_cnt))
                        fig.add_subplot(111, frameon = False)
                        plt.tick_params(labelcolor = 'none', top = False, bottom = False, left = False, right = False)
                        outer = gridspec.GridSpec(outer_rows_cnt, outer_cols_cnt, wspace = 0.2, hspace = 1.3)
                        font = {'color':  'black', 'weight': 'bold', 'size': 10}

                        i = 0
                        for req_type, buffers_waiting_times in buffer_times_by_request.items():

                            if len(buffers_waiting_times) > 0:
                                ax_out = fig.add_subplot(outer[i], frameon = False)
                                ax_out.set_title(f'Request type {req_type}', y = 1.05, fontdict = font)
                                ax_out.set_xlabel('Time spent waiting in the service buffer, ms')
                                ax_out.set_ylabel('Waiting requests')
                                ax_out.xaxis.labelpad = len(str(int(global_max_waiting_time))) * 10
                                ax_out.yaxis.labelpad = 25
                                ax_out.set_xticks([])
                                ax_out.set_yticks([])

                                fig.add_subplot(ax_out)
                                max_waiting_time = max([max(sublist) for sublist in list(buffers_waiting_times.values())])
                                bins_cnt = plotting_constants.HISTOGRAM_BINS_PER_INCH * plotting_constants.SQUARE_PLOT_SIDE_INCH
                                binwidth = global_max_waiting_time / bins_cnt

                                # Plotting for req type
                                inner = gridspec.GridSpecFromSubplotSpec(inner_rows_cnt, inner_cols_cnt, subplot_spec = outer[i],
                                                                         wspace = 0.25, hspace = len(str(int(global_max_waiting_time))) * 0.1)

                                ax = None
                                if isinstance(buffers_waiting_times, collections.Mapping):
                                    for service_name, service_buffer_waiting_times in buffers_waiting_times.items():
                                        if len(service_buffer_waiting_times) > 0:

                                            ax = fig.add_subplot(inner[services_order[service_name]], sharey = ax)
                                            #plt.Subplot(fig, inner[services_order[service_name]], sharey = ax)
                                            ax.hist(service_buffer_waiting_times, bins = np.arange(min(service_buffer_waiting_times), max(service_buffer_waiting_times) + binwidth, binwidth))#, width = bins_size_ms)
                                            ax.title.set_text(f'{service_name[:plotting_constants.VARIABLE_NAMES_SIZE_LIMIT]}...')

                                            ax.set_xlim(0, int(global_max_waiting_time + 50))
                                            major_ticks_interval_raw = int(ax.get_xlim()[1] / (plotting_constants.SQUARE_PLOT_SIDE_INCH * plotting_constants.MAJOR_TICKS_PER_INCH))
                                            major_ticks_interval = round(major_ticks_interval_raw, -max(len(str(major_ticks_interval_raw)) - 2, 0))
                                            minor_ticks_interval = major_ticks_interval // plotting_constants.MINOR_TICKS_PER_MAJOR_TICK_INTERVAL
                                            ax.xaxis.set_major_locator(ticker.MultipleLocator(major_ticks_interval))
                                            ax.xaxis.set_major_formatter(ticker.StrMethodFormatter("{x:.0f}"))
                                            ax.xaxis.set_minor_locator(ticker.MultipleLocator(minor_ticks_interval))
                                            ax.tick_params('x', labelrotation = 90)

                                            ax.yaxis.set_major_locator(ticker.MaxNLocator(plotting_constants.MAJOR_TICKS_PER_INCH * plotting_constants.SQUARE_PLOT_SIDE_INCH))
                                            if not ax.is_first_col():
                                                plt.setp(ax.get_yticklabels(), visible = False)

                                            fig.add_subplot(ax)

                                i += 1

                        if not figures_dir is None:
                            figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                            plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
                        else:
                            plt.suptitle(f'Distribution of requests by buffer waiting time in {region_name}', y = 1.05)
                            plt.show()

                        plt.close()
