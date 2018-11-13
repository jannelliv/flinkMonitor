#!/usr/bin/env python3

import itertools
import numbers
import pathlib
import re
import sys
import textwrap

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

ANY = slice(None)

def warn(message):
    sys.stderr.write("Warning: " + message + "\n")

def varying_levels(index):
    return {name for name, levels in zip(index.names, index.levels) if len(levels) > 1}

def enumerate_keys(index, names):
    # TODO: Only enumerate combinations that are actually used by the index.
    name_list = [name for name in index.names if name in names]
    dimensions = [list(levels) for name, levels in zip(index.names, index.levels) if name in names]
    return name_list, list(itertools.product(*dimensions))

def describe_key_value(name, value):
    pretty_name = name.replace('_', ' ')
    if isinstance(value, bool):
        return pretty_name if value else "no " + pretty_name
    elif isinstance(value, numbers.Number):
        return pretty_name + " " + str(value)
    else:
        return str(value)

def describe_key(names, key):
    parts = [describe_key_value(name, value) for name, value in zip(names, key)]
    return ", ".join([part for part in parts if part is not None])

def key_to_selector(index, names, key):
    return tuple([(key[names.index(name)] if name in names else ANY) for name in index.names])


class Data:
    subplot_font = {'fontsize': 11}

    def __init__(self, name, df):
        self.name = name
        self.df = df

    def select(self, experiment=ANY, tool=ANY, adaptivity=ANY, processors=ANY, formula=ANY, window=ANY, acceleration=ANY, repetition=ANY, statistics=ANY):
        view = self.df.loc[(experiment, tool, adaptivity, processors, formula, window, acceleration, repetition, statistics), :]
        return Data(self.name, view)

    def export(self, *columns, drop_levels=[], path=None):
        index = self.df.index.remove_unused_levels()
        key_levels = varying_levels(index) - set(drop_levels)
        unused_levels = set(index.names) - key_levels

        columns = self.df.columns.intersection(columns)
        result = self.df.loc[:, columns].copy()
        result.reset_index(list(unused_levels), drop=True, inplace=True)
        result.reset_index(inplace=True)

        if path is not None:
            result.to_csv(path, index=False)
        return result

    def plot(self, x_level, y_columns, series_levels=[], column_levels=[], box_plot=None, style='-o', title=None, path=None):
        if box_plot:
            df = self.df
        else:
            df = self.df.reset_index(level=x_level)

        y_columns = [y_columns] if isinstance(y_columns, str) else y_columns
        series_levels = set(series_levels)
        column_levels = set(column_levels)

        index = df.index.remove_unused_levels()
        levels = varying_levels(index)
        extra_levels = levels - series_levels - {x_level}
        if box_plot:
            extra_levels -= {box_plot}

        row_levels = extra_levels - column_levels
        column_levels = extra_levels & column_levels
        series_levels = levels & set(series_levels)

        row_level_list, row_keys = enumerate_keys(index, row_levels)
        column_level_list, column_keys = enumerate_keys(index, column_levels)
        series_level_list, series_keys = enumerate_keys(index, series_levels)
        plot_key_names = row_level_list + column_level_list
        series_key_names = plot_key_names + series_level_list

        if box_plot:
            series_key_names += [x_level]
            x_keys = list(index.levels[index.names.index(x_level)])

        nrows = len(row_keys)
        ncols = len(column_keys) * len(y_columns)
        figsize = (4.0 * ncols, 3.0 * nrows)
        fig, axes = plt.subplots(nrows, ncols, sharex='row', sharey='row', squeeze=False, figsize=figsize)
        if title is not None:
            fig.suptitle(title, y = 1 - 0.3 / figsize[1])

        lines = []
        for row, row_key in enumerate(row_keys):
            for col1, column_key in enumerate(column_keys):
                for col2, y_column in enumerate(y_columns):
                    col = col1 * len(y_columns) + col2
                    plot_key = row_key + column_key

                    ax = axes[row, col]
                    plot_title = describe_key(plot_key_names, plot_key)
                    ax.set_title('\n'.join(textwrap.wrap(plot_title, 40)), fontdict=self.subplot_font)
                    ax.set_xlabel(x_level)
                    ax.set_ylabel(y_column)

                    lines = []
                    for series in series_keys:
                        if box_plot:
                            samples = [df.loc[key_to_selector(index, series_key_names, plot_key + series + (x,)), y_column].values for x in x_keys]
                            ax.boxplot(samples, labels=x_keys)
                        else:
                            selector = key_to_selector(index, series_key_names, plot_key + series)
                            X = df.loc[selector, x_level]
                            Y = df.loc[selector, y_column]
                            lines += ax.plot(X, Y, style)

        if not box_plot:
            fig.legend(lines, map(lambda k: describe_key(series_level_list, k), series_keys), 'upper right')
        fig.tight_layout(pad=0.5, h_pad=1, w_pad=0.5, rect=[0, 0, 1 - 1 / figsize[0], 1 - 0.8 / figsize[1]])
        if path is not None:
            fig.savefig(path)
        return fig


class Loader:
    job_levels = ['experiment', 'tool', 'adaptivity', 'processors', 'formula', 'window', 'acceleration', 'repetition', 'statistics']

    job_regex = r"(nokia|synth)_(monpoly|flink)(_ft)?(?:_(\d+))?_((?:del|ins)[-_]\d[-_]\d|[a-zA-Z0-9]+)(?:_(\d+))?(?:_(\d+))_(\d+)(?:_(predictive|reactive|static))?"
    #synth_flink_ft_2_triangle_2_predictive_8_3_job.txt
    #job_regex = r"(nokia|synth)_(monpoly|flink)(_ft)?(?:_(\d+))?_((?:del|ins)[-_]\d[-_]\d|[a-zA-Z0-9]+)(?:_(\d+))?(?:_(predictive|reactive|static))?(?:_(\d+))_(\d+)"
    metrics_pattern = re.compile(r"metrics_" + job_regex + r"\.csv")
    delay_pattern = re.compile(job_regex + r"_delay\.txt")
    time_pattern = re.compile(job_regex + r"_time(?:_(\d+))?\.txt")
    job_pattern = re.compile(job_regex + r"_job\.txt")

    delay_header = ['timestamp', 'current_indices', 'current_events', 'current_latency', 'peak', 'max', 'average']

    summary_keys = []
    summary_data = []

    series_keys = []
    series_data = []

    memory_keys = []
    memory_data = []

    runtime_keys = []
    runtime_data = []

    throughput_keys = []
    throughput_data_tmp = []
    throughput_data = []

    runtime_map = {}


    def warn_skipped_path(self, path):
        #print("Skipped " + str(path))
        pass

    def warn_invalid_file(self, path):
        warn("Invalid data in file " + str(path))

    def read_runtime(self, key, df):
        #job_regex = r"Job Runtime: (?:(\d+)) ms"
        #runtime_pattern = re.compile(job_regex)
        list = df.index.values
        first = min(list)
        last = max(list)

        runtime = last-first
        data = pd.DataFrame([runtime], columns=['runtime'])
        self.runtime_map[key] = runtime
        self.runtime_keys.append(key)
        self.runtime_data.append(data)

    def read_throughput(self, key, df):
        #job_regex = r"Job Runtime: (?:(\d+)) ms"
        #runtime_pattern = re.compile(job_regex)
        list = df['current_events'].tolist()
        # TODO: length of this list isn't actual running time
        sum_tp = sum(list)
        del list[-1]
        max_tp = max(list)

        self.throughput_keys.append(key)
        self.throughput_data_tmp.append((sum_tp, max_tp))
        self.throughput_data.append("")

    def read_metrics(self, key, path):
        try:
            df = pd.read_csv(path, header=0, index_col=0)
        except Exception as e:
            raise Exception("Error while reading file " + str(path)) from e

        if df.shape[0] > 0 and df.index.name == 'timestamp' and set(df.columns) >= {'peak', 'max', 'average', 'sum_tp'}:
            self.read_runtime(key, df)
            df.loc[:, 'peak'].replace(to_replace=0, inplace=True, method='ffill')

            summary = df.tail(1)
            self.summary_keys.append(key)
            self.summary_data.append(summary)

            start_timestamp = df.iloc[0].name - 1
            series = df.rename(index = lambda timestamp: timestamp - start_timestamp)
            self.series_keys.append(key)
            self.series_data.append(series)
        else:
            self.warn_invalid_file(path)

    def read_replayer_delay(self, key, path):
        try:
            with open(str(path), 'r') as f:
                first_line = f.readline()
                skip_rows = 1 if first_line and first_line.startswith("Client connected:") else 0
            df = pd.read_csv(path, sep='\\s+', header=None, names=self.delay_header, index_col=0, skiprows=skip_rows)
        except Exception as e:
            raise Exception("Error while reading file " + str(path)) from e

        if df.shape[0] > 0:
            df.loc[:, df.columns.intersection(['current_latency', 'peak', 'max', 'average'])] *= 1000.0
            df.loc[:, 'peak'].replace(to_replace=0, inplace=True, method='ffill')

            summary = df.loc[:, ['current_events', 'peak', 'max', 'average']].tail(1)
            self.summary_keys.append(key)
            self.summary_data.append(summary)

            series = df.copy()
            self.series_keys.append(key)
            self.series_data.append(series)
        else:
            self.warn_invalid_file(path)

    def read_replayer_events(self, key, path):
        try:
            with open(str(path), 'r') as f:
                first_line = f.readline()
                skip_rows = 1 if first_line and first_line.startswith("Client connected:") else 0
            df = pd.read_csv(path, sep='\\s+', header=None, names=self.delay_header, index_col=0, skiprows=skip_rows)
        except Exception as e:
            raise Exception("Error while reading file " + str(path)) from e

        if df.shape[0] > 0:
            self.read_throughput(key, df)
            #df.loc[:, ['current_events']].tail(1)
            #series = df.copy()
            #self.series_keys.append(key)
            #self.series_data.append(series)
        else:
            self.warn_invalid_file(path)

    def read_memory(self, key, monitor_index, path):
        try:
            with open(str(path), 'r') as f:
                memory_usages = []
                lines = f.readlines()
                for line in lines:
                    if not line or ';' not in line:
                        self.warn_invalid_file(path)
                        return
                    memory_usages += [np.int32(line.split(';', 1)[1])]

                memory_usage = max(memory_usages + [0])
        except Exception as e:
            raise Exception("Error while reading file " + str(path)) from e

        memory = pd.DataFrame([memory_usage], index=pd.Index([monitor_index], name='monitor'), columns=['memory'])
        self.memory_keys.append(key)
        self.memory_data.append(memory)

    def read_file(self, path):
        metrics_match = self.metrics_pattern.fullmatch(path.name)
        if metrics_match:
            key = (
                metrics_match.group(1),
                metrics_match.group(2),
                bool(metrics_match.group(3)),
                int(metrics_match.group(4) or 0),
                metrics_match.group(5).replace('_', '-'),
                int(metrics_match.group(6) or 0),
                int(metrics_match.group(7)),
                int(metrics_match.group(8)),
                (metrics_match.group(9) or "none")
            )
            self.read_metrics(key, path)
            return

        delay_match = self.delay_pattern.fullmatch(path.name)
        if delay_match:
            if delay_match.group(2) != 'monpoly':
                # NOTE: We silently ignore replayer data for non-Monpoly experiments.
                key = (
                    delay_match.group(1),
                    delay_match.group(2),
                    bool(delay_match.group(3)),
                    int(delay_match.group(4) or 0),
                    delay_match.group(5).replace('_', '-'),
                    int(delay_match.group(6) or 0),
                    int(delay_match.group(7)),
                    int(delay_match.group(8)),
                    (delay_match.group(9) or "none")
                )
                self.read_replayer_events(key, path)
                return
            key = (
                delay_match.group(1),
                delay_match.group(2),
                bool(delay_match.group(3)),
                int(delay_match.group(4) or 0),
                delay_match.group(5).replace('_', '-'),
                int(delay_match.group(6) or 0),
                int(delay_match.group(7)),
                int(delay_match.group(8)),
                (delay_match.group(9) or "none")
            )
            self.read_replayer_delay(key, path)
            return

        #job_match = self.job_pattern.fullmatch(path.name)
        #if job_match:
        #    key = (
        #        job_match.group(1),
        #        job_match.group(2),
        #        bool(job_match.group(3)),
        #        int(job_match.group(4) or 0),
        #        job_match.group(5).replace('_', '-'),
        #        int(job_match.group(6) or 0),
        #        int(job_match.group(7)),
        #        int(job_match.group(8)),
        #        (job_match.group(9) or "none")
        #    )
        #    self.read_runtime(key, path)
        #    return

        time_match = self.time_pattern.fullmatch(path.name)
        if time_match:
            key = (
                time_match.group(1),
                time_match.group(2),
                bool(time_match.group(3)),
                int(time_match.group(4) or 0),
                time_match.group(5).replace('_', '-'),
                int(time_match.group(6) or 0),
                int(time_match.group(7)),
                int(time_match.group(8) or 0),
                (time_match.group(9) or "none")
            )
            monitor_index = int(time_match.group(10) or 0)
            self.read_memory(key, monitor_index, path)
            return

        self.warn_skipped_path(path)

    def read_files(self, path):
        if path.is_file():
            self.read_file(path)
        elif path.is_dir():
            for entry in path.iterdir():
                if entry.is_file():
                    self.read_file(entry)
        else:
            self.warn_skipped_path(path)

    def max_memory(self, df):
        group_levels = list(df.index.names)
        group_levels.remove('monitor')
        return df.groupby(level=group_levels).max()

    def avg_throughput(self):
        for i in range(0, len(self.throughput_keys)):
            sum_tp, avg_tp = self.throughput_data_tmp[i]
            key = self.throughput_keys[i]
            runtime = self.runtime_map[key]
            avg_tp = sum_tp / runtime
            data = pd.DataFrame([[avg_tp, sum_tp, avg_tp]], columns=['avg_tp', 'sum_tp', 'max_tp'])
            self.throughput_data[i] = data

    def average_repetitions(self, df):
        group_levels = list(df.index.names)
        group_levels.remove('repetition')
        return df.groupby(level=group_levels).mean()

    def average_repetitions_sum(self, df):
        group_levels = list(df.index.names)
        group_levels.remove(None)
        group_levels.remove('repetition')
        return df.groupby(level=group_levels).mean()

    def process(self):
        raw_memory = pd.concat(self.memory_data, keys=self.memory_keys, names=self.job_levels)
        memory = self.max_memory(raw_memory)

        raw_summary = pd.concat(self.summary_data, sort=True, keys=self.summary_keys, names=self.job_levels)
        raw_summary.reset_index('timestamp', drop=True, inplace=True)
        raw_summary = raw_summary.merge(memory, 'outer', left_index=True, right_index=True)

        summary = self.average_repetitions(raw_summary)
        summary.sort_index(inplace=True)

        raw_runtime = pd.concat(self.runtime_data, keys=self.runtime_keys, names=self.job_levels)
        runtime = self.average_repetitions_sum(raw_runtime)
        runtime.sort_index(inplace=True)

        self.avg_throughput()
        raw_throughput = pd.concat(self.throughput_data, keys=self.throughput_keys, names=self.job_levels)
        throughput = self.average_repetitions_sum(raw_throughput)
        throughput.sort_index(inplace=True)

        monitor_columns = [column for column in list(raw_summary.columns) if column.startswith('monitor')]
        raw_slices = raw_summary.loc[:, monitor_columns].stack()
        raw_slices.index.rename('monitor', level=-1, inplace=True)
        raw_slices.name = 'total_events'
        slices = self.average_repetitions(raw_slices).to_frame()
        series = pd.concat(self.series_data, sort=True, keys=self.series_keys, names=self.job_levels)
        #series = raw_series[raw_series['peak'] > 0]
        series.sort_index(inplace=True)

        return Data("Summary", summary), Data("Slices", slices), Data("Time series", series), Data("Runtime", runtime), Data("Throughput", throughput)

    @classmethod
    def load(cls, paths):
        loader = cls()
        for path in paths:
            loader.read_files(path)
        return loader.process()


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        paths = map(pathlib.Path, sys.argv[1:])
        summary, slices, series, runtime, throughput = Loader.load(paths)

        # SYNTHETIC
        # gen_nproc = summary.select(experiment='gen', statistics=False, index_rate=1000)
        # gen_nproc.plot('event_rate', ['peak', 'max', 'average'], series_levels=['tool', 'processors'], title="Latency (synthetic, 1000)" , path="gen_nproc.pdf")

        # gen_memory = summary.select(experiment='gen', checkpointing=False, statistics=False, index_rate=1000)
        # gen_memory.plot('event_rate', 'memory', series_levels=['tool', 'processors'], column_levels=['formula'], title="Max. monitor memory (synthetic, 1000)" , path="gen_memory.pdf")

        # gen_formulas = summary.select(experiment='gen', tool='flink', checkpointing=True, statistics=False)
        # gen_formulas.plot('event_rate', ['peak', 'max', 'average'], series_levels=['formula', 'index_rate'], title="Latency (synthetic)", path="gen_formulas.pdf")

        # # NOKIA
        # nokia_nproc = summary.select(experiment='nokia', statistics=False)
        # nokia_nproc.plot('event_rate', ['peak', 'max', 'average'], series_levels=['tool', 'processors'], title="Latency (Nokia)" , path="nokia_nproc.pdf")

        # nokia_formulas = summary.select(experiment='nokia', tool='flink', checkpointing=True, statistics=False)
        # nokia_formulas.plot('event_rate', ['peak', 'max', 'average'], series_levels=['formula'], title="Latency (Nokia)", path="nokia_formulas.pdf")

        # nokia_series = series.select(experiment='nokia', statistics=False)
        # nokia_series.plot('timestamp', 'peak', series_levels=['tool', 'processors'], column_levels=['checkpointing', 'repetition'], style='-', title="Latency (Nokia)", path="nokia_series.pdf")

        # # SLICES
        # gen_slices = slices.select(experiment='gen', tool='flink', checkpointing=False, index_rate=1000)
        # gen_slices.plot('processors', 'total_events', column_levels=['formula'], box_plot='monitor', title="Slice sizes (synthetic)", path="gen_slices.pdf")

        # genh_slices = slices.select(experiment='genh', tool='flink', checkpointing=True, event_rate=4000, index_rate=1000)
        # genh_slices.plot('processors', 'total_events', column_levels=['statistics', 'heavy_hitters'], box_plot='monitor', title="Slice sizes (synthetic w/ skew)", path="genh3_slices.pdf")


        # gen_nproc_export = summary.select(experiment='gen', checkpointing=True, statistics=False, formula='star', index_rate=1000)
        # gen_nproc_export.export('max', 'memory', path="gen_nproc.csv")

        # # ALL
        # gen_nproc_export = summary.select()
        # gen_nproc_export = summary.select()
        # gen_nproc_export.export('max', 'peak', 'average', 'memory', path="all.csv")

        # NOKIA EXPERIMENTS
        #synth_summary = summary.select(experiment='nokia')
        #synth_summary.export('max', 'peak', 'average', 'current_events', 'memory', path="plot-nokia.csv")
#
        #synth_plot_tp = summary.select(experiment='nokia', adaptivity=False)
        #synth_plot_tp.export('processors', 'sum_tp', 'acceleration', path="plots_nokia-tp_nonad.csv")
        #synth_plot_tp.plot('processors', 'sum_tp', column_levels=['acceleration'], title="Plot throughput Nonad", path="nokia-plots-tp-nonad.pdf")
#
        #synth_plot_tp = summary.select(experiment='nokia', adaptivity=True)
        #synth_plot_tp.export('processors', 'statistics', 'sum_tp', 'acceleration', path="plots_tp_ad.csv")
        #synth_plot_tp.plot('processors', 'sum_tp', series_levels=['statistics'], column_levels=['acceleration'], title="Plot throughput Ad", path="nokia-plots-tp-ad.pdf")
#
        ## PLOT3
        #synth_series = series.select(experiment='nokia', adaptivity=False)
        #synth_series.export('peak', path="plot-nokia-time-f.csv")
        #synth_series.plot('timestamp', 'peak', series_levels=['statistics'], column_levels=['processors'],  title="Latency-Nonadaptive", path="nokia-plots-peak-latency-nonad.pdf")
#
        #synth_series = series.select(experiment='nokia', adaptivity=True)
        #synth_series.export('peak', path="plot-nokia-time-t.csv")
        #synth_series.plot('timestamp', 'peak', series_levels=['statistics'], column_levels=['processors'],  title="Latency Adaptive", path="nokia-plots-peak-latency-ad.pdf")

        # PLOT2
        #nokia_nproc = summary.select(experiment='nokia')
        #nokia_nproc.export('max', 'peak', 'average', 'current_events', 'memory', 'sum_tp', path="plot-nokia.csv")

        # PLOT3
        #nokia_series = series.select(experiment='nokia', adaptivity=False, repetition=1)
        #nokia_series.export('peak', path="plot-nokia-time-f.csv")

        # PLOT4
        #nokia_series = series.select(experiment='nokia', adaptivity=True, repetition=1)
        #nokia_series.export('peak', path="plot-nokia-time-t.csv")


        # NOKIA EXPERIMENTS
        #nokia_plot_summary = summary.select(experiment='nokia')
        #nokia_plot_summary.export('max', 'peak', 'average', 'current_events', 'memory', path="plot-summary.csv")

        #nokia_plot_tp_proc_ad = summary.select(experiment='nokia', adaptivity=False)
        #nokia_plot_tp_proc_ad.export('processors', 'current_events', 'acceleration', path="plot-tp-proc-nonad.csv")
        #nokia_plot_tp_proc_ad.plot('processors', 'current_events', column_levels=['acceleration'], title="Plot throughput Nonad", path="plot-tp-proc-nonad.pdf")

        #nokia_plot_tp_proc_nonad = summary.select(experiment='nokia', adaptivity=True)
        #nokia_plot_tp_proc_nonad.export('processors', 'statistics', 'current_events', 'acceleration', path="plot-tp-proc-ad.csv")
        #nokia_plot_tp_proc_nonad.plot('processors', 'current_events', series_levels=['statistics'], column_levels=['acceleration'], title="Plot throughput Ad", path="plot-tp-proc-ad.pdf")

        #nokia_plot_tp_trace_nonad = series.select(experiment='nokia', adaptivity=False)
        #nokia_plot_tp_trace_nonad.export('current_events', path="plot-tp-trace-nonad.csv")
        #nokia_plot_tp_trace_nonad.plot('timestamp', 'current_events', column_levels=['processors', 'acceleration'], title="TP NonAdaptive", path="plot-tp-trace-nonad.pdf")

        #nokia_plot_tp_trace_ad = series.select(experiment='nokia', adaptivity=True)
        #nokia_plot_tp_trace_ad.export('current_ events', path="plot-tp_trace-ad.csv")
        #nokia_plot_tp_trace_ad.plot('timestamp', 'current_events', column_levels=['processors', 'acceleration'],  title="Tp-Adaptive", path="plot-tp-trace-ad.pdf")

        #nokia_plot_peak_lat_nonad = series.select(experiment='nokia', adaptivity=False)
        #nokia_plot_peak_lat_nonad.export('peak', path="plot-peak-lat-nonad.csv")
        #nokia_plot_peak_lat_nonad.plot('timestamp', 'peak', series_levels=['statistics'], column_levels=['processors'],  title="Latency-Nonadaptive", path="plot-peak-lat-nonad.pdf")

        #nokia_plot_peak_lat_ad = series.select(experiment='nokia', adaptivity=True)
        #nokia_plot_peak_lat_ad.export('peak', path="plot-peak-lat-ad.csv")
        #nokia_plot_peak_lat_ad.plot('timestamp', 'peak', series_levels=['statistics'], column_levels=['processors'],  title="Latency Adaptive", path="plot-peak-lat-ad.pdf")

        #nokia_plot_tp_comparative= series.select(experiment='nokia', tool='flink')
        #nokia_plot_tp_comparative.plot('timestamp', 'average', series_levels=['statistics'], column_levels=['processors'],  title="Latency Comparative", path="plot-avg-lat-comparative.pdf")
        #nokia_plot_tp_comparative.plot('timestamp', 'current_events', series_levels=['statistics'], column_levels=['processors'],  title="Throughput Comparative", path="plot-tp-comparative.pdf")

        #nokia_plot_runtime_comparative = runtime.select(experiment='nokia', tool='flink', adaptivity=True)
        #nokia_plot_runtime_comparative.plot('processors', 'runtime', series_levels=['statistics'], column_levels=['windows'],  title="Runtime Comparative", path="plot-runtime-comparative.pdf")

        plots = "synth"

        if plots == "nokia":
            acceleration=3000
            nokia_summary = summary.select(experiment='nokia', acceleration=acceleration)
            nokia_summary.export('max', 'peak', 'average', 'acceleration', 'memory', path="plot-summary.csv")
            nokia_summary_ad= summary.select(experiment='nokia', adaptivity=True, acceleration=acceleration)
            nokia_summary_ad.plot('window', 'peak', series_levels=['statistics'], column_levels=['processors'],  title="Latency-Nonadaptive", path="plot-peak-lat-ad.pdf")
            nokia_summary_nonad= summary.select(experiment='nokia', adaptivity=False)
            nokia_summary_nonad.plot('processors', 'peak', title="Latency-Nonadaptive", path="plot-peak-lat-nonad.pdf")

            #nokia_plot_tp_proc_ad = summary.select(experiment='nokia', adaptivity=False)
            #nokia_plot_tp_proc_ad.export('processors', 'current_events', 'acceleration', path="plot-tp-proc-nonad.csv")
            #nokia_plot_tp_proc_ad.plot('processors', 'current_events', series_levels=['acceleration'], title="Plot throughput Nonad", path="plot-tp-proc-nonad.pdf")

            #nokia_plot_tp_proc_nonad = summary.select(experiment='nokia', adaptivity=True)
            #nokia_plot_tp_proc_nonad.export('processors', 'statistics', 'current_events', 'acceleration', path="plot-tp-proc-ad.csv")
            #nokia_plot_tp_proc_nonad.plot('processors', 'current_events', series_levels=['statistics'], column_levels=['acceleration'], title="Plot throughput Ad", path="plot-tp-proc-ad.pdf")

            nokia_plot_peak_lat_nonad = series.select(experiment='nokia', adaptivity=False, repetition=1, acceleration=acceleration)
            nokia_plot_peak_lat_nonad.export('peak', path="plot-peak-lat-trace-nonad.csv")
            nokia_plot_peak_lat_nonad.plot('timestamp', 'peak', column_levels=['processors'],  title="Latency-Nonadaptive", path="plot-peak-lat-trace-nonad.pdf")

            nokia_plot_peak_lat_ad = series.select(experiment='nokia', adaptivity=True, repetition=1, acceleration=acceleration)
            nokia_plot_peak_lat_ad.export('peak', path="plot-peak-lat-trace-ad.csv")
            nokia_plot_peak_lat_ad.plot('timestamp', 'peak', series_levels=['statistics'], column_levels=['processors', 'windows'],  title="Latency Adaptive", path="plot-peak-lat-trace-ad.pdf")

            #nokia_plot_tp_comparative= series.select(experiment='nokia', tool='flink')
            #nokia_plot_tp_comparative.plot('timestamp', 'average', series_levels=['statistics'], column_levels=['windows', 'processors'],  title="Latency Comparative", path="plot-avg-lat-comparative.pdf")
            #nokia_plot_tp_comparative.plot('timestamp', 'current_events', series_levels=['statistics'], column_levels=['windows', 'processors'],  title="Throughput Comparative", path="plot-tp-comparative.pdf")

            nokia_plot_tp_ad = throughput.select(experiment='nokia', tool='flink', adaptivity=True, acceleration=acceleration)
            #nokia_plot_tp_ad.export('max_tp', path="plot-tp-ad.csv")
            #nokia_plot_tp_ad.plot('window', 'max_tp', series_levels=['statistics'], column_levels=['processors'],  title="Runtime Adaptive", path="plot-tp-ad.pdf")
            nokia_plot_tp_ad.export('avg_tp', path="plot-tp-avg-ad.csv")
            nokia_plot_tp_ad.plot('window', 'avg_tp', series_levels=['statistics'], column_levels=['processors'],  title="Runtime Adaptive", path="plot-tp-avg-ad.pdf")

            nokia_plot_tp_nonad = throughput.select(experiment='nokia', adaptivity=False, acceleration=acceleration)
            #nokia_plot_tp_nonad.export('max_tp', path="plot-tp-max-nonad.csv")
            #nokia_plot_tp_nonad.plot('window', 'max_tp', series_levels=['statistics'], column_levels=['processors'],  title="Max Throughput Non-Adaptive", path="plot-tp-max-nonad.pdf")
            nokia_plot_tp_nonad.export('avg_tp', path="plot-tp-avg-nonad.csv")
            nokia_plot_tp_nonad.plot('window', 'avg_tp', series_levels=['statistics'], column_levels=['processors'],  title="Avg Throughput Non-Adaptive", path="plot-tp-avg-nonad.pdf")

            nokia_plot_tp = throughput.select(experiment='nokia', tool='flink', acceleration=acceleration)
            nokia_plot_tp.export('avg_tp', path="plot-tp.csv")
            nokia_plot_runtime = runtime.select(experiment='nokia', tool='flink', acceleration=acceleration)
            nokia_plot_runtime.export('runtime', path="plot-runtime.csv")

            nokia_plot_runtime_ad = runtime.select(experiment='nokia', tool='flink', adaptivity=True, acceleration=acceleration)
            nokia_plot_runtime_ad.export('runtime', path="plot-runtime-ad.csv")
            nokia_plot_runtime_ad.plot('window', 'runtime', series_levels=['statistics'], column_levels=['processors'],  title="Runtime Adaptive", path="plot-runtime-ad.pdf")
            nokia_plot_runtime_nonad = runtime.select(experiment='nokia', tool='flink', adaptivity=False, acceleration=acceleration)
            nokia_plot_runtime_nonad.export('runtime', path="plot-runtime-non-ad.csv")
            nokia_plot_runtime_nonad.plot('processors', 'runtime', series_levels=['acceleration'], title="Runtime Non Adaptive", path="plot-runtime-nonad.pdf")

        elif plots == "synth":
            #SYNTHETIC EXPERIMENTS
            synth_summary = summary.select(experiment='synth')
            synth_summary.export('max', 'peak', 'average', 'acceleration', 'memory', path="plot-summary.csv")
            synth_summary_ad= summary.select(experiment='synth', adaptivity=True)
            synth_summary_ad.plot('window', 'peak', series_levels=['statistics'], column_levels=['processors'],  title="Latency-Nonadaptive", path="plot-peak-lat-ad.pdf")
            synth_summary_nonad= summary.select(experiment='synth', adaptivity=False)
            synth_summary_nonad.plot('processors', 'peak', title="Latency-Nonadaptive", path="plot-peak-lat-nonad.pdf")


            synth_plot_tp_proc_ad = summary.select(experiment='synth', adaptivity=False)
            synth_plot_tp_proc_ad.export('processors', 'current_events', 'acceleration', path="plot-tp-proc-nonad.csv")
            synth_plot_tp_proc_ad.plot('processors', 'current_events', series_levels=['acceleration'], title="Plot throughput Nonad", path="plot-tp-proc-nonad.pdf")

            synth_plot_tp_proc_nonad = summary.select(experiment='synth', adaptivity=True)
            synth_plot_tp_proc_nonad.export('processors', 'statistics', 'current_events', 'acceleration', path="plot-tp-proc-ad.csv")
            synth_plot_tp_proc_nonad.plot('processors', 'current_events', series_levels=['statistics'], column_levels=['acceleration'], title="Plot throughput Ad", path="plot-tp-proc-ad.pdf")

            #synth_plot_tp_trace_nonad = series.select(experiment='synth', adaptivity=False)
            #synth_plot_tp_trace_nonad.export('current_events', path="plot-tp-trace-nonad.csv")
            #synth_plot_tp_trace_nonad.plot('timestamp', 'current_events', series_levels=['acceleration'], column_levels=['processors'], title="TP NonAdaptive", path="plot-tp-trace-nonad.pdf")

            #synth_plot_tp_trace_ad = series.select(experiment='synth', adaptivity=True)
            #synth_plot_tp_trace_ad.export('current_ events', path="plot-tp_trace-ad.csv")
            #synth_plot_tp_trace_ad.plot('timestamp', 'current_events', series_levels=['statistics'], column_levels=['processors', 'acceleration'],  title="Tp-Adaptive", path="plot-tp-trace-ad.pdf")

            synth_plot_peak_lat_nonad = series.select(experiment='synth', adaptivity=False)
            synth_plot_peak_lat_nonad.export('peak', path="plot-peak-lat-trace-nonad.csv")
            synth_plot_peak_lat_nonad.plot('timestamp', 'peak', column_levels=['processors'],  title="Latency-Nonadaptive", path="plot-peak-lat-trace-nonad.pdf")

            synth_plot_peak_lat_ad = series.select(experiment='synth', adaptivity=True)
            synth_plot_peak_lat_ad.export('peak', path="plot-peak-lat-trace-ad.csv")
            synth_plot_peak_lat_ad.plot('timestamp', 'peak', series_levels=['statistics'], column_levels=['processors', 'windows'],  title="Latency Adaptive", path="plot-peak-lat-trace-ad.pdf")

            #synth_plot_peak_lat_ad = series.select(experiment='synth', adaptivity=True, window=8, processors=8)
            #synth_plot_peak_lat_ad.export('peak', path="plot-peak-lat-trace-ad-8-8.csv")
            #synth_plot_peak_lat_ad.plot('timestamp', 'peak', series_levels=['statistics'], column_levels=['processors', 'windows'],  title="Latency Adaptive", path="plot-peak-lat-trace-8-8-ad.pdf")

            synth_plot_tp_comparative= series.select(experiment='synth', tool='flink')
            synth_plot_tp_comparative.plot('timestamp', 'peak', series_levels=['statistics'], column_levels=['windows', 'processors'],  title="Latency Comparative", path="plot-peak-lat-comparative.pdf")

            synth_plot_latency= series.select(experiment='synth', tool='flink', repetition=1, window=4, acceleration=8)
            synth_plot_latency.export('peak', path="plot-latency-trace.csv")
            synth_plot_latency.plot('timestamp', 'peak', series_levels=['statistics'], column_levels=['processors'],  title="Latency Comparative", path="plot-peak-lat-comparative-w=4-a=8.pdf")

            synth_plot_tp_ad = throughput.select(experiment='synth', tool='flink', adaptivity=True)
            synth_plot_tp_ad.export('max_tp', path="plot-tp-ad.csv")
            synth_plot_tp_ad.plot('window', 'max_tp', series_levels=['statistics'], column_levels=['processors'],  title="Runtime Adaptive", path="plot-tp-ad.pdf")
            synth_plot_tp_ad.export('avg_tp', path="plot-tp-avg-ad.csv")
            synth_plot_tp_ad.plot('window', 'avg_tp', series_levels=['statistics'], column_levels=['processors'],  title="Runtime Adaptive", path="plot-tp-avg-ad.pdf")

            synth_plot_tp_nonad = throughput.select(experiment='synth', adaptivity=False)
            synth_plot_tp_nonad.export('max_tp', path="plot-tp-max-nonad.csv")
            synth_plot_tp_nonad.plot('window', 'max_tp', series_levels=['statistics'], column_levels=['processors'],  title="Max Throughput Non-Adaptive", path="plot-tp-max-nonad.pdf")
            synth_plot_tp_nonad.export('avg_tp', path="plot-tp-avg-nonad.csv")
            synth_plot_tp_nonad.plot('window', 'avg_tp', series_levels=['statistics'], column_levels=['processors'],  title="Avg Throughput Non-Adaptive", path="plot-tp-avg-nonad.pdf")

            synth_plot_tp = throughput.select(experiment='synth', tool='flink')
            synth_plot_tp.export('avg_tp', path="plot-tp.csv")

            synth_plot_runtime_ad = runtime.select(experiment='synth', tool='flink')
            synth_plot_runtime_ad.export('runtime', path="plot-runtime-ad.csv")
            synth_plot_runtime_ad.plot('window', 'runtime', series_levels=['statistics'], column_levels=['processors'],  title="Runtime Adaptive", path="plot-runtime-ad.pdf")
            synth_plot_runtime_nonad = runtime.select(experiment='synth', tool='flink', adaptivity=False)
            synth_plot_runtime_nonad.export('runtime', path="plot-runtime-non-ad.csv")
            synth_plot_runtime_nonad.plot('processors', 'runtime', series_levels=['acceleration'], title="Runtime Non Adaptive", path="plot-runtime-nonad.pdf")
    else:
        sys.stderr.write("Usage: {} path ...\n".format(sys.argv[0]))
        sys.exit(1)