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

    def select(self, experiment=ANY, tool=ANY, checkpointing=ANY, statistics=ANY, processors=ANY, formula=ANY, heavy_hitters=ANY, event_rate=ANY, index_rate=ANY, repetition=ANY):
        view = self.df.loc[(experiment, tool, checkpointing, statistics, processors, formula, heavy_hitters, event_rate, index_rate, repetition), :]
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
    job_levels = ['experiment', 'tool', 'checkpointing', 'statistics', 'processors', 'formula', 'heavy_hitters', 'event_rate', 'index_rate', 'repetition']

    job_regex = r"(nokia|nokia2|gen|genh3)_(monpoly|flink)(_ft)?(_stats)?(?:_(\d+))?_((?:del|ins)[-_]\d[-_]\d|[a-zA-Z0-9]+)(?:_h(\d+))?_(\d+)(?:_(\d+))?_(\d+)"
    metrics_pattern = re.compile(r"metrics_" + job_regex + r"\.csv")
    delay_pattern = re.compile(job_regex + r"_delay\.txt")
    time_pattern = re.compile(job_regex + r"_time(?:_(\d+))?\.txt")

    delay_header = ['timestamp', 'current_indices', 'current_events', 'current_latency', 'peak', 'max', 'average']

    summary_keys = []
    summary_data = []

    series_keys = []
    series_data = []

    memory_keys = []
    memory_data = []

    def warn_skipped_path(self, path):
        #warn("Skipped " + str(path))
        pass

    def warn_invalid_file(self, path):
        warn("Invalid data in file " + str(path))

    def read_metrics(self, key, path):
        try:
            df = pd.read_csv(path, header=0, index_col=0)
        except Exception as e:
            raise Exception("Error while reading file " + str(path)) from e

        if df.shape[0] > 0 and df.index.name == 'timestamp' and set(df.columns) >= {'peak', 'max', 'average'}:
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
            with open(path, 'r') as f:
                first_line = f.readline()
                skip_rows = 1 if first_line and first_line.startswith("Client connected:") else 0
            df = pd.read_csv(path, sep='\\s+', header=None, names=self.delay_header, index_col=0, skiprows=skip_rows)
        except Exception as e:
            raise Exception("Error while reading file " + str(path)) from e

        if df.shape[0] > 0:
            df.loc[:, df.columns.intersection(['current_latency', 'peak', 'max', 'average'])] *= 1000.0
            df.loc[:, 'peak'].replace(to_replace=0, inplace=True, method='ffill')

            summary = df.loc[:, ['peak', 'max', 'average']].tail(1)
            self.summary_keys.append(key)
            self.summary_data.append(summary)

            series = df.copy()
            self.series_keys.append(key)
            self.series_data.append(series)
        else:
            self.warn_invalid_file(path)

    def read_memory(self, key, monitor_index, path):
        try:
            with open(path, 'r') as f:
                first_line = f.readline()
                if not first_line or ';' not in first_line:
                    self.warn_invalid_file(path)
                    return
                memory_usage = np.int32(first_line.split(';', 1)[1])
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
                bool(metrics_match.group(4)),
                int(metrics_match.group(5)),
                metrics_match.group(6).replace('_', '-'),
                int(metrics_match.group(7) or 0),
                int(metrics_match.group(8)),
                int(metrics_match.group(9) or 0),
                int(metrics_match.group(10))
                )
            self.read_metrics(key, path)
            return

        delay_match = self.delay_pattern.fullmatch(path.name)
        if delay_match:
            if delay_match.group(2) != 'monpoly':
                # NOTE: We silently ignore replayer data for non-Monpoly experiments.
                return
            key = (
                'nokia2' if delay_match.group(1) == 'nokia' else delay_match.group(1),
                delay_match.group(2),
                bool(delay_match.group(3)),
                bool(delay_match.group(4)),
                int(delay_match.group(5) or 1),
                delay_match.group(6),
                int(delay_match.group(7) or 0),
                int(delay_match.group(8)),
                int(delay_match.group(9) or 0),
                int(delay_match.group(10))
                )
            self.read_replayer_delay(key, path)
            return

        time_match = self.time_pattern.fullmatch(path.name)
        if time_match:
            key = (
                'nokia2' if time_match.group(1) == 'nokia' and time_match.group(2) == 'monpoly' else time_match.group(1),
                time_match.group(2),
                bool(time_match.group(3)),
                bool(time_match.group(4)),
                int(time_match.group(5) or 1),
                time_match.group(6),
                int(time_match.group(7) or 0),
                int(time_match.group(8)),
                int(time_match.group(9) or 0),
                int(time_match.group(10))
                )
            monitor_index = int(time_match.group(11) or 0)
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

    def average_repetitions(self, df):
        group_levels = list(df.index.names)
        group_levels.remove('repetition')
        return df.groupby(level=group_levels).mean()

    def process(self):
        raw_memory = pd.concat(self.memory_data, keys=self.memory_keys, names=self.job_levels)
        memory = self.max_memory(raw_memory)

        raw_summary = pd.concat(self.summary_data, keys=self.summary_keys, names=self.job_levels)
        raw_summary.reset_index('timestamp', drop=True, inplace=True)
        raw_summary = raw_summary.merge(memory, 'outer', left_index=True, right_index=True)
        summary = self.average_repetitions(raw_summary)
        summary.sort_index(inplace=True)

        monitor_columns = [column for column in list(raw_summary.columns) if column.startswith('monitor')]
        raw_slices = raw_summary.loc[:, monitor_columns].stack()
        raw_slices.index.rename('monitor', level=-1, inplace=True)
        raw_slices.name = 'total_events'
        slices = self.average_repetitions(raw_slices).to_frame()

        series = pd.concat(self.series_data, keys=self.series_keys, names=self.job_levels)
        series.sort_index(inplace=True)

        return Data("Summary", summary), Data("Slices", slices), Data("Time series", series)

    @classmethod
    def load(cls, paths):
        loader = cls()
        for path in paths:
            loader.read_files(path)
        return loader.process()


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        paths = map(pathlib.Path, sys.argv[1:])
        summary, slices, series = Loader.load(paths)

        gen_nproc = summary.select(experiment='gen', statistics=False, index_rate=1000)
        gen_nproc.plot('event_rate', ['peak', 'max', 'average'], series_levels=['tool', 'processors'], title="Latency (synthetic, 1000)" , path="gen_nproc.pdf")

        gen_memory = summary.select(experiment='gen', checkpointing=False, statistics=False, index_rate=1000)
        gen_memory.plot('event_rate', 'memory', series_levels=['tool', 'processors'], column_levels=['formula'], title="Max. monitor memory (synthetic, 1000)" , path="gen_memory.pdf")

        gen_formulas = summary.select(experiment='gen', tool='flink', checkpointing=True, statistics=False)
        gen_formulas.plot('event_rate', ['peak', 'max', 'average'], series_levels=['formula', 'index_rate'], title="Latency (synthetic)", path="gen_formulas.pdf")

        gen_slices = slices.select(experiment='gen', tool='flink', checkpointing=False, index_rate=1000)
        gen_slices.plot('processors', 'total_events', column_levels=['formula'], box_plot='monitor', title="Slice sizes (synthetic)", path="gen_slices.pdf")

        genh_slices = slices.select(experiment='genh3', tool='flink', checkpointing=True, event_rate=4000, index_rate=1000)
        genh_slices.plot('processors', 'total_events', column_levels=['statistics', 'heavy_hitters'], box_plot='monitor', title="Slice sizes (synthetic w/ skew)", path="genh3_slices.pdf")

        nokia_nproc = summary.select(experiment='nokia2', statistics=False)
        nokia_nproc.plot('event_rate', ['peak', 'max', 'average'], series_levels=['tool', 'processors'], title="Latency (Nokia)" , path="nokia_nproc.pdf")

        nokia_formulas = summary.select(experiment='nokia2', tool='flink', checkpointing=True, statistics=False)
        nokia_formulas.plot('event_rate', ['peak', 'max', 'average'], series_levels=['formula'], title="Latency (Nokia)", path="nokia_formulas.pdf")

        nokia_series = series.select(experiment='nokia2', statistics=False)
        nokia_series.plot('timestamp', 'peak', series_levels=['tool', 'processors'], column_levels=['checkpointing', 'repetition'], style='-', title="Latency (Nokia)", path="nokia_series.pdf")

        gen_nproc_export = summary.select(experiment='gen', checkpointing=True, statistics=False, formula='star', index_rate=1000)
        gen_nproc_export.export('max', 'memory', path="gen_nproc.csv")

        genh_slices_export = slices.select(experiment='genh3', tool='flink', heavy_hitters=[0,1], event_rate=4000, index_rate=1000)
        genh_slices_export.export('total_events', drop_levels=['monitor'], path="genh3_slices.csv")
    else:
        sys.stderr.write("Usage: {} path ...\n".format(sys.argv[0]))
        sys.exit(1)
