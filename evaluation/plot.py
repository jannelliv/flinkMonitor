#!/usr/bin/env python3

import itertools
import pathlib
import re
import sys

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
    if isinstance(value, bool):
        return name if value else "no " + name
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

    def plot(self, x_level, y_columns, series_levels=[], column_levels=[], title=None, path=None):
        df = self.df.reset_index(level=x_level)

        y_columns = [y_columns] if isinstance(y_columns, str) else y_columns
        series_levels = set(series_levels)
        column_levels = set(column_levels)

        index = df.index.remove_unused_levels()
        levels = varying_levels(index)
        extra_levels = levels - series_levels - {x_level}

        row_levels = extra_levels - column_levels
        column_levels = extra_levels & column_levels
        series_levels = levels & set(series_levels)

        row_level_list, row_keys = enumerate_keys(index, row_levels)
        column_level_list, column_keys = enumerate_keys(index, column_levels)
        series_level_list, series_keys = enumerate_keys(index, series_levels)
        plot_key_names = row_level_list + column_level_list
        series_key_names = plot_key_names + series_level_list

        nrows = len(row_keys)
        ncols = len(column_keys) * len(y_columns)
        figsize = (4.0 * ncols, 3.0 * nrows)
        fig, axes = plt.subplots(nrows, ncols, sharex='row', sharey='row', squeeze=False, figsize=figsize)
        if title is not None:
            fig.suptitle(title)

        lines = []
        for row, row_key in enumerate(row_keys):
            for col1, column_key in enumerate(column_keys):
                for col2, y_column in enumerate(y_columns):
                    col = col1 * len(y_columns) + col2
                    plot_key = row_key + column_key

                    ax = axes[row, col]
                    ax.set_title(describe_key(plot_key_names + [""], plot_key + (y_column,)), fontdict=self.subplot_font)
                    ax.set_xlabel(x_level)
                    #ax.set_ylabel(y_column)

                    lines = []
                    for series in series_keys:
                        selector = key_to_selector(index, series_key_names, plot_key + series)
                        X = df.loc[selector, x_level]
                        Y = df.loc[selector, y_column]
                        lines += ax.plot(X, Y, '-o')

        fig.legend(lines, map(lambda k: describe_key(series_level_list, k), series_keys), 'upper right')
        fig.tight_layout(pad=0.5, h_pad=1, w_pad=0.5, rect=[0, 0, 1 - 1 / figsize[0], 1 - 0.8 / figsize[1]])
        if path is not None:
            fig.savefig(path)
        return fig


class Loader:
    job_levels = ['experiment', 'tool', 'checkpointing', 'statistics', 'processors', 'formula', 'heavy_hitters', 'event_rate', 'index_rate', 'repetition']

    job_regex = r"(nokia|nokia2|gen|genh3)_(monpoly|flink)(_ft)?(_stats)?(?:_(\d+))?_((?:del|ins)_\d_\d|[a-zA-Z0-9]+)(?:_h(\d+))?_(\d+)(?:_(\d+))?_(\d+)"
    metrics_pattern = re.compile(r"metrics_" + job_regex + r"\.csv")
    delay_pattern = re.compile(job_regex + r"_delay\.txt")
    time_pattern = re.compile(job_regex + r"_time(?:_(\d+))?\.txt")

    summary_keys = []
    summary_data = []

    latency_keys = []
    latency_data = []

    def warn_skipped_path(self, path):
        warn("Skipped " + str(path))

    def warn_invalid_file(self, path):
        warn("Invalid data in file " + str(path))

    def read_metrics(self, key, path):
        try:
            df = pd.read_csv(path, header=0, index_col=0)
        except Exception as e:
            raise Exception("Error while reading file " + str(entry)) from e

        if df.shape[0] > 0 and df.index.name == 'timestamp' and set(df.columns) >= {'peak', 'max', 'average'}:
            df.loc[:, 'peak'].replace(to_replace=0, inplace=True, method='ffill')

            summary = df.tail(1)
            self.summary_keys.append(key)
            self.summary_data.append(summary)

            first_timestamp = df.iloc[0].name
            latency = df.loc[:, 'peak'].rename(index = lambda timestamp: timestamp - first_timestamp)
            self.latency_keys.append(key)
            self.latency_data.append(latency)
        else:
            self.warn_invalid_file(path)

    def read_replayer_delay(self, key, path):
        raise NotImplemented()

    def read_file(self, path):
        metrics_match = self.metrics_pattern.fullmatch(path.name)
        if metrics_match:
            key = (
                metrics_match.group(1),
                metrics_match.group(2),
                bool(metrics_match.group(3)),
                bool(metrics_match.group(4)),
                int(metrics_match.group(5)),
                metrics_match.group(6),
                int(metrics_match.group(7) or 0),
                int(metrics_match.group(8)),
                int(metrics_match.group(9) or 0),
                int(metrics_match.group(10))
                )
            self.read_metrics(key, path)
        else:
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

    def average_repetitions(self, df):
        group_levels = list(df.index.names)
        group_levels.remove('repetition')
        group_levels.remove('timestamp')
        return df.groupby(level=group_levels).mean()

    def process(self):
        raw_summary = pd.concat(self.summary_data, keys=self.summary_keys, names=self.job_levels)
        summary = self.average_repetitions(raw_summary)
        latency = pd.concat(self.latency_data, keys=self.latency_keys, names=self.job_levels)
        return Data("Summary", summary), Data("Peak latency", latency)

    @classmethod
    def load(cls, paths):
        loader = cls()
        for path in paths:
            loader.read_files(path)
        return loader.process()


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        paths = map(pathlib.Path, sys.argv[1:])
        summary, latency = Loader.load(paths)

        gen_nproc = summary.select(experiment='gen', statistics=False, index_rate=1000)
        gen_nproc.plot('event_rate', ['peak', 'max', 'average'], series_levels=['tool', 'processors'], title="Latency (synthetic, 1000)" , path="gen_nproc.pdf")

        gen_formulas = summary.select(experiment='gen', tool='flink', checkpointing=True, statistics=False)
        gen_formulas.plot('event_rate', ['peak', 'max', 'average'], series_levels=['formula', 'index_rate'], title="Latency (synthetic)", path="gen_formulas.pdf")

        nokia_nproc = summary.select(experiment='nokia')
        nokia_nproc.plot('event_rate', ['peak', 'max', 'average'], series_levels=['tool', 'processors'], title="Latency (Nokia)" , path="nokia_nproc.pdf")

        nokia_formulas = summary.select(experiment='nokia', tool='flink', checkpointing=True, statistics=False)
        nokia_formulas.plot('event_rate', ['peak', 'max', 'average'], series_levels=['formula'], title="Latency (Nokia)", path="nokia_formulas.pdf")
    else:
        sys.stderr.write("Usage: {} path ...\n".format(sys.argv[0]))
        sys.exit(1)
