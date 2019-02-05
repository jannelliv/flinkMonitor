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

    #     job_levels = ['experiment', 'formula', 'event_rate', 'index_rate', 'strategy', 'processors', part']

    def select(self, experiment=ANY, formula=ANY, event_rate=ANY, index_rate=ANY, strategy=ANY, processors=ANY, part=ANY):
        view = self.df.loc[(experiment, formula, event_rate, index_rate, strategy, processors, part), :]
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
    job_levels = ['experiment', 'formula', 'event_rate', 'index_rate', 'strategy', 'processors'] 
    #add part, repetition and slice
    job_levels_full = job_levels + ['part', 'slice', 'repetition']

    job_regex = r"(nokia|gen|genh|genadaptive)_(-S|-L|-T)_(\d+)_(\d+)_(\d+)_(\d+)"
    slices_pattern = re.compile(job_regex)
    
    slices_header = ['baseline', 'merge', 'monitor', 'split']
    slices_header_full = slices_header + ['adaptive']
    

    slices_keys = []
    slices_data = []

    def warn_skipped_path(self, path):
        #warn("Skipped " + str(path))
        pass

    def warn_invalid_file(self, path):
        warn("Invalid data in file " + str(path))

    def read_slices(self, key, path):
        try:
            df = pd.read_csv(path, header=0, sep=',\s+', engine='python', index_col=[0,1])
        except Exception as e:
            raise Exception("Error while reading file " + str(path)) from e

        
        if df.shape[0] > 0 and df.index.names == ['Part', 'Repetition'] and set(df.columns) >= {'Baseline0', 'Split0', 'Monitor0', 'Merge0'}:
            df.replace(to_replace='- ', inplace=True, method='ffill', value=0)
            df.replace(to_replace=' -', inplace=True, method='ffill', value=0)
            df.replace(to_replace='-', inplace=True, method='ffill', value=0)
        
            summary = df
            self.slices_keys.append(key)
            self.slices_data.append(summary)

        else:
            self.warn_invalid_file(path)


    def read_file(self, path):
        metrics_match = self.slices_pattern.fullmatch(path.name)
        if metrics_match:
            key = (
                metrics_match.group(1),
                metrics_match.group(2),
                int(metrics_match.group(3)),
                int(metrics_match.group(4)),
                int(metrics_match.group(5)),
                int(metrics_match.group(6))
                )
            self.read_slices(key, path)
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

    def max_slice(self, df):
        group_levels = list(df.index.names)
        group_levels.remove('slice')
        return df.groupby(level=group_levels).max()

    def process(self):
        names = self.job_levels+['part', 'repetition']

        raw = pd.concat(self.slices_data, keys=self.slices_keys, names=self.job_levels, sort=True)
        raw.index.rename(names)

        raw=raw.drop(axis=0,labels=0,level=6)

        raw=raw.apply(pd.to_numeric)
        
        cpus = raw.index.levels[5].max()
        df_dict = {}
        for i in range(0,cpus):
            tmp = raw.loc[:, ['Baseline'+str(i), 'Merge'+str(i),'Monitor'+str(i),'Split'+str(i)]]
            tmp = tmp.rename(columns=dict(list(zip(tmp.columns.tolist(),self.slices_header))))
            df_dict[i]=tmp
        
        raw = pd.concat(df_dict.values(), keys=df_dict.keys(), axis=0, names=['slice']+names, sort=True)
        raw = raw.reorder_levels(self.job_levels_full)
        slices = raw.dropna(axis=0, how = 'all')
        slices = self.average_repetitions(slices)
        slices = self.max_slice(slices)
        slices['adaptive']=slices[['merge','monitor','split']].sum(axis=1)

        return Data("Slices", slices)

    @classmethod
    def load(cls, paths):
        loader = cls()
        for path in paths:
            loader.read_files(path)
        return loader.process()


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        paths = map(pathlib.Path, sys.argv[1:])
        slices = Loader.load(paths)

        #ADAPTIVE
        #Index: ['experiment', 'formula', 'event_rate', 'index_rate', 'strategy', 'processors', 'part']
        #Columns: ['baseline', 'merge', 'monitor', 'split', 'adaptive']

        gen_adapt_strat = slices.select(experiment='genadaptive', part=2)
        gen_adapt_strat.plot('strategy', ['baseline', 'adaptive'], series_levels=['event_rate','index_rate'], column_levels=['formula'], title="Time x-strategy" , path="gen_adapt_strat.pdf")
        

        gen_adapt_nproc = slices.select(experiment='genadaptive', part=2)
        gen_adapt_nproc.plot('processors', ['baseline', 'adaptive'], series_levels=['event_rate','index_rate'], column_levels=['formula'], title="Time x-processors" , path="gen_adapt_nproc.pdf")
        


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
        # gen_nproc_export.export('max', 'peak', 'average', 'memory', path="all.csv")

        # # PLOT1
        # gen_nproc_export = summary.select(experiment='gen',checkpointing=True)
        # gen_nproc_export.export('max', 'peak', 'average', 'memory', path="plot-synthetic.csv")

        # # PLOT2
        # nokia_nproc = summary.select(experiment='nokia', statistics=False)
        # nokia_nproc.export('max', 'peak', 'average', 'memory', path="plot-nokia.csv")

        # # PLOT3
        # nokia_series = series.select(experiment='nokia', checkpointing=False, repetition=2, statistics=False)
        # nokia_series.export('peak', path="plot-nokia-time-f.csv")

        # # PLOT4
        # nokia_series = series.select(experiment='nokia', checkpointing=True, repetition=2, statistics=False)
        # nokia_series.export('peak', path="plot-nokia-time-t.csv")
    
        # # PLOT5
        # genh_slices_export = slices.select(experiment='genh', tool='flink', heavy_hitters=[0,1], event_rate=4000, index_rate=1000)
        # genh_slices_export.export('total_events', drop_levels=['monitor'], path="genh_slices.csv")

    else:
        sys.stderr.write("Usage: {} path ...\n".format(sys.argv[0]))
        sys.exit(1)