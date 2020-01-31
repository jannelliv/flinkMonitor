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

    def select(self, experiment=ANY, tool=ANY, checkpointing=ANY, statistics=ANY, numsources=ANY, processors=ANY, cmd=ANY, variant=ANY, formula=ANY, heavy_hitters=ANY, event_rate=ANY, index_rate=ANY, stage=ANY):
        # print(self.df)
        # print("Select  " + str((experiment, tool, checkpointing, statistics, processors, formula, heavy_hitters, event_rate, index_rate, stage)))
        view = self.df.loc[(experiment, tool, checkpointing, statistics, numsources, processors, cmd, variant, formula, heavy_hitters, event_rate, index_rate, stage), :]
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
    job_levels = ['experiment', 'tool', 'checkpointing', 'statistics', 'numsources', 'processors', 'cmd', 'variant', 'formula', 'heavy_hitters', 'event_rate', 'index_rate', 'stage', 'repetition']

                #metrics_nokia_flink_monpoly_stats_8_2_normalcmd_4_del_1_2_neg_5000_1_1.csv
    job_regex = r"(nokia|nokiaCMP|gen|genCMP|genh)(_flink)?_(monpoly|dejavu)(_ft)?(_stats)?_(\d+)_(\d+)_([a-z]+)_(\d+)_([a-zA-Z0-9-_]+neg)(?:_h(\d+))?_(\d+)(?:_(\d+))?_([01])_(\d+)"
    metrics_pattern = re.compile(r"metrics_" + job_regex + r"\.csv")
    delay_pattern = re.compile(job_regex + r"_delay\.txt")
    time_pattern = re.compile(job_regex + r"_time(?:_(\d+))?\.txt")
    job_pattern = re.compile(job_regex + r"_job\.txt")
    length_pattern = re.compile(r"(gen|genCMP|genh)\.length")
    events_pattern = re.compile(r"(nokia|nokiaCMP).events")
    

    delay_header = ['timestamp', 'current_indices', 'current_events', 'current_latency', 'peak', 'max', 'average']

    summary_keys = []
    summary_data = []

    summary_slice_keys = []
    summary_slice_data = []


    series_keys = []
    series_data = []

    memory_keys = []
    memory_data = []

    runtime_keys = []
    runtime_data = []

    trace_length = {}
    trace_events = {}

    debug = False

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

        if df.shape[0] > 0 and df.index.name == 'timestamp' and set(df.columns) >= {'peak', 'max', 'average', 'sum_tp'}:
            df.loc[:, 'peak'].replace(to_replace=0, inplace=True, method='ffill')

            summary = df.tail(1)
            self.summary_keys.append(key)
            self.summary_data.append(summary)

            monitor_columns = [column for column in list(df.columns) if column.startswith('monitor')] + ['sum_tp'] 
            raw_slices = df.loc[:, monitor_columns]
            slices = raw_slices.sum().rename('Total')
            self.summary_slice_keys.append(key)
            self.summary_slice_data.append(slices)

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

        if monitor_index != -1: # monitor with index -1 is the flink client time measurement
            memory = pd.DataFrame([[memory_usage]], index=pd.Index([monitor_index], name='monitor'), columns=['memory'])
            self.memory_keys.append(key)
            self.memory_data.append(memory)

    def read_time(self, key, path,flink):
        if flink: # if so read the *_job.txt file
            job_content_regex = r"Job Runtime: (?:(\d+)) ms"
            time_pattern = re.compile(job_content_regex)
            line_match = None
            try:
                with open(path, 'r') as f:
                    while True:
                        line = f.readline()
                        line_match = time_pattern.match(line)
                        if line == '' or line_match:
                            break
            except Exception as e:
                raise Exception("Error while reading file " + str(path)) from e

            if line_match:
                runtime=float(line_match.group(1))/1000
                data = pd.DataFrame([[runtime]], columns=['runtime'])
                self.runtime_keys.append(key)
                self.runtime_data.append(data)
        else:   # otherwise read the *_time.txt file
            try:
                with open(path, 'r') as f:
                    first_line = f.readline()
                    if not first_line or ';' not in first_line:
                        self.warn_invalid_file(path)
                        return
                    runtime   = np.float64(first_line.split(';', 1)[0])
            except Exception as e:
                raise Exception("Error while reading file " + str(path)) from e
            data = pd.DataFrame([[runtime]], columns=['runtime'])
            self.runtime_keys.append(key)
            self.runtime_data.append(data)

    def read_file(self, path):
        if self.debug:
            print("Reading: " + path.name)
        metrics_match = self.metrics_pattern.fullmatch(path.name)
        if metrics_match:
            if self.debug:
                print("Metrics match")
            if bool(metrics_match.group(2)):                #flink?
                tool="flink_"+metrics_match.group(3)         
            else:                                           #tool
                tool=metrics_match.group(3)
            key = (
                metrics_match.group(1),                     #experiment
                tool,
                bool(metrics_match.group(4)),               #ft?
                bool(metrics_match.group(5)),               #stats?
                int(metrics_match.group(6) or 1),           #Num sources
                int(metrics_match.group(7)),                #CPUs?
                metrics_match.group(8),                      #monpoly cmd
                int(metrics_match.group(9)),                #Multisource variant
                metrics_match.group(10).replace('_', '-'),   #formula
                int(metrics_match.group(11) or 0),           #heavy hitters?
                int(metrics_match.group(12)),                #event rate
                int(metrics_match.group(13) or 0),          #index rate?
                int(metrics_match.group(14)),               #stage
                int(metrics_match.group(15)),               #repetition
                )
            self.read_metrics(key, path)
            return

        delay_match = self.delay_pattern.fullmatch(path.name)
        if delay_match:
            if self.debug:
                print("Delay match")
            if bool(delay_match.group(2)):                #flink?
                # NOTE: We silently ignore replayer data for flink experiments.
                return
            else:                                           #tool
                tool=delay_match.group(3)
            key = (
                delay_match.group(1),                     #experiment
                tool,
                bool(delay_match.group(4)),               #ft?
                bool(delay_match.group(5)),               #stats?
                int(delay_match.group(6) or 1),           #Num sources
                int(delay_match.group(7)),                #CPUs?
                delay_match.group(8),                      #monpoly cmd
                int(delay_match.group(9)),                #Multisource variant
                delay_match.group(10).replace('_', '-'),   #formula
                int(delay_match.group(11) or 0),           #heavy hitters?
                int(delay_match.group(12)),                #event rate
                int(delay_match.group(13) or 0),          #index rate?
                int(delay_match.group(14)),               #stage
                int(delay_match.group(15)),               #repetition
                )
            self.read_replayer_delay(key, path)
            return

        time_match = self.time_pattern.fullmatch(path.name)
        if time_match:
            if self.debug:
                print("Time match")
            if bool(time_match.group(2)):                #flink?
                default_monitor_index = -1
                tool="flink_"+time_match.group(3)         
            else:                                        #tool
                default_monitor_index = 0
                tool=time_match.group(3)
            key = (
                time_match.group(1),                     #experiment
                tool,
                bool(time_match.group(4)),               #ft?
                bool(time_match.group(5)),               #stats?
                int(time_match.group(6) or 1),           #Num sources
                int(time_match.group(7)),                #CPUs?
                time_match.group(8),                 #monpoly cmd
                int(time_match.group(9)),                #Multisource variant
                time_match.group(10).replace('_', '-'),   #formula
                int(time_match.group(11) or 0),           #heavy hitters?
                int(time_match.group(12)),                #event rate
                int(time_match.group(13) or 0),          #index rate?
                int(time_match.group(14)),               #stage
                int(time_match.group(15)),               #repetition
                )
            monitor_index = int(time_match.group(16) or default_monitor_index)
            self.read_memory(key, monitor_index, path)
            if not bool(time_match.group(2)):            #flink?
                self.read_time(key,path,False)
            return

        job_match = self.job_pattern.fullmatch(path.name)
        if job_match:
            if self.debug:
                print("Job match")
            if bool(job_match.group(2)):                #flink?
                tool="flink_"+job_match.group(3)         
            else:                                        #tool
                tool=job_match.group(3)
            key = (
                job_match.group(1),                     #experiment
                tool,
                bool(job_match.group(4)),               #ft?
                bool(job_match.group(5)),               #stats?
                int(job_match.group(6) or 1),           #Num sources
                int(job_match.group(7)),                #CPUs?
                job_match.group(8),                      #monpoly cmd
                int(job_match.group(9)),                #Multisource variant
                job_match.group(10).replace('_', '-'),   #formula
                int(job_match.group(11) or 0),           #heavy hitters?
                int(job_match.group(12)),                #event rate
                int(job_match.group(13) or 0),          #index rate?
                int(job_match.group(14)),               #stage
                int(job_match.group(15)),               #repetition
                )
            self.read_time(key, path, True)
            return

        length_match = self.length_pattern.fullmatch(path.name)
        if length_match:
            try:
                with open(path, 'r') as f:
                    first_line = f.readline()
                    if not first_line:
                        self.warn_invalid_file(path)
                        return
                    self.trace_length[length_match.group(1)] = int(first_line)
            except Exception as e:
                raise Exception("Error while reading file " + str(path)) from e
            return

        events_match = self.events_pattern.fullmatch(path.name)
        if events_match:
            try:
                with open(path, 'r') as f:
                    first_line = f.readline()
                    if not first_line:
                        self.warn_invalid_file(path)
                        return
                    self.trace_events[events_match.group(1)] = int(first_line)
            except Exception as e:
                raise Exception("Error while reading file " + str(path)) from e
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

    def sum_slices(self, df):
        group_levels = list(df.index.names)
        group_levels.remove('monitor')
        return df.groupby(level=group_levels).sum()

    def calculate_throughput(self, timedf):
        def foo(x):
            if x['experiment'] in self.trace_events:
                return self.trace_events[x['experiment']]
            else:
                return x['event_rate'] * self.trace_length[x['experiment']]
        i = timedf.index
        tmp1 = timedf.reset_index()
        tmp1.index = i
        tmp1['events'] = tmp1.apply(foo, axis=1)
        timedf['throughput'] = tmp1['events'] / timedf['runtime']
        return timedf

    def process(self):
        raw_memory = pd.concat(self.memory_data, keys=self.memory_keys, names=self.job_levels)
        memory = self.max_memory(raw_memory)

        raw_summary = pd.concat(self.summary_data, sort=True, keys=self.summary_keys, names=self.job_levels)
        raw_summary.reset_index('timestamp', drop=True, inplace=True)
        raw_summary = raw_summary.merge(memory, 'outer', left_index=True, right_index=True)
        summary = self.average_repetitions(raw_summary)
        summary.sort_index(inplace=True)

        raw_slices = pd.concat(self.summary_slice_data, sort=True, keys=self.summary_slice_keys, names=self.job_levels)
        raw_slices.index.rename('monitor', level=-1, inplace=True)
        raw_slices.name = 'total_events'
        # raw_slices.reset_index('timestamp', drop=True, inplace=True)
        slices = self.average_repetitions(raw_slices).to_frame()

        series = pd.concat(self.series_data, sort=True, keys=self.series_keys, names=self.job_levels)
        series.sort_index(inplace=True)

        raw_time = pd.concat(self.runtime_data, sort=True, keys=self.runtime_keys, names=self.job_levels)
        raw_time.index.rename('bla', level=-1, inplace=True)
        raw_time.reset_index('bla', drop=True, inplace=True)
        time = self.average_repetitions(raw_time)
        throughput = self.calculate_throughput(time)

        return Data("Summary", summary), Data("Slices", slices), Data("Time series", series), Data("Throughput",throughput)

    @classmethod
    def load(cls, paths):
        loader = cls()
        for path in paths:
            loader.read_files(path)
        return loader.process()


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        paths = map(pathlib.Path, sys.argv[1:])
        summary, slices, series, throughput = Loader.load(paths)

        # print(summary.df)

        # print(slices.df)

        # print(series.df)

        # print(throughput.df)

        # EXPLORATORY PLOTTING...

        # # SYNTHETIC
        # gen_nproc = summary.select(experiment='gen', stage = 1, index_rate=1000)
        # gen_nproc.plot('event_rate', ['peak', 'max'], series_levels=['tool', 'processors'], column_levels=['statistics'], title="Latency (synthetic, 1000)" , path="gen_nproc.pdf")

        # gen_memory = summary.select(experiment='gen', checkpointing=False, statistics=False, index_rate=1000)
        # gen_memory.plot('event_rate', 'memory', series_levels=['tool', 'processors'], column_levels=['formula'], title="Max. monitor memory (synthetic, 1000)" , path="gen_memory.pdf")

        # gen_formulas = summary.select(experiment='gen',  checkpointing=True, statistics=False)
        # gen_formulas.plot('event_rate', ['peak', 'max'], series_levels=['formula', 'index_rate'], title="Latency (synthetic)", path="gen_formulas_monpoly.pdf")

        # gen_throughput = throughput.select(experiment='gen', stage=0)
        # gen_throughput.plot('event_rate', ['throughput'], series_levels=['tool', 'processors'], column_levels=['formula'], title="Throughput (synthetic)" , path="gen_throughput_monpoly.pdf")
        # gen_throughput.plot('event_rate', ['runtime'], series_levels=['tool', 'processors'], column_levels=['formula'], title="Throughput (synthetic)" , path="gen_runtime_monpoly.pdf")


        # gen_series = series.select(experiment='gen', statistics=False, stage=1)
        # gen_series.plot('timestamp', 'peak', series_levels=['tool', 'processors'], column_levels=['checkpointing', 'repetition'], style='-', title="Latency (gen)", path="gen_series.pdf")


        # gen_nproc = summary.select(experiment='genCMP')
        # gen_nproc.df.reset_index('index_rate', drop=True, inplace=True)
        # gen_nproc.plot('event_rate', ['max'], series_levels=['tool', 'processors'], column_levels=['statistics'], title="Latency (synthetic, 1000)" , path="genCMP_nproc.pdf")

        # gen_memory = summary.select(experiment='genCMP', tool='flink_dejavu')
        # gen_memory.df.reset_index('index_rate', drop=True, inplace=True)
        # gen_memory.plot('event_rate', 'memory', series_levels=['tool', 'processors'], column_levels=['formula'], title="Max. monitor memory (synthetic, 1000)" , path="genCMP_memory.pdf")

        # gen_formulas = summary.select(experiment='genCMP')
        # gen_formulas.df.reset_index('index_rate', drop=True, inplace=True)
        # gen_formulas.plot('event_rate', ['max'], series_levels=['formula'], column_levels=['statistics'], title="Latency (synthetic)", path="genCMP_formulas.pdf")

        # gen_throughput = throughput.select(experiment='genCMP', stage=0)
        # gen_throughput.df.reset_index('index_rate', drop=True, inplace=True)
        # gen_throughput.plot('event_rate', ['throughput'], series_levels=['tool', 'processors'], column_levels=['formula'], title="Throughput (synthetic with Dejavu)" , path="gen_throughput_dejavu.pdf")
        # gen_throughput.plot('event_rate', ['runtime'], series_levels=['tool', 'processors'], column_levels=['formula'], title="Throughput (synthetic with Dejavu)" , path="gen_runtime_dejavu.pdf")


        # # NOKIA
        nokia_nproc = summary.select(experiment='nokia', stage=1)
        nokia_nproc.plot('event_rate', ['max'], series_levels=['tool', 'processors'], column_levels=['numsources'],  title="Latency (Nokia)" , path="nokia_nproc.pdf")
        
        #nokia_formulas = summary.select(experiment='nokia', tool='flink_monpoly', checkpointing=True, stage=1)
        #nokia_formulas.plot('event_rate', ['max'], series_levels=['formula'], column_levels=['statistics'], title="Latency (Nokia)", path="nokia_formulas.pdf")
        
        #nokia_series = series.select(experiment='nokia', statistics=False, stage=1)
        #nokia_series.plot('timestamp', 'peak', series_levels=['tool', 'processors'], column_levels=['checkpointing', 'repetition'], style='-', title="Latency (Nokia)", path="nokia_series.pdf")

        #nokia_throughput = throughput.select(experiment='nokia', stage=1)
        #nokia_throughput.plot('event_rate', ['throughput'], series_levels=['tool', 'processors'], column_levels=['formula'], title="Throughput (nokia)" , path="nokia_throughput_monpoly.pdf")
        #nokia_throughput.plot('event_rate', ['runtime'], series_levels=['tool', 'processors'], column_levels=['formula'], title="Throughput (nokia)" , path="nokia_runtime_monpoly.pdf")


        # nokia_nproc = summary.select(experiment='nokiaCMP', stage=1)
        # nokia_nproc.plot('event_rate', ['max'], series_levels=['tool', 'processors'], title="Latency (Nokia)" , path="nokiaCMP_nproc.pdf")

        # nokia_series = series.select(experiment='nokiaCMP', stage=1)
        # nokia_series.plot('timestamp', 'peak', series_levels=['tool', 'processors'], column_levels=['checkpointing', 'repetition'], style='-', title="Latency (Nokia)", path="nokiaCMP_series.pdf")

        # nokia_throughput = throughput.select(experiment='nokiaCMP', stage=1)
        # nokia_throughput.plot('event_rate', ['throughput'], series_levels=['tool', 'processors'], column_levels=['formula'], title="Throughput (nokia with Dejavu)" , path="nokia_throughput_dejavu.pdf")
        # nokia_throughput.plot('event_rate', ['runtime'], series_levels=['tool', 'processors'], column_levels=['formula'], title="Throughput (nokia with Dejavu)" , path="nokia_runtime_dejavu.pdf")
        
        # # SLICES
        # gen_slices = slices.select(experiment='gen', tool='flink_monpoly', checkpointing=False, index_rate=1)
        # gen_slices.plot('processors', 'total_events', column_levels=['formula'], box_plot='monitor', title="Slice sizes (synthetic)", path="gen_slices.pdf")

        # genh_slices = slices.select(experiment='genh', tool='flink_monpoly', checkpointing=True, event_rate=750, index_rate=1, stage=1)
        # monitors=['monitor_tp0','monitor_tp1','monitor_tp2','monitor_tp3','monitor_tp4','monitor_tp5','monitor_tp6','monitor_tp7','monitor_tp8','monitor_tp9','monitor_tp10','monitor_tp11','monitor_tp12','monitor_tp13','monitor_tp14','monitor_tp15']
        # genh_slices.df=genh_slices.df.loc[(ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,monitors), :]
        # genh_slices.plot('processors', 'total_events', column_levels=['statistics', 'heavy_hitters'], box_plot='monitor', title="Slice sizes (synthetic w/ skew)", path="genh_slices.pdf")


        # EXPORTING...
       
        # # ALL
        # gen_nproc_export = summary.select()
        # gen_nproc_export.export('max', 'peak', 'average', 'memory', path="all.csv")

        # # PLOT1
        #synth_export = summary.select(experiment='gen', heavy_hitters=0, stage=1)
        #synth_export.export('max', 'memory', path="plot1-latency.csv")

        #synth_export = throughput.select(experiment='gen', heavy_hitters=0, stage=0)
        #synth_export.export('throughput', path="plot1-throughput.csv")

        # # PLOT1-dejavu
        #synth_export = summary.select(experiment='genCMP',checkpointing=False, heavy_hitters=0, stage=1)
        #synth_export.export('max', 'memory', path="plot1-dejavu-latency.csv")

        #synth_export = throughput.select(experiment='genCMP',checkpointing=False, heavy_hitters=0, stage=0)
        #synth_export.export('throughput', path="plot1-dejavu-throughput.csv")


        # # PLOT2
        #nokia_export = summary.select(experiment='nokia', statistics=False, heavy_hitters=0, stage=1)
        #nokia_export.export('max', 'memory', path="plot2-latency.csv")

        #nokia_export = series.select(experiment='nokia', statistics=False, heavy_hitters=0, stage=1)
        #nokia_export.df=nokia_export.df.loc[(ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,1), :]
        #nokia_export.export('peak', 'memory', path="plot2-series.csv")


        # # PLOT2-dejavu
        #nokia_export = summary.select(experiment='nokiaCMP', statistics=False, heavy_hitters=0, stage=1)
        #nokia_export.export('max', 'memory', path="plot2-dejavu-latency.csv")

        #nokia_export = series.select(experiment='nokiaCMP', statistics=False, heavy_hitters=0, stage=1)
        #nokia_export.df=nokia_export.df.loc[(ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,1), :]
        #nokia_export.export('peak', 'memory', path="plot2-dejavu-series.csv")

        # # PLOT3
        #genh_slices_export = slices.select(experiment='genh', tool='flink_monpoly', checkpointing=True, event_rate=750, index_rate=1, stage=1)
        #monitors=['monitor_tp0','monitor_tp1','monitor_tp2','monitor_tp3','monitor_tp4','monitor_tp5','monitor_tp6','monitor_tp7','monitor_tp8','monitor_tp9','monitor_tp10','monitor_tp11','monitor_tp12','monitor_tp13','monitor_tp14','monitor_tp15']
        #genh_slices_export.df=genh_slices_export.df.loc[(ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,ANY,monitors), :]
        #genh_slices_export.export('total_events', drop_levels=['monitor'], path="plot3-slices.csv")



    else:
        sys.stderr.write("Usage: {} path ...\n".format(sys.argv[0]))
        sys.exit(1)
