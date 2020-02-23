library("stringr")
library("purrr")
library("cowplot")
library(dplyr)
library(ggplot2)
max_peak <- function(file_name, n_cols_drop) {
  tmp_file = read.csv(file_name)
  if (n_cols_drop != 0) {
    file_tail = tmp_file[-(1:n_cols_drop),]
    return(max(file_tail$peak))
  } else {
    return(max(tmp_file$peak))
  }
}

all_peak <- function(file_name, n_cols_drop) {
  tmp_file = read.csv(file_name)
  if (n_cols_drop != 0) {
    file_tail = tmp_file[-(1:n_cols_drop),]
    return(select(file_tail, "timestamp", "peak"))
  } else {
    return(select(tmp_file, "timestamp", "peak"))
  }
}
process_max_peak <- function(name, m_regex, params, n_cols_drop) {
  metrics_regex = "metrics_(.*)"
  frame = dir(name) %>%
    map(function (x) {
          tmp = str_match(x, metrics_regex)[2]
          if (is.na(tmp)) {
            return(list())
          } else {
            return(list(tmp))
          }
        }) %>%
    flatten() %>%
    map_dfr(function (x) {
      data.frame()
      mp = max_peak(paste0(name, "/metrics_", x), n_cols_drop)
      mp = append(mp, str_match(x, m_regex)[2:(length(params) + 1)])
      return(as.data.frame(t(mp)))
    })
  colnames(frame) <- append(list("maxlatency"), params)
  return(frame)
}

process_all_peak <- function(name, m_regex, params, n_cols_drop) {
  metrics_regex = "metrics_(.*)"
  frame = dir(name) %>%
    map(function (x) {
      tmp = str_match(x, metrics_regex)[2]
      if (is.na(tmp)) {
        return(list())
      } else {
        return(list(tmp))
      }
    }) %>%
    flatten() %>%
    map_dfr(function (x) {
      peaks = filter(all_peak(paste0(name, "/metrics_", x), n_cols_drop), peak != 0)
      df = str_match(x, m_regex)[2:(length(params) + 1)]
      df = as.data.frame(t(df))
      return(merge(peaks, df, by=NULL))
    })
  colnames(frame)[-(1:2)] <- params
  return(frame)
}

nokia_regex = "^nokia_flink_monpoly_(\\d+)_(\\d+)_([a-z]+)_(\\d+)_([A-Za-z_0-9]+_neg)_(\\d+)_1_(\\d+)\\.csv$"
synthetic_regex = "^gen_flink_monpoly_(\\d+)_(\\d+)_(\\d+)_([A-Za-z-0-9]+_neg)_(\\d+)_(\\d+)_(\\d+)_(\\d+)_(\\d+)_0_(\\d+)\\.csv$"
imbalance_regex = "^gen_flink_monpoly_(\\d+)_(\\d+)_(\\d+)_([A-Za-z-0-9]+_neg)_(\\d+)_(\\d+)_(\\d+)_(\\d+)_(\\d+)_0_(\\d+)_(\\d+)\\.csv$"
imbalance_latency = process_max_peak("socket_imbalance_reports", imbalance_regex, c("procs", "numsources", "variant", "formula", "eventrate", "indexrate", "maxooo", "wmperiod", "acc", "distidx", "rep"), 30)
nokia_exp_list = c("nokia_kafka_overhead_reports", "nokia_socket_no_overhead_reports", "nokia_socket_overhead_reports", "nokia_socket_reports")
synthetic_exp_list = c("socket_reports", "socket_mode4_reports")
experiments = list()
for (experiment in nokia_exp_list) {
  tmp = process_max_peak(experiment, nokia_regex, c("numsources", "procs", "cmd", "variant", "formula", "acc", "rep"), 0)
  experiments[[experiment]] <- tmp
}
for (experiment in synthetic_exp_list) {
  tmp = process_max_peak(experiment, synthetic_regex, c("procs", "numsources", "variant", "formula", "eventrate", "indexrate", "maxooo", "wmperiod", "acc", "rep"), 10)
  experiments[[experiment]] <- tmp
}
kafka_overhead_reports_peak = process_all_peak("nokia_kafka_overhead_reports", nokia_regex, c("numsources", "procs", "cmd", "variant", "formula", "acc", "rep"), 0)
no_overhead_reports_peak = process_all_peak("nokia_socket_no_overhead_reports", nokia_regex, c("numsources", "procs", "cmd", "variant", "formula", "acc", "rep"), 0)

#Imbalance plot
imbalance_latency$eventrate <- as.integer(as.character(imbalance_latency$eventrate))
imbalance_latency$maxlatency <- as.numeric(as.character(imbalance_latency$maxlatency))
ggplot(data = imbalance_latency, aes(x=eventrate, y=maxlatency, group=distidx)) + geom_line(aes(linetype=distidx)) + geom_point()

#synthetic plot with index rate
socket_reports = experiments$socket_reports
socket_reports$eventrate <- as.integer(as.character(socket_reports$eventrate))
socket_reports$maxlatency <- as.numeric(as.character(socket_reports$maxlatency))
fixed_idx_rate <- filter(socket_reports, indexrate==2000)
fixed_no_sources <- filter(socket_reports, numsources==2)
fixed_idx_rate1 <- filter(fixed_idx_rate, variant==1)
fixed_idx_rate2 <- filter(fixed_idx_rate, variant==2)
fixed_no_sources1 <- filter(fixed_no_sources, variant==1)
fixed_no_sources2 <- filter(fixed_no_sources, variant==2)
pg00 <- ggplot(data = fixed_no_sources1, aes(x=eventrate, y=maxlatency, group=indexrate)) + geom_line(aes(color=indexrate)) + geom_point()
pg01 <- ggplot(data = fixed_no_sources2, aes(x=eventrate, y=maxlatency, group=indexrate)) + geom_line(aes(color=indexrate)) + geom_point()
pg10 <- ggplot(data = fixed_idx_rate1, aes(x=eventrate, y=maxlatency, group=numsources)) + geom_line(aes(color=numsources)) + geom_point()
pg11 <- ggplot(data = fixed_idx_rate2, aes(x=eventrate, y=maxlatency, group=numsources)) + geom_line(aes(color=numsources)) + geom_point()
plot_grid(pg00, pg01, pg10, pg11, labels=c("mode1", "mode2", "mode1", "mode2"), ncol = 2, nrow = 2)

#mode 4 plots
mode4_reports = filter(experiments$socket_mode4_reports, numsources != 1)
mode4_reports$maxlatency <- as.numeric(as.character(mode4_reports$maxlatency))
mode4_reports$maxooo <- as.integer(as.character(mode4_reports$maxooo))
ggplot(data = mode4_reports, aes(x=maxooo, y=maxlatency, group=numsources)) + geom_line(aes(color=numsources)) + geom_point() + facet_grid(. ~ wmperiod)

#nokia grid
nokia_reports = filter(experiments$nokia_socket_reports, procs != 1)
nokia_reports$maxlatency <- as.numeric(as.character(nokia_reports$maxlatency))
nokia_reports$acc <- as.integer(as.character(nokia_reports$acc))
nokia_reports$numsources <- as.integer(as.character(nokia_reports$numsources))
nokia_reports$procs <- as.integer(as.character(nokia_reports$procs))
ggplot(data = nokia_reports, aes(x=acc, y=maxlatency, group=numsources)) + geom_line(aes(color=numsources)) + geom_point() + facet_grid(procs ~ formula)

#kafka vs sockets experiment
kafka_overhead_add = merge(data.frame(source="kafka"), kafka_overhead_reports_peak, by = NULL)
min_kafka = group_by(kafka_overhead_add, acc) %>% summarise(mints = min(timestamp))
no_overhead_report_add = merge(data.frame(source="sockets"), no_overhead_reports_peak, by = NULL)
min_no_overhead = group_by(no_overhead_report_add, acc) %>% summarise(mints = min(timestamp))
kafka_overhead_add = merge(kafka_overhead_add, min_kafka, by = "acc")
no_overhead_report_add = merge(no_overhead_report_add, min_no_overhead, by = "acc")
kafka_overhead_add = kafka_overhead_add %>% mutate(timestamp = timestamp - mints) %>% select(-"mints")
no_overhead_report_add = no_overhead_report_add %>% mutate(timestamp = timestamp - mints) %>% select(-"mints")
overhead_joined = rbind(kafka_overhead_add, no_overhead_report_add)
overhead_joined$timestamp <- as.integer(as.character(overhead_joined$timestamp))
overhead_joined$acc <- as.integer(as.character(overhead_joined$acc))
overhead_joined = filter(overhead_joined, acc < 5000)
overhead_joined$peak <- as.numeric(as.character(overhead_joined$peak))
ggplot(data = overhead_joined, aes(x=timestamp, y=peak, group=source)) + geom_line(aes(color=source)) + facet_grid(. ~ acc) + ylim(0,20000) + xlim(0,65)

