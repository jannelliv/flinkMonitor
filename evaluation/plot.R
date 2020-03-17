library("stringr")
library("purrr")
library("cowplot")
library("dplyr")
library("ggplot2")
library("tikzDevice")
library("RColorBrewer")
library("grid")
library("gtable")

plot_overall_labels <- function(p, row_label = NULL, col_label = NULL) {
  labelR = row_label
  labelT = col_label
  
  # Get the ggplot grob
  z <- ggplotGrob(p)
  
  # Get the positions of the strips in the gtable: t = top, l = left, ...
  if (!is.null(row_label)) {
    posR <- subset(z$layout, grepl("strip-r", name), select = t:r)
  }
  if (!is.null(col_label)) {
    posT <- subset(z$layout, grepl("strip-t", name), select = t:r)
  }
  
  # Add a new column to the right of current right strips, 
  # and a new row on top of current top strips
  if (!is.null(row_label)) {
    width <- z$widths[max(posR$r)]    # width of current right strips
  }
  if (!is.null(col_label)) {
    height <- z$heights[min(posT$t)]  # height of current top strips
  }
  if (!is.null(row_label)) {
    z <- gtable_add_cols(z, width, max(posR$r))
  }
  if (!is.null(col_label)) {
    z <- gtable_add_rows(z, height, min(posT$t)-1)
  }
  
  # Construct the new strip grobs
  if (!is.null(row_label)) {
    stripR <- gTree(name = "Strip_right", children = gList(
      rectGrob(gp = gpar(col = NA, fill = "grey85")),
      textGrob(labelR, rot = -90, gp = gpar(fontsize = 10.0, col = "grey10"))))
  }
  
  if (!is.null(col_label)) {
    stripT <- gTree(name = "Strip_top", children = gList(
      rectGrob(gp = gpar(col = NA, fill = "grey85")),
      textGrob(labelT, gp = gpar(fontsize = 10.0, col = "grey10"))))
  }
  
  # Position the grobs in the gtable
  if (!is.null(row_label)) {
    z <- gtable_add_grob(z, stripR, t = min(posR$t)+1, l = max(posR$r) + 1, b = max(posR$b)+1, name = "strip-right")
  }
  if (!is.null(col_label)) {
    z <- gtable_add_grob(z, stripT, t = min(posT$t), l = min(posT$l), r = max(posT$r), name = "strip-top")
  }
  
  # Add small gaps between strips
  if (!is.null(row_label)) {
    z <- gtable_add_cols(z, unit(1/5, "line"), max(posR$r))
  }
  if (!is.null(col_label)) {
    z <- gtable_add_rows(z, unit(1/5, "line"), min(posT$t))
  }
  
  # Draw it
  grid.newpage()
  print(grid.draw(z))
}

std <- function(x) sd(x)/sqrt(length(x))

prepend_labeller <- function(t) {
  return (function (x) str_c(t, x))
}

kformatter <- function(x) paste0(format(round(x / 1e3, 1), trim = TRUE, scientific = FALSE), "k")

variant_to_text <- function(v) {
  return(case_when (
    v == 1 ~ "Total order",
    v == 2 ~ "Per-partition order",
    v == 4 ~ "Watermark order"
  ))
}
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
  frame$maxlatency <- as.numeric(as.character(frame$maxlatency))
  frame = mutate(frame, maxlatency=maxlatency/1000)
  frame = select(frame, -"rep") %>%
    group_by_at(vars(-maxlatency)) %>%
    summarize(
      min_run=mean(maxlatency) - 2*std(maxlatency),
      max_run=mean(maxlatency) + 2*std(maxlatency),
      maxlatency=mean(maxlatency)) %>%
    ungroup()
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
  frame$peak
  frame = mutate(frame, peak=peak/1000)
  colnames(frame)[-(1:2)] <- params
  return(frame)
}

nokia_regex = "^nokia_flink_monpoly_([a-z]+)_([a-z]+)_(\\d+)_(\\d+)_(\\d+)_([A-Za-z_0-9]+_neg)_(\\d+)_(\\d+)\\.csv$"
nokia_cols = c("inptype", "reorder", "numsources", "procs", "variant", "formula", "acc", "rep")
synthetic_regex = "^gen_flink_monpoly_(\\d+)_(\\d+)_(\\d+)_([A-Za-z-0-9]+_neg)_(\\d+)_(\\d+)_(\\d+)_(\\d+)_(\\d+)_0_(\\d+)\\.csv$"
imbalance_regex = "^gen_flink_monpoly_(\\d+)_(\\d+)_(\\d+)_([A-Za-z-0-9]+_neg)_(\\d+)_(\\d+)_(\\d+)_(\\d+)_(\\d+)_0_(\\d+)_(\\d+)\\.csv$"
imbalance_latency = process_max_peak("socket_imbalance_reports", imbalance_regex, c("procs", "numsources", "variant", "formula", "eventrate", "indexrate", "maxooo", "wmperiod", "acc", "distidx", "rep"), 10)
nokia_exp_list = c("nokia1_reports", "nokia2_reports", "nokia3_reports")
synthetic_exp_list = c("socket_reports", "socket_mode4_reports")
experiments = list()
for (experiment in nokia_exp_list) {
  tmp = process_max_peak(experiment, nokia_regex, nokia_cols, 0)
  experiments[[experiment]] <- tmp
}
for (experiment in synthetic_exp_list) {
  tmp = process_max_peak(experiment, synthetic_regex, c("procs", "numsources", "variant", "formula", "eventrate", "indexrate", "maxooo", "wmperiod", "acc", "rep"), 10)
  experiments[[experiment]] <- tmp
}

nokia3_peak = process_all_peak("nokia3_reports", nokia_regex, nokia_cols, 0)
#Imbalance plot
imbalance_latency$eventrate <- as.numeric(as.character(imbalance_latency$eventrate))
tikz(file = "imbalance_plot.tex", width = 4.5, height = 3)
plot <- ggplot(data = imbalance_latency, aes(x=eventrate, y=maxlatency, group=distidx)) +
  geom_line(aes(linetype=distidx)) +
  geom_point(aes(shape=distidx)) +
  coord_cartesian(ylim = c(0,25)) +
  scale_x_continuous(labels = kformatter) +
  labs(x = "Event rate (1/s)", y="Maximum Latency (s)") +
  scale_linetype_discrete(name = "Load distribution", breaks = c(0,2,1), labels=c("$(\\frac{1}{4},\\frac{1}{4},\\frac{1}{4},\\frac{1}{4})$", "$(\\frac{1}{3},\\frac{1}{3},\\frac{1}{6},\\frac{1}{6})$", "$(\\frac{2}{3},\\frac{1}{9},\\frac{1}{9},\\frac{1}{9})$")) +
  scale_shape_discrete(name = "Load distribution", breaks = c(0,2,1), labels=c("$(\\frac{1}{4},\\frac{1}{4},\\frac{1}{4},\\frac{1}{4})$", "$(\\frac{1}{3},\\frac{1}{3},\\frac{1}{6},\\frac{1}{6})$", "$(\\frac{2}{3},\\frac{1}{9},\\frac{1}{9},\\frac{1}{9})$")) +
  theme_bw() +
  theme(legend.position = "bottom")
print(plot)
dev.off()
print(plot)

#synthetic plot with index rate
socket_reports = experiments$socket_reports
socket_reports <- socket_reports %>% mutate(variant = as.factor(variant_to_text(variant)))
socket_reports$eventrate <- as.numeric(as.character(socket_reports$eventrate))
fixed_idx_rate <- filter(socket_reports, indexrate==4000, numsources!=1)
#fixed_no_sources <- filter(socket_reports, numsources!=1)
# pg0 <- ggplot(data = fixed_no_sources, aes(x=eventrate, y=maxlatency, group=indexrate)) +
#   geom_line(aes(linetype=indexrate)) +
#   geom_point(aes(shape=indexrate)) +
#   labs(x = "Eventrate (1/s)", y = "Maximum latency (s)", shape="Indexrate", linetype="Indexrate") +
#   scale_x_continuous(labels = kformatter) +
#   geom_errorbar(aes(x=eventrate, ymin=min_run, ymax=max_run, color=indexrate)) +
#   facet_grid(numsources ~ variant) +
#   theme_bw() +
#   theme(legend.position = "bottom")
pg1 <- ggplot(data = fixed_idx_rate, aes(x=eventrate, y=maxlatency, group=numsources)) +
  geom_line(aes(linetype=numsources)) +
  geom_point(aes(shape=numsources)) +
  scale_x_continuous(labels = kformatter) +
  labs(x = "Eventrate (1/s)", y = "Maximum latency (s)", shape="Number of inputs", linetype="Number of inputs") +
  geom_errorbar(aes(x=eventrate, ymin=min_run, ymax=max_run), width=20000, size=0.2, show.legend = FALSE) +
  facet_grid(. ~ variant) +
  theme_bw() +
  theme(legend.position = "bottom")

# tikz(file = "synth_plot_fixed_src.tex", width = 5, height = 3)
# print(pg0)
# dev.off()
# print(pg0)
print(pg1)
tikz(file = "synth_plot_fixed_idx_rate.tex", width = 5, height = 3)
print(pg1)
dev.off()

#mode 4 plots
mode4_reports = filter(experiments$socket_mode4_reports, numsources != 1)
mode4_reports$maxooo <- as.integer(as.character(mode4_reports$maxooo))
plot <- ggplot(data = mode4_reports, aes(x=maxooo, y=maxlatency, group=numsources)) +
  geom_line(aes(linetype=numsources)) +
  geom_point(aes(shape=numsources)) +
  geom_errorbar(aes(x=maxooo, ymin=min_run, ymax=max_run), size=0.25, width=0.2, show.legend = FALSE) +
  labs(x = "$\\delta_{\\mathit{max}}$ (s)", y="Maximum Latency (s)", linetype="Number of inputs", shape="Number of inputs") +
  facet_grid(eventrate ~ wmperiod, scales = "free_y") +
  theme_bw() +
  theme(legend.position = "bottom")
print(plot)
tikz(file = "mode4_plot.tex", width = 6.5, height = 4)
plot_overall_labels(plot, "Event rate", "$T_{\\mathit{wm}}$")
dev.off()

#nokia1 grid
nokia_reports = filter(experiments$nokia1_reports, procs != 6)
nokia_reports$acc <- as.numeric(as.character(nokia_reports$acc))
nokia_reports$procs <- as.integer(as.character(nokia_reports$procs))
nokia_reports <- mutate(nokia_reports, formula = str_replace_all(as.character(formula), fixed("_"), "-"))
plot <- ggplot(data = nokia_reports, aes(x=acc, y=maxlatency, group=numsources)) +
  geom_line(aes(linetype=numsources)) +
  geom_point(aes(shape=numsources)) +
  labs(x = "Acceleration", y = "Maximum latency (s)", linetype="Number of inputs", shape="Number of inputs") +
  scale_x_continuous(labels = kformatter) +
  facet_grid(procs ~ formula) +
  coord_cartesian(ylim = c(0.0, 16.5)) +
  theme_bw() +
  theme(legend.position = "bottom")
tikz(file = "nokia1.tex", width = 6.5, height = 7)
plot_overall_labels(plot, "Number of submonitors", "Formula")
dev.off()
print(plot)
#Nokia2 reorder overhead
nokia2_fixed_acc = filter(experiments$nokia2_reports, acc!=7000)
nokia2_fixed_acc$numsources <- as.numeric(as.character(nokia2_fixed_acc$numsources))
plot <- ggplot(data = nokia2_fixed_acc, aes(x = numsources, y = maxlatency, group = reorder)) +
  geom_line(aes(linetype=reorder)) +
  geom_point(aes(shape=reorder)) +
  facet_grid(. ~ acc) +
  geom_errorbar(aes(x=numsources, ymin=min_run, ymax=max_run), position = "dodge", width=0.3, size=0.3, show.legend = FALSE) +
  labs(x = "Number of inputs", y = "Maximum latency (s)", linetype="Reorder", shape="Reorder") +
  theme_bw() +
  theme(legend.position = "bottom")
print(plot)
tikz(file = "nokia2.tex", width = 6, height = 3)
plot_overall_labels(plot, col_label = "Acceleration")
dev.off()

#Nokia3 kafka vs sockets
mints = nokia3_peak %>% group_by(inptype, acc) %>% summarise(mints = min(timestamp)) %>% ungroup()
nokia3_peak_transformed = inner_join(nokia3_peak, mints, by = c("acc", "inptype")) %>%
  mutate(timestamp = timestamp - mints) %>%
  select(-"mints") %>%
  filter(peak < 22.0)
nokia3_peak_transformed$timestamp <- as.integer(as.character(nokia3_peak_transformed$timestamp))
nokia3_peak_transformed$acc <- as.integer(as.character(nokia3_peak_transformed$acc))
plot <- ggplot(data = nokia3_peak_transformed, aes(x=timestamp, y=peak, group=inptype)) +
  geom_line(aes(linetype=inptype)) +
  labs(x = "Time (s)", y = "Peak latency (s)") +
  coord_cartesian(ylim = c(0,20)) +
  facet_grid(. ~ acc, space = "free_x", scales = "free_x") +
  scale_linetype_discrete(name = "Input type", breaks = c("kafka", "sockets"), labels=c("Kafka", "Sockets")) +
  theme_bw() +
  theme(legend.position = "bottom")
tikz(file = "nokia3.tex", width = 6, height = 3)
plot_overall_labels(plot, col_label = "Acceleration")
dev.off()
print(plot)
