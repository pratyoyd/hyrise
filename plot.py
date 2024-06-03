#!/usr/bin/env python

##
## After running the hyriseUCIHPI binary, you can call this script like that:
##   python3 plot.py match_distribution__EqualWaves__100000000_rows.csv progressive_scan__1_cores__synthetic__1_runs.csv
## First parameter is the CSV with distribution of matches, second is the CSV with runtimes.
##

import matplotlib.pyplot as plt
import os
import pandas as pd
import seaborn as sns
import sys
import time

from matplotlib.gridspec import GridSpec
from matplotlib import ticker


def plot(data_distribution, runtimes, output_file_name):
  sns.set_style("white")

  dis_plot = sns.catplot(data_distribution, kind="bar", x="CHUNK_ID", y="MATCH_COUNT", color="red", height=5.0, aspect=10.0)
  dis_fig = dis_plot._figure
  dis_fig.savefig(f"progressive_plots/data_distribution.pdf") 

  runtimes["RUNTIME_MS"] = runtimes["RUNTIME_NS"].astype(float) / 1000 / 1000
  runtimes["RUNTIME_S"] = runtimes["RUNTIME_MS"] / 1000
  runtimes = runtimes.sort_values(by=["SCAN_TYPE", "SCAN_ID", "RUNTIME_NS"])
  runtimes["CUMU_ROWS_EMITTED"] = runtimes.groupby(["SCAN_TYPE", "SCAN_ID"])["ROWS_EMITTED"].cumsum()

  runtimes["GROUP_KEY"] = runtimes.SCAN_TYPE + runtimes.SCAN_ID.astype(str)

  # print("Plotting runtimes.")
  # runtimes_plot = sns.relplot(runtimes, kind="line", x="RUNTIME_MS", y="CUMU_ROWS_EMITTED", units="GROUP_KEY", hue="SCAN_TYPE", estimator=None, height=10.0, aspect=5.0)
  # runtimes_fig = runtimes_plot._figure
  # runtimes_fig.savefig(f"progressive_plots/runtimes.pdf")


  # print("Test plotting regressed runtimes.")
  # lm_runtimes_plot = sns.lmplot(runtimes,  x="RUNTIME_MS", y="CUMU_ROWS_EMITTED", hue="SCAN_TYPE", order=2, height=10.0, aspect=5.0, scatter_kws={'alpha': 0.1})
  # lm_runtimes_fig = lm_runtimes_plot._figure
  # lm_runtimes_fig.savefig(f"progressive_plots/lm_runtimes.pdf")

  # fig, axes = plt.subplots(ncols=2, nrows=3)

  fig = plt.figure(constrained_layout=True, figsize=(10, 12))
  gs = fig.add_gridspec(4, 2, height_ratios=[2.0, 2.0, 0.5, 2.0])
  ax1 = fig.add_subplot(gs[0, :])
  ax2 = fig.add_subplot(gs[1, :])
  ax3 = fig.add_subplot(gs[2, :])
  ax4 = fig.add_subplot(gs[3, 0])
  ax5 = fig.add_subplot(gs[3, 1])

  plot_ax1 = sns.lineplot(runtimes, x="RUNTIME_MS", y="CUMU_ROWS_EMITTED", units="GROUP_KEY", hue="SCAN_TYPE", estimator=None, ax=ax1, alpha=0.8)
  plot_ax1.legend_.set_title(None)
  ax1.set_title("Scan Runs (all)", fontdict={"fontweight": "bold"})
  ax1.set_ylabel("Rows Emitted")
  ax1.set_xlabel("Runtime (ms)")

  # Going for the second version now. I don't really like it, but it helps a bit to analyze. Nothing for a paper though.
  # runtimes_avg = runtimes.sort_values(by=["SCAN_TYPE", "RUNTIME_NS"]).groupby(["SCAN_TYPE"])[["RUNTIME_MS", "CUMU_ROWS_EMITTED"]].rolling(100, min_periods=1).mean().reset_index()
  runtimes_avg = runtimes.sort_values(by=["SCAN_TYPE", "CUMU_ROWS_EMITTED"]).groupby(["SCAN_TYPE"])[["RUNTIME_MS", "CUMU_ROWS_EMITTED"]].rolling(1000, min_periods=1).mean().reset_index()
  plot_ax2 = sns.lineplot(runtimes_avg, x="RUNTIME_MS", y="CUMU_ROWS_EMITTED", hue="SCAN_TYPE", estimator=None, ax=ax2, alpha=0.8)
  plot_ax2.legend_.set_title(None)
  ax2.set_title("Scan Runs (Rolling Average)", fontdict={"fontweight": "bold"})
  ax2.set_ylabel("Rows Emitted")
  ax2.set_xlabel("Runtime (ms)")

  # sns.regplot(runtimes, x="RUNTIME_MS", y="CUMU_ROWS_EMITTED", hue="SCAN_TYPE", order=2, height=10.0, aspect=5.0, scatter_kws={'alpha': 0.1}, ax=ax2)
  sns.lineplot(data_distribution, x="CHUNK_ID", y="MATCH_COUNT", color="gray", ax=ax3)
  ax3.fill_between(data_distribution.CHUNK_ID.values, data_distribution.MATCH_COUNT.values, color="gray")
  ax3.set_title("Match Distribution", fontdict={"fontweight": "bold"})
  ax3.xaxis.set_major_locator(plt.MaxNLocator(10))
  ax3.set_ylabel("Matches")
  ax3.set_xlabel("Chunk ID")

  runtimes["COSTS_PROGRESSIVE"] = runtimes.RUNTIME_S * runtimes.ROWS_EMITTED
  costs_total = runtimes.groupby(["SCAN_TYPE", "SCAN_ID"])["RUNTIME_MS"].max().reset_index()
  costs_progressive = runtimes.groupby(["SCAN_TYPE", "SCAN_ID"])["COSTS_PROGRESSIVE"].sum().reset_index()

  axis = 0
  for title, y, y_title, df, ax in [("Cost Metric: Progressive", "COSTS_PROGRESSIVE", "Costs", costs_progressive, ax4),
                           ("Cost Metric: Total Runtime (ms)", "RUNTIME_MS", "Runtime (ms)", costs_total, ax5)]:
    # plot = sns.catplot(df, kind="box", x="SCAN_TYPE", y=y, hue="SCAN_TYPE", height=3.5, aspect=2.0)
    # plot.set(title=title)
    # plot.tick_params(axis="x", labelrotation=45)
    # plot_fig = plot._figure
    # plot_fig.savefig(f"progressive_plots/costs__{title.replace(' ', '_').lower()}.pdf")

    sns.boxplot(data=df, x="SCAN_TYPE", y=y, hue="SCAN_TYPE", ax=ax)
    ax.set_title(title, fontdict={"fontweight": "bold"})
    ax.tick_params("x", labelrotation=45)
    ax.set_xticks(ax.get_xticks())  # Silence warning: https://stackoverflow.com/a/68794383/1147726
    ax.set_xticklabels([x.get_text().replace(" ", "\n", 1) for x in ax.get_xticklabels()])
    ax.xaxis.get_label().set_visible(False)
    ax.set_ylabel(y_title)
    axis += 1
    
  fig.tight_layout()
  fig.savefig(f"progressive_plots/{output_file_name}__{int(time.time())}.pdf")
  # fig.savefig(f"progressive_plots/overview.pdf")


if __name__ == '__main__':
    assert len(sys.argv) == 3, "Expected two arguments: (i) csv of match distributions and (ii) runtime measurements"

    input_file_name = sys.argv[2]
    output_file_name = f"overview__{input_file_name[input_file_name.find('__')+2:input_file_name.rfind('.csv')]}"

    data_distribution = pd.read_csv(sys.argv[1])
    runtimes = pd.read_csv(input_file_name)

    plot(data_distribution, runtimes, output_file_name)