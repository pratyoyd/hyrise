#!/usr/bin/env python

##
## After running the hyriseUCIHPI binary, you can call this script like that:
##   python3 plot.py match_distribution__EqualWaves__100000000_rows.csv progressive_scan__1_cores__synthetic__1_runs.csv
## First parameter is the CSV with distribution of matches, second is the CSV with runtimes.
##

import math
import matplotlib.pyplot as plt
import os
import pandas as pd
import seaborn as sns
import sys
import time

from matplotlib.gridspec import GridSpec
from matplotlib import ticker


def plot(data_distribution, runtimes, output_file_name_suffix):
  timestamp = int(time.time())
  sns.set_style("white")

  # just a helper
  runtimes.insert(0, "ROW_ID", range(0, len(runtimes)))

  runtimes["RUNTIME_MS"] = runtimes["RUNTIME_NS"].astype(float) / 1000 / 1000
  runtimes["RUNTIME_S"] = runtimes["RUNTIME_MS"] / 1000
  runtimes = runtimes.sort_values(by=["SCAN_TYPE", "SCAN_ID", "RUNTIME_NS"])
  runtimes["CUMU_ROWS_EMITTED"] = runtimes.groupby(["SCAN_TYPE", "SCAN_ID"])["ROWS_EMITTED"].cumsum()

  runtimes["GROUP_KEY"] = runtimes.SCAN_TYPE + runtimes.SCAN_ID.astype(str)

  ##
  ## Heat map bar chart: shows when chunk matches have been emitted
  ##

  scan_approach_count = len(pd.unique(runtimes.SCAN_TYPE)) - 1  # We don't plot the "traditional" Hyrise scan
  run_id_to_plot = math.ceil(runtimes.SCAN_ID.max() / 2)

  chunk_count_estimated = runtimes.groupby("SCAN_TYPE").count().max().ROW_ID / runtimes.SCAN_ID.max()
  width = max(10, chunk_count_estimated / 1_000 * 20)
  fig = plt.figure(constrained_layout=True, figsize=(width, 15))
  grid_spec = fig.add_gridspec(scan_approach_count, 1)
  axes = []
  for approach_id in range(scan_approach_count):
    axes.append(fig.add_subplot(grid_spec[approach_id]))

  sns.set_context(rc = {'patch.linewidth': 0.0})

  # We could probably use a FacetPlot here, but now it's too late.
  for approach_id, approach_name in enumerate(pd.unique(runtimes.SCAN_TYPE)):
    if "traditional" in approach_name.lower():
      continue

    df = runtimes.query("SCAN_TYPE == @approach_name and SCAN_ID == @run_id_to_plot").copy()
    df = df.sort_values("ROW_ID")
    df.insert(0, "CHUNK_ID", range(0, len(df)))
    # df = df.query("CHUNK_ID <= 800")
    sns.barplot(df, x="CHUNK_ID", y="ROWS_EMITTED", hue="RUNTIME_MS", palette="rocket", ax=axes[approach_id], width=0.95, legend=(approach_id == 0))
    axes[approach_id].set_title(approach_name, fontdict={"fontweight": "bold"})
    if approach_id == 0:
      axes[approach_id].legend_.set_title("Runtime (ms)")
    axes[approach_id].set_xlabel("Chunk ID")
    axes[approach_id].set_ylabel("Rows Emitted")
    axes[approach_id].xaxis.set_major_locator(plt.MaxNLocator(10))

  fig.savefig(f"progressive_plots/time_of_rows_emitted__{output_file_name_suffix}__{timestamp}.pdf")


  ##
  ## Overview plot
  ##

  fig = plt.figure(constrained_layout=True, figsize=(10, 12))
  grid_spec = fig.add_gridspec(4, 2, height_ratios=[2.0, 2.0, 0.5, 2.0])
  ax1 = fig.add_subplot(grid_spec[0, :])
  ax2 = fig.add_subplot(grid_spec[1, :])
  ax3 = fig.add_subplot(grid_spec[2, :])
  ax4 = fig.add_subplot(grid_spec[3, 0])
  ax5 = fig.add_subplot(grid_spec[3, 1])

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
    sns.boxplot(data=df, x="SCAN_TYPE", y=y, hue="SCAN_TYPE", ax=ax)
    ax.set_title(title, fontdict={"fontweight": "bold"})
    ax.tick_params("x", labelrotation=45)
    ax.set_xticks(ax.get_xticks())  # Silence warning: https://stackoverflow.com/a/68794383/1147726
    ax.set_xticklabels([x.get_text().replace(" ", "\n", 1) for x in ax.get_xticklabels()])
    ax.xaxis.get_label().set_visible(False)
    ax.set_ylabel(y_title)
    axis += 1

  fig.savefig(f"progressive_plots/overview__{output_file_name_suffix}__{timestamp}.pdf")
  # fig.savefig(f"progressive_plots/overview.pdf")


if __name__ == '__main__':
    assert len(sys.argv) == 3, "Expected two arguments: (i) csv of match distributions and (ii) runtime measurements"

    input_file_name = sys.argv[2]
    output_file_name_suffix = f"{input_file_name[input_file_name.find('__')+2:input_file_name.rfind('.csv')]}"

    data_distribution = pd.read_csv(sys.argv[1])
    runtimes = pd.read_csv(input_file_name)

    plot(data_distribution, runtimes, output_file_name_suffix)
