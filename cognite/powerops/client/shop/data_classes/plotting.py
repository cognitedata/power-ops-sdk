from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.axes import Axes


def create_time_series_plot() -> Axes:
    fig, ax = plt.subplots(figsize=(10, 10))
    fig.autofmt_xdate()
    return ax


def ax_plot_time_series(
    ax: Axes,
    time_series: dict[datetime, float],
    label: str,
):
    ax.plot(
        time_series.keys(),
        time_series.values(),
        linestyle="-",
        marker=".",
        label=label,
        alpha=0.7,
    )
    ax.xaxis.set_major_formatter(
        mdates.DateFormatter("%d. %b %y %H:%M"),
    )
