import matplotlib.dates as mdates
from matplotlib.axes import Axes


def ax_plot_time_time_series(ax: Axes, time_series: dict, label: str):
    ax.plot(time_series.keys(), time_series.values(), linestyle="-", marker=".", label=label)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d. %b %y %H:%M"))
