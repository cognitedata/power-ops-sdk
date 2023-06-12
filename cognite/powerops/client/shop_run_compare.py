from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional, Sequence

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.axes import Axes

from cognite.powerops.client.shop_result_files import ShopYamlFile

if TYPE_CHECKING:
    from cognite.powerops import PowerOpsClient

logger = logging.getLogger(__name__)


class ShopRunCompareAPI:
    def __init__(self, po_client: PowerOpsClient):
        self._po_client = po_client

    def _ax_plot(self, ax: Axes, time_series: dict, label: str):
        ax.plot(time_series.keys(), time_series.values(), linestyle="-", marker=".", label=label)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d. %b %y %H:%M"))

    def time_series_plots(
        self, post_run_list: Sequence[ShopYamlFile], comparison_key: str, labels: Optional[Sequence[str]] = ()
    ):
        """Stacked line charts of the given post runs at the same key.
        Labels must be in the same order as the post runs."""
        if labels and (len(labels) != len(set(labels)) or len(labels) != len(post_run_list)):
            logger.error("Titles mus be unique and match the number of provided post runs")
            return
        print("post_run_list")

        plots: dict[str, dict[datetime, float]] = {}

        for i, shop_yaml in enumerate(post_run_list):
            if time_series := shop_yaml._retrieve_time_series_dict(comparison_key):
                plot_label = labels[i] if labels else shop_yaml.name
                plots[plot_label] = time_series

        fig, ax = plt.subplots(figsize=(10, 10))
        fig.autofmt_xdate()
        for label, time_series in plots.items():
            self._ax_plot(ax, time_series, label)
        ax.legend()
        plt.show()
