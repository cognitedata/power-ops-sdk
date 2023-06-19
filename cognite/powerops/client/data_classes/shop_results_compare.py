from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional, Sequence

import matplotlib.pyplot as plt

from cognite.powerops.client.data_classes.shop_result_files import ShopYamlFile
from cognite.powerops.utils.plotting import ax_plot_time_series, create_time_series_plot

logger = logging.getLogger(__name__)


class ShopResultsCompare:
    def plot_time_series(
        self,
        post_run_list: Sequence[ShopYamlFile],
        comparison_key: str,
        labels: Optional[Sequence[str]] = None,
    ):
        """Stacked line charts of the given post runs at the same key.
        Labels must be in the same order as the post runs."""
        if labels and (len(labels) != len(set(labels)) or len(labels) != len(post_run_list)):
            logger.error("Titles must be unique and match the number of provided post runs")
            return

        plots: dict[str, dict[datetime, float]] = {}

        for i, shop_yaml in enumerate(post_run_list):
            if time_series := shop_yaml._retrieve_time_series_dict(comparison_key):
                plot_label = labels[i] if labels else shop_yaml.name
                plots[plot_label] = time_series

        ax = create_time_series_plot()
        for label, time_series in plots.items():
            ax_plot_time_series(ax, time_series, label)
        ax.legend()
        plt.show()

    def yaml_difference(
        self,
        yaml_1: ShopYamlFile,
        yaml_2: ShopYamlFile,
    ):
        raise NotImplementedError()
