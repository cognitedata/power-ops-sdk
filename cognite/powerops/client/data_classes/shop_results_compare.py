from __future__ import annotations

import logging
from datetime import datetime
from pprint import pformat
from typing import Optional, Sequence

import matplotlib.pyplot as plt
from deepdiff import DeepDiff

from cognite.powerops.client.data_classes.helpers import format_deep_diff_path, get_data_from_nested_dict
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

    def yaml_deep_diff(
        self,
        post_run_yaml_1: ShopYamlFile,
        post_run_yaml_2: ShopYamlFile,
        **kwargs,
    ) -> str:
        """DeepDiff wrapper for ShopYamlFile objects."""
        return DeepDiff(post_run_yaml_1.data, post_run_yaml_2.data, **kwargs)

    def yaml_difference_md(
        self,
        post_run_yaml_1: Optional[ShopYamlFile] = None,
        post_run_yaml_2: Optional[ShopYamlFile] = None,
        names: Optional[Sequence[str]] = None,
    ):
        """
        Returns a markdown string of the difference between two post run yaml files
        In a notebook, this can be displayed using
        ```
        from IPython.display import Markdown

        Markdown(yaml_difference_md(...))
        ```
        """
        if post_run_yaml_1 is None or post_run_yaml_2 is None:
            logger.error("Must provide two post run yaml files ")
            return ""
        if names and len(names) != 2:
            logger.error("Please provide two names")
            return ""

        if not names:
            # The file names are of the form "post-run-<uuid>.yaml"
            # We use the first section of the uuid as the name
            names = (
                post_run_yaml_1.name.split("-")[2],
                post_run_yaml_2.name.split("-")[2],
            )
        yaml_deep_diff = self.yaml_deep_diff(post_run_yaml_1, post_run_yaml_2)
        builder = YamlDiffMDBuilder(
            yaml_deep_diff,
            post_run_yaml_1.data,
            post_run_yaml_2.data,
            *names,
        )

        output_string = f"# Changes from {names[0]} to {names[1]}"
        output_string += "\n\n"
        output_string += builder.build_md()
        return output_string


class YamlDiffMDBuilder:
    def __init__(
        self,
        deep_diff: DeepDiff,
        yaml_dict_1: dict,
        yaml_dict_2: dict,
        name_1: str,
        name_2: str,
    ):
        self.deep_diff = deep_diff
        self.yaml_dict_1 = yaml_dict_1
        self.yaml_dict_2 = yaml_dict_2
        self.name_1 = name_1
        self.name_2 = name_2
        self.md_string = None

    def _add_modification(self, modifications):
        self.md_string += f"\n### Items in {self.name_2} which are not in {self.name_1}:"
        for location in modifications:
            data = get_data_from_nested_dict(self.yaml_dict_2, location)
            self.md_string += format_deep_diff_path(location) + "\n" + pformat(data) + "\n"

    def _remove_modification(self, modifications):
        self.md_string += f"\n### Items in {self.name_1} which are not in {self.name_2}:"
        for location in modifications:
            data = get_data_from_nested_dict(self.yaml_dict_1, location)
            self.md_string += format_deep_diff_path(location) + "\n" + pformat(data) + "\n"

    def _changed_modifications(self, modifications):
        self.md_string += f"\n### {self.name_1}(top) and {self.name_2}(bottom)\n"
        self.md_string += "have different values for the following keys:"
        seen_parents = set()

        for location, changes in modifications.items():
            parent_location = f'{"][".join(location.split("][")[:-1])}]'
            if parent_location in seen_parents:
                continue

            try:
                parent_data = get_data_from_nested_dict(self.yaml_dict_1, parent_location)
            except KeyError:
                parent_data = None

            if isinstance(parent_data, list):
                seen_parents.add(parent_location)
                self.md_string += f"\n{parent_location.replace('root', '')}:\n"
                try:
                    self.md_string += get_data_from_nested_dict(self.yaml_dict_1, parent_location)
                    self.md_string += get_data_from_nested_dict(self.yaml_dict_2, parent_location)
                except KeyError:
                    continue
            else:
                self.md_string += f"{format_deep_diff_path(location)}:"
                self.md_string += pformat(changes["old_value"]) + "\n" + pformat(changes["new_value"]) + "\n"

    def build_md(self) -> str:
        self.md_string = ""
        for modification_type, modifications in self.deep_diff.items():
            if "set" in modification_type:
                # Unsure if the YAML parser will return sets at all
                logger.error("Cannot handle sets")

            elif "add" in modification_type:
                string_builder_func = self._add_modification

            elif "remove" in modification_type:
                string_builder_func = self._remove_modification
            elif "change" in modification_type:
                string_builder_func = self._changed_modifications

            else:
                logger.error(f"Could not handle modification_type {modification_type}")
                continue

            string_builder_func(modifications)
        return self.md_string
