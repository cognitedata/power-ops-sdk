from __future__ import annotations

import logging
from datetime import datetime
from pprint import pformat
from typing import Callable, Optional, Sequence

import matplotlib.pyplot as plt
from deepdiff import DeepDiff
from deepdiff.model import PrettyOrderedSet

from cognite.powerops.client.data_classes.helpers import (
    format_deep_diff_path,
    get_data_from_nested_dict,
    is_time_series_dict,
)
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
        **deepdiff_kwargs,
    ) -> str:
        """DeepDiff wrapper for ShopYamlFile objects."""
        return DeepDiff(
            post_run_yaml_1.data,
            post_run_yaml_2.data,
            ignore_type_in_groups=[(int, float)],
            **deepdiff_kwargs,
        )

    def yaml_difference_md(
        self,
        post_run_yaml_a: Optional[ShopYamlFile] = None,
        post_run_yaml_b: Optional[ShopYamlFile] = None,
        name_a: str = "Result A",
        name_b: str = "Result B",
        **deepdiff_kwargs,
    ):
        """
        Returns a markdown string of the difference between two post run yaml files
        In a notebook, this can be displayed using
        ```
        from IPython.display import Markdown

        Markdown(yaml_difference_md(...))
        ```
        """
        if post_run_yaml_a is None or post_run_yaml_b is None:
            logger.error("Must provide two post run yaml files ")
            return ""

        yaml_deep_diff = self.yaml_deep_diff(post_run_yaml_a, post_run_yaml_b)
        builder = YamlDiffMDBuilder(
            yaml_deep_diff, post_run_yaml_a.data, post_run_yaml_b.data, name_a, name_b, **deepdiff_kwargs
        )

        output_string = f"# Changes from {name_a} to {name_b}"
        output_string += "\n\n"
        output_string += builder.build_md()
        return output_string


class YamlDiffMDBuilder:
    def __init__(
        self,
        deep_diff: DeepDiff,
        yaml_dict_a: dict,
        yaml_dict_b: dict,
        name_a: str,
        name_b: str,
    ):
        self.deep_diff = deep_diff
        self.yaml_dict_a = yaml_dict_a
        self.yaml_dict_b = yaml_dict_b
        self.name_a = name_a
        self.name_b = name_b
        self.md_string = None

    def _loop_add_remove_modifications(
        self,
        modifications: PrettyOrderedSet,
        yaml_lookup: dict,
        yaml_name: str,
    ):
        seen_parents = set()
        time_series_keys = set()
        list_keys = set()
        other_value_keys = set()

        for location in modifications:
            parent_location = f'{"][".join(location.split("][")[:-1])}]'
            if parent_location in seen_parents:
                continue
            try:
                parent_data = get_data_from_nested_dict(yaml_lookup, parent_location)
            except KeyError:
                parent_data = None
            if isinstance(parent_data, list):
                seen_parents.add(parent_location)
                list_keys.add(format_deep_diff_path(parent_location))

            elif is_time_series_dict(parent_data):
                seen_parents.add(parent_location)
                time_series_keys.add(format_deep_diff_path(parent_location))
            else:
                other_value_keys.add(format_deep_diff_path(location))

        if other_value_keys:
            self.md_string += "#### The following are **values**: \n"
            for key in other_value_keys:
                self.md_string += f" * `{key}`\n"
                self.md_string += pformat(get_data_from_nested_dict(yaml_lookup, location))
                self.md_string += "\n"
            self.md_string += "\n"

        if list_keys:
            self.md_string += "#### The following are **lists**: \n"
            for key in list_keys:
                self.md_string += f" - `{key}`\n"
                self.md_string += "\n    - ".join(get_data_from_nested_dict(self.yaml_dict_b, key))
                self.md_string += "\n"
                self.md_string += pformat(get_data_from_nested_dict(yaml_lookup, location))
                self.md_string += "\n"
            self.md_string += "\n"

        if time_series_keys:
            self.md_string += "#### The following are **time series** which can be plotted. \n"
            self.md_string += f"Use `post_run.plot(key)` on {yaml_name} with one of the following keys \n"
            for key in time_series_keys:
                self.md_string += f" - `{format_deep_diff_path(key)}`"
                self.md_string += "\n"
            self.md_string += "\n"

    def _loop_changed_modifications(
        self,
        modifications: dict,
        yaml_dict_lookup: dict,
        yaml_names: tuple[str, str],
    ):
        seen_parents = set()
        time_series_keys = set()
        list_keys = set()
        other_value_keys = {}

        for location, changes in modifications.items():
            parent_location = f'{"][".join(location.split("][")[:-1])}]'
            if parent_location in seen_parents:
                continue
            try:
                parent_data = get_data_from_nested_dict(yaml_dict_lookup, parent_location)
            except KeyError:
                parent_data = None

            if isinstance(parent_data, list):
                seen_parents.add(parent_location)
                list_keys.add(format_deep_diff_path(parent_location))

            elif is_time_series_dict(parent_data):
                seen_parents.add(parent_location)
                time_series_keys.add(format_deep_diff_path(parent_location))

            else:
                other_value_keys[format_deep_diff_path(location)] = changes

        if other_value_keys:
            self.md_string += "#### The following **values** have changed: \n"
            for key, changes in other_value_keys.items():
                self.md_string += f" - `{key}`\n"
                self.md_string += f"    - {pformat(changes['old_value'])}\n"
                self.md_string += f"    - {pformat(changes['new_value'])}\n"
            self.md_string += "\n"

        if list_keys:
            self.md_string += "#### The following are **lists** which changed: \n"
            for key in list_keys:
                self.md_string += f" - `{key}`\n"

                self.md_string += f"    - **{yaml_names[0]}** \n"
                self.md_string += "\n      - ".join(get_data_from_nested_dict(self.yaml_dict_a, key))
                self.md_string += "\n"

                self.md_string += f"    - **{yaml_names[1]}** \n"
                self.md_string += "\n      - ".join(get_data_from_nested_dict(self.yaml_dict_b, key))
                self.md_string += "\n"
            self.md_string += "\n"

        if time_series_keys:
            self.md_string += "#### The following are **time series** which changed.\n"
            self.md_string += f"Use `powerops.shop.results.compare.plot_time_series({yaml_names}, key)` "
            self.md_string += " on with one of the following keys \n"
            for key in time_series_keys:
                self.md_string += f" * `{format_deep_diff_path(key)}`"
                self.md_string += "\n"

            self.md_string += "\n"

    def build_md(self) -> str:
        self.md_string = ""
        loop_modifications: Callable[[str, PrettyOrderedSet | dict], None] = None

        for modification_type, modifications in self.deep_diff.items():
            if "set" in modification_type:
                # Unsure if the YAML parser will return sets at all
                logger.error("Cannot handle sets")
                continue

            elif "add" in modification_type:
                self.md_string += f"\n## Items in {self.name_b} which are not in {self.name_a}:\n"
                yaml_dict_lookup = self.yaml_dict_b
                yaml_name = self.name_b
                loop_modifications = self._loop_add_remove_modifications

            elif "remove" in modification_type:
                self.md_string += f"\n## Items in {self.name_a} which are not in {self.name_b}:\n"
                yaml_dict_lookup = self.yaml_dict_a
                yaml_name = self.name_a
                loop_modifications = self._loop_add_remove_modifications

            elif "change" in modification_type:
                self.md_string += f"\n## Items that are both in {self.name_a} (top) and {self.name_b} (bottom) "
                self.md_string += "but are different: \n"
                yaml_dict_lookup = self.yaml_dict_a
                yaml_name = self.name_a, self.name_b
                loop_modifications = self._loop_changed_modifications

            else:
                logger.error(f"Could not handle modification_type {modification_type}")
                continue

            loop_modifications(modifications, yaml_dict_lookup, yaml_name)
        return self.md_string
