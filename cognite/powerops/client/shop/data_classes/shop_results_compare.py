from __future__ import annotations

import contextlib
import logging
from collections.abc import Sequence
from datetime import datetime
from pprint import pformat
from typing import Callable, Optional

import matplotlib.pyplot as plt
from deepdiff import DeepDiff
from deepdiff.model import PrettyOrderedSet

from cognite.powerops.client.shop.data_classes.helpers import (
    format_deep_diff_path,
    get_data_from_nested_dict,
    is_time_series_dict,
)
from cognite.powerops.client.shop.data_classes.plotting import ax_plot_time_series, create_time_series_plot
from cognite.powerops.client.shop.data_classes.shop_result_files import ShopYamlFile

logger = logging.getLogger(__name__)


class ShopResultsCompare:
    def plot_time_series(
        self, post_run_list: Sequence[ShopYamlFile], comparison_key: str, labels: Optional[Sequence[str]] = None
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

    def yaml_deep_diff(self, post_run_yaml_1: ShopYamlFile, post_run_yaml_2: ShopYamlFile) -> DeepDiff:
        """DeepDiff wrapper for ShopYamlFile objects."""
        return DeepDiff(post_run_yaml_1.data, post_run_yaml_2.data, ignore_type_in_groups=[(int, float)])

    def yaml_difference_md(
        self,
        post_run_yaml_a: ShopYamlFile = None,
        post_run_yaml_b: ShopYamlFile = None,
        name_a: str = "Result A",
        name_b: str = "Result B",
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
            raise ValueError("Must provide two post run yaml files")

        yaml_deep_diff = self.yaml_deep_diff(post_run_yaml_a, post_run_yaml_b)
        builder = _YamlDiffMDBuilder(yaml_deep_diff, post_run_yaml_a.data, post_run_yaml_b.data, name_a, name_b)

        output_string = f"# Changes from {name_a} to {name_b}"
        output_string += "\n\n"
        output_string += builder.build_md()
        return output_string


class _YamlDiffMDBuilder:
    def __init__(self, deep_diff: DeepDiff, yaml_dict_a: dict, yaml_dict_b: dict, name_a: str, name_b: str):
        self.deep_diff = deep_diff
        self.yaml_dict_a = yaml_dict_a
        self.yaml_dict_b = yaml_dict_b
        self.name_a = name_a
        self.name_b = name_b
        self.md_string_builder: list = None

    def _loop_ordered_set_diff(self, diffs: PrettyOrderedSet, yaml_lookup: dict, yaml_name: str):
        seen_parents = set()
        time_series_keys = set()
        list_keys = set()
        other_value_keys = set()

        for location in diffs:
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
            self.md_string_builder.append("#### Value Items: \n")
            for key in other_value_keys:
                self.md_string_builder.append(f" * `{key}`\n")
                self.md_string_builder.append(pformat(get_data_from_nested_dict(yaml_lookup, location)))
                self.md_string_builder.append("\n")
            self.md_string_builder.append("\n")

        if list_keys:
            self.md_string_builder.append("#### Lists: \n")
            for key in list_keys:
                self.md_string_builder.append(f" - `{key}`\n")
                self.md_string_builder.append("\n    - ".join(get_data_from_nested_dict(yaml_lookup, key)))
                self.md_string_builder.append("\n")
            self.md_string_builder.append("\n")

        if time_series_keys:
            self.md_string_builder.append("#### Time series:  \n")
            self.md_string_builder.append(f"Use `post_run.plot(key)` on {yaml_name} with one of the following keys \n")
            for key in time_series_keys:
                self.md_string_builder.append(f" - `{format_deep_diff_path(key)}`")
                self.md_string_builder.append("\n")
            self.md_string_builder.append("\n")

    def _loop_dict_diff(self, diff: dict, yaml_dict_lookup: dict, yaml_names: tuple[str, str]):
        seen_parents = set()
        time_series_keys = set()
        list_keys = set()
        other_values = {}

        for location, changes in diff.items():
            with contextlib.suppress(TypeError):
                # Ignore changes to temporary file paths. Not all changes are iterable
                if "/tmp/" in changes["old_value"] and "/tmp/" in changes["new_value"]:
                    continue
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
                other_values[format_deep_diff_path(location)] = changes

        if other_values:
            self.md_string_builder.append("#### Values: \n")
            for key, changes in other_values.items():
                self.md_string_builder.append(f" - `{key}`\n")
                self.md_string_builder.append(f"    - {pformat(changes['old_value'])}\n")
                self.md_string_builder.append(f"    - {pformat(changes['new_value'])}\n")
            self.md_string_builder.append("\n")

        if list_keys:
            self.md_string_builder.append("#### Lists: \n")
            for key in list_keys:
                self.md_string_builder.append(f" - `{key}`\n")

                self.md_string_builder.append(f"    - **{yaml_names[0]}** \n")
                self.md_string_builder.append("\n      - ".join(get_data_from_nested_dict(self.yaml_dict_a, key)))
                self.md_string_builder.append("\n")

                self.md_string_builder.append(f"    - **{yaml_names[1]}** \n")
                self.md_string_builder.append("\n      - ".join(get_data_from_nested_dict(self.yaml_dict_b, key)))
                self.md_string_builder.append("\n")

            self.md_string_builder += "\n"

        if time_series_keys:
            self.md_string_builder.append("#### Time series:\n")
            self.md_string_builder.append(f"Use `powerops.shop.results.compare.plot_time_series({yaml_names}, key)` ")
            self.md_string_builder.append(" on with one of the following keys \n")
            for key in time_series_keys:
                self.md_string_builder.append(f" * `{format_deep_diff_path(key)}`")
                self.md_string_builder.append("\n")

            self.md_string_builder.append("\n")

    def build_md(self) -> str:
        self.md_string_builder = []
        loop_modifications: Callable[[str, PrettyOrderedSet | dict], None] = None

        for modification_type, modifications in self.deep_diff.items():
            if "set" in modification_type:
                # Unsure if the YAML parser will return sets at all
                logger.error("Cannot handle sets")
                continue

            elif "add" in modification_type:
                self.md_string_builder.append(f"\n## Items in {self.name_b} which are not in {self.name_a}:\n")
                yaml_dict_lookup = self.yaml_dict_b
                yaml_name = self.name_b
                loop_modifications = self._loop_ordered_set_diff

            elif "remove" in modification_type:
                self.md_string_builder.append(f"\n## Items in {self.name_a} which are not in {self.name_b}:\n")
                yaml_dict_lookup = self.yaml_dict_a
                yaml_name = self.name_a
                loop_modifications = self._loop_ordered_set_diff

            elif "change" in modification_type:
                self.md_string_builder.append(
                    f"\n## Items that are both in {self.name_a} (top) and {self.name_b} (bottom) "
                )
                self.md_string_builder.append("but are different: \n")
                yaml_dict_lookup = self.yaml_dict_a
                yaml_name = self.name_a, self.name_b
                loop_modifications = self._loop_dict_diff

            else:
                logger.error(f"Could not handle modification_type {modification_type}")
                continue

            loop_modifications(modifications, yaml_dict_lookup, yaml_name)
        return "".join(self.md_string_builder)
