from __future__ import annotations

import logging
from collections.abc import Callable
from functools import cached_property
from typing import TYPE_CHECKING, Optional, Union

import pandas as pd

from cognite.powerops.client.shop.data_classes.shop_result_files import SHOPLogFile, SHOPResultFile, SHOPYamlFile

if TYPE_CHECKING:
    from cognite.powerops.client.shop.data_classes import SHOPRun

logger = logging.getLogger(__name__)


class SHOPRunResult:
    def __init__(
        self,
        retrieve_objective_function: Callable[[SHOPRun], ObjectiveFunction],
        shop_run: SHOPRun,
        cplex: SHOPLogFile,
        shop_messages: SHOPLogFile,
        post_run: SHOPYamlFile,
    ) -> None:
        self._retrieve_objective_function = retrieve_objective_function
        self._shop_run = shop_run
        self._cplex = cplex
        self._shop_messages = shop_messages
        self._post_run = post_run

    @cached_property
    def files(self) -> dict[str, Optional[SHOPResultFile]]:
        """
        Returns a dictionary of the files associated with the shop run.

        The dictionary keys are:
            - post_run
            - shop_messages
            - cplex

        Returns:
            Dictionary of files associated with the shop run.
        """

        return {"post_run": self._post_run, "shop_messages": self._shop_messages, "cplex": self._cplex}

    @cached_property
    def objective_function(self) -> ObjectiveFunction:
        """
        Returns the objective function of the shop run.

        Returns:
            The objective function of the shop run.
        """
        return self._retrieve_objective_function(self._shop_run)

    def error_message(self) -> str | None:
        """
        Returns the error message of the shop run.

        Returns:
            The error message of the shop run.
        """
        if not self._shop_run.succeeded:
            return "(sample) Error: invalid configuration"
        return None

    @property
    def post_run(self) -> Optional[SHOPYamlFile]:
        """
        Returns the post run yaml file of the shop run.

        Returns:
            The post run yaml file of the shop run.
        """
        return self._post_run

    @property
    def cplex(self) -> SHOPLogFile | None:  # TODO rename to cplex_messages? _logs?
        """
        Returns the cplex log file of the shop run.

        Returns:
            The cplex log file of the shop run.
        """
        return self._cplex

    @property
    def shop_messages(self) -> Optional[SHOPLogFile]:  # TODO rename to _logs?
        """
        Returns the shop messages log file of the shop run.

        Returns:
            The shop messages log file of the shop run.
        """
        return self._shop_messages

    def __repr__(self):
        return f"<SHOPRunResult status={self._shop_run.status}>"


class ObjectiveFunction:
    def __init__(
        self,
        external_id: str,
        data: dict,
        watercourse: str,
        penalty_breakdown: dict,
        field_definitions_df: pd.DataFrame,
    ) -> None:
        self._external_id = external_id
        self._data = data
        self._watercourse = watercourse
        self._penalty_breakdown = penalty_breakdown
        self._field_definitions_df = field_definitions_df

    def __repr__(self) -> str:
        return f"<OBJECTIVE sequence_external_id={self._external_id}>"

    @property
    def watercourse(self) -> str:
        """
        Returns the watercourse of the objective function.

        Returns:
            The watercourse of the objective function.
        """
        return self._watercourse

    @property
    def data(self) -> dict:
        """
        Returns the data of the objective function.

        Returns:
            The data of the objective function.
        """
        return self._data

    @property
    def penalty_breakdown(self) -> dict:
        """
        Returns the penalty breakdown of the objective function.

        Returns:
            The penalty breakdown of the objective function.
        """
        return self._penalty_breakdown

    @property
    def field_definitions_df(self) -> pd.DataFrame:
        """
        The field definitions of the objective function.

        Returns:
            The field definitions of the objective function.
        """
        return self._field_definitions_df

    def data_as_str(self) -> str:
        """
        Returns the data of the objective function as a string representation.

        Returns:
            String representation of the data of the objective function.

        """
        return "\n".join([f"{k}= {v}" for k, v in self.data.items()])

    def penalty_breakdown_as_str(self) -> str:
        """
        Returns the penalty breakdown of the objective function as a string.

        Returns:
            Markdown string of the penalty breakdown.
        """

        # return the penalty breakdown as markdown string some time in future?
        separator = "--------------------------\n"

        def breakdown_inner_dict(key):
            if inner_dict := self.penalty_breakdown.get(key):
                title = f"Breakdown of {key} penalties\n{separator}"
                return title + _inner_string_builder(inner_dict)
            else:
                return ""

        def _inner_string_builder(dictionary: dict[str, Union[str, dict]]) -> str:
            _base = ""
            for key, value in dictionary.items():
                # Assumes only one level of nesting
                if isinstance(value, dict):
                    _base += f" - {key.capitalize()}\n"
                    _base += _inner_string_builder(value)
                else:
                    _base += f"  * {format_dict_key(key)}: {value}\n"
            return _base

        def format_dict_key(key: str) -> str:
            return key.replace("_", " ").replace(".", " ").replace("-", " ").capitalize()

        sum_penalties = self.penalty_breakdown.get("sum_penalties")
        major_penalties = self.penalty_breakdown.get("major_penalties")
        minor_penalties = self.penalty_breakdown.get("minor_penalties")

        penalty_as_string = f"Sum penalties: {sum_penalties} |"
        penalty_as_string += f"Major penalties: {major_penalties} |"
        penalty_as_string += f"Minor penalties: {minor_penalties}\n"
        penalty_as_string += separator.replace("-", "=")

        penalty_as_string += breakdown_inner_dict("minor")
        penalty_as_string += breakdown_inner_dict("major")
        penalty_as_string += breakdown_inner_dict("unknown")

        return penalty_as_string
