from __future__ import annotations

import contextlib
import json
import logging
from functools import cached_property
from typing import TYPE_CHECKING, Optional, Union

import pandas as pd
from cognite.client import CogniteClient

from cognite.powerops.client.shop_result_files import ShopLogFile, ShopResultFile, ShopYamlFile
from cognite.powerops.client.shop_results_compare import ShopResultsCompareAPI
from cognite.powerops.utils.cdf_utils import retrieve_relationships_from_source_ext_id
from cognite.powerops.utils.labels import RelationshipLabels

if TYPE_CHECKING:
    from cognite.powerops import PowerOpsClient
    from cognite.powerops.client.shop_run import ShopRun


logger = logging.getLogger(__name__)


class ShopRunResult:
    def __init__(
        self,
        po_client: PowerOpsClient,
        shop_run: ShopRun,
        cplex_logs: ShopLogFile,
        shop_logs: ShopLogFile,
        post_run: ShopYamlFile,
    ) -> None:
        self._po_client = po_client
        self._shop_run = shop_run
        self._cplex_logs = cplex_logs
        self._shop_logs = shop_logs
        self._post_run = post_run

    @cached_property
    def files(self) -> dict[str, Optional[ShopResultFile]]:
        return {
            "post_run": self._post_run,
            "shop_logs": self._shop_logs,
            "cplex_logs": self._cplex_logs,
        }

    @cached_property
    def objective_function(self) -> ObjectiveFunction:
        return self._po_client.shop.results.get_objective_function(self._shop_run)

    def error_message(self) -> Optional[str]:
        if not self._shop_run.succeeded:
            return "(sample) Error: invalid configuration"
        return None

    @property
    def post_run(self) -> Optional[ShopYamlFile]:
        return self._post_run

    @property
    def cplex_logs(self) -> Optional[ShopLogFile]:
        return self._cplex_logs

    @property
    def shop_logs(self) -> Optional[ShopLogFile]:
        return self._shop_logs

    def __repr__(self):
        return f"<ShopRunResult status={self._shop_run.status}>"


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
        return self._watercourse

    @property
    def data(self) -> dict:
        return self._data

    @property
    def penalty_breakdown(self) -> dict:
        return self._penalty_breakdown

    @property
    def field_definitions_df(self) -> pd.DataFrame:
        return self._field_definitions_df

    def data_as_str(self) -> str:
        return "\n".join([f"{k}= {v}" for k, v in self.data.items()])

    def penalty_breakdown_as_str(self) -> str:
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


class ShopRunResultsAPI:
    def __init__(self, po_client: PowerOpsClient):
        self._po_client = po_client
        self.compare = ShopResultsCompareAPI(po_client)

    def get_for_run(self, shop_run: ShopRun) -> ShopRunResult:
        if shop_run.in_progress:
            raise ValueError("ShopRun not completed.")

        post_run_yaml = None
        cplex_log = None
        shop_log = None

        related_log_files = self._po_client.shop.files.retrieve_related_meta(
            source_external_id=shop_run.shop_run_event.external_id,
            label_ext_id=RelationshipLabels.LOG_FILE,
        )
        for metadata in related_log_files:
            ext_id = metadata.external_id
            if ext_id.endswith(".log") and "cplex" in ext_id:
                cplex_log: Optional[ShopLogFile] = self._po_client.shop.files.retrieve(metadata, ShopLogFile)
            elif ext_id.endswith(".log") and "shop_messages" in ext_id:
                # todo: encoding should be set in the metadata when creating/uploading the file
                # existing files will not have this set
                if not metadata.metadata.get("encoding", False):
                    metadata.metadata["encoding"] = "latin-1"
                shop_log: Optional[ShopLogFile] = self._po_client.shop.files.retrieve(metadata, ShopLogFile)
            elif ext_id.endswith(".yaml"):
                post_run_yaml: Optional[ShopYamlFile] = self._po_client.shop.files.retrieve(metadata, ShopYamlFile)
            else:
                logger.error("Unknown file type")
        return ShopRunResult(self._po_client, shop_run, cplex_log, shop_log, post_run_yaml)

    def get_objective_function(self, shop_run: ShopRun) -> ObjectiveFunction:
        # TODO: ability to retrieve the objective function from the post run yaml file
        cdf: CogniteClient = self._po_client.cdf
        relationships = retrieve_relationships_from_source_ext_id(
            cdf,
            shop_run.shop_run_event.external_id,
            RelationshipLabels.OBJECTIVE_SEQUENCE,
            target_types=["sequence"],
        )
        sequences = cdf.sequences.retrieve_multiple(external_ids=[r.target_external_id for r in relationships])
        for seq in sequences:
            # sometimes the label is given to the non-objective sequence too
            if "objective" not in seq.name.lower():
                continue
            # Data is inserted as a single row DataFrame
            seq_data = cdf.sequences.data.retrieve_dataframe(external_id=seq.external_id, start=0, end=-1).to_dict(
                "records"
            )[0]
            # This shape will always be (72, 7) 72 fields and 7 properties of each field
            column_definitions_df = pd.DataFrame(seq.columns)

            penalty_breakdown = {}
            with contextlib.suppress(Exception):
                penalty_breakdown = json.loads(seq.metadata.get("shop:penalty_breakdown", {}))
                if type(penalty_breakdown) != dict:
                    penalty_breakdown = {}

            return ObjectiveFunction(
                external_id=seq.external_id,
                data=seq_data,
                watercourse=seq.metadata.get("shop:watercourse", ""),
                penalty_breakdown=penalty_breakdown,
                field_definitions_df=column_definitions_df,
            )

        logger.error("Objective function sequence not found")
        raise ValueError("Objective function sequence not found")
