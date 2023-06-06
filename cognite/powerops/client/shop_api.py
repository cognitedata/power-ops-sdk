from __future__ import annotations

import abc
import contextlib
import json
import logging
import os
import random
import tempfile
import time
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Generic, Literal, Optional, Sequence, TextIO, TypeVar, Union

import pandas as pd
import requests
import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import Event, FileMetadata

from cognite.powerops.case.shop_run_event import ShopRunEvent
from cognite.powerops.utils.cdf_utils import (
    retrieve_event,
    retrieve_relationships_from_source_ext_id,
    simple_relationship,
)
from cognite.powerops.utils.labels import RelationshipLabels

if TYPE_CHECKING:
    from cognite.powerops import Case, PowerOpsClient

logger = logging.getLogger(__name__)


InputFileTypeT = Literal["case", "cut", "mapping", "extra"]


ContentTypeT = TypeVar("ContentTypeT", bound=Union[str, dict])


class ShopResultFile(abc.ABC, Generic[ContentTypeT]):
    """Base class for handling a results file from Shop."""

    def __init__(self, po_client: PowerOpsClient, file_metadata: FileMetadata = None, encoding="utf-8") -> None:
        self._po_client = po_client
        self._file_metadata = file_metadata
        self._encoding = encoding

    @property
    def encoding(self) -> str:
        return self._encoding

    @property
    def external_id(self):
        return self._file_metadata.external_id

    @property
    def name(self):
        return self._file_metadata.name

    @cached_property
    def data(self) -> ContentTypeT:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = self._po_client.shop.files.download(self, tmp_dir)
            with open(tmp_path, "r", encoding=self.encoding) as downloaded_file:
                return self._parse_file(downloaded_file)

    def _parse_file(self, file: TextIO) -> ContentTypeT:
        """Read downloaded file and return data."""
        raise NotImplementedError()

    def save(self, dir_path: str = "") -> str:
        """Save file to local filesystem."""
        file_path = os.path.join(dir_path or os.getcwd(), self._file_metadata.external_id)
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding=self.encoding) as out_file:
            out_file.write(self.file_content)
        return file_path

    @property
    def file_content(self) -> str:
        """Data encoded for writing to local filesystem."""
        raise NotImplementedError()


class ShopLogFile(ShopResultFile[str]):
    """Plain text result file (for SHOP messages and CPlex logs)."""

    def _parse_file(self, file: TextIO) -> str:
        return file.read()

    @property
    def file_content(self) -> str:
        return self.data


class ShopYamlFile(ShopResultFile[dict]):
    """Yaml-formatted results file (for post_run.yaml file created by SHOP)."""

    def _parse_file(self, file: TextIO) -> dict:
        return yaml.safe_load(file)

    @property
    def file_content(self) -> str:
        return yaml.safe_dump(self.data, sort_keys=False)


class ObjectiveFunction:
    def __init__(
        self,
        shop_run_result: ShopRunResult,
        external_id: str,
        data: dict,
        watercourse: str,
        penalty_breakdown: dict,
        field_definitions_df: pd.DataFrame = None,
    ) -> None:
        self._shop_run_result = shop_run_result
        self._external_id = external_id
        self._data = data
        self._watercourse = watercourse
        self._penalty_breakdown = penalty_breakdown
        if field_definitions_df is not None:
            self._field_definitions_df = field_definitions_df

    def __repr__(self) -> str:
        return f"<OBJECTIVE sequence_external_id={self._external_id}>"

    def __str__(self) -> str:
        return self.__repr__()

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
        SEPARATOR = "--------------------------\n"

        def breakdown_inner_dict(key):
            if inner_dict := self.penalty_breakdown.get(key):
                title = f"Breakdown of {key} penalties\n{SEPARATOR}"
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
        penalty_as_string += SEPARATOR.replace("-", "=")

        penalty_as_string += breakdown_inner_dict("minor")
        penalty_as_string += breakdown_inner_dict("major")
        penalty_as_string += breakdown_inner_dict("unknown")

        return penalty_as_string


class ShopRunResult:
    def __init__(self, po_client: PowerOpsClient, shop_run: ShopRun) -> None:
        if not shop_run.is_complete:
            raise ValueError("ShopRun not completed.")

        self._po_client = po_client
        self._shop_run = shop_run
        self._post_run = None
        self._cplex = None
        self._shop_messages = None
        related_log_files = self._po_client.shop.files.retrieve_related(
            source_external_id=self._shop_run.shop_run_event.external_id,
            label_ext_id=RelationshipLabels.LOG_FILE,
        )
        for metadata in related_log_files:
            ext_id = metadata.external_id
            if ext_id.endswith(".log") and "cplex" in ext_id:
                self._cplex = ShopLogFile(self._po_client, metadata)
            elif ext_id.endswith(".log") and "shop_messages" in ext_id:
                self._shop_messages = ShopLogFile(self._po_client, metadata)
            elif ext_id.endswith(".yaml"):
                self._post_run = ShopYamlFile(self._po_client, metadata)
            else:
                logger.error("Unknown file type")

    @property
    def files(self) -> dict[str, Optional[ShopResultFile]]:
        return {
            "post_run": self._post_run,
            "shop_messages": self._shop_messages,
            "cplex": self._cplex,
        }

    @cached_property
    def success(self) -> bool:
        return self._shop_run.status == ShopRun.Status.SUCCEEDED

    @property
    def error_message(self) -> Optional[str]:
        if not self.success:
            return "(sample) Error: invalid configuration"
        return None

    @property
    def post_run(self) -> Optional[ShopYamlFile]:
        return self._post_run

    @property
    def cplex(self) -> Optional[ShopLogFile]:
        return self._cplex

    @property
    def shop(self) -> Optional[ShopLogFile]:
        return self._shop_messages

    def __repr__(self):
        return f"<ShopRunResult success={self.success}>"

    @cached_property
    def objective_function(self) -> ObjectiveFunction:
        # TODO: ability to retrieve the objective function from the post run yaml file
        cdf: CogniteClient = self._shop_run._po_client.cdf
        relationships = retrieve_relationships_from_source_ext_id(
            cdf,
            self._shop_run.shop_run_event.external_id,
            RelationshipLabels.OBJECTIVE_SEQUENCE,
            target_types=["sequence"],
        )
        for r in relationships:
            seq = cdf.sequences.retrieve(external_id=r.target_external_id)
            # sometimes the label is given to the non-objective sequence too
            if seq.name.lower().find("objective") != -1:
                # Data is inserted as a single row DataFrame
                seq_data = cdf.sequences.data.retrieve_dataframe(
                    external_id=r.target_external_id, start=0, end=-1
                ).to_dict("records")[0]
                # This shape will always be (72, 7) 72 fields and 7 properties of each field
                column_definitions_df = pd.DataFrame(seq.columns)

                penalty_breakdown = {}
                with contextlib.suppress(Exception):
                    penalty_breakdown = json.loads(seq.metadata.get("shop:penalty_breakdown", {}))
                    if type(penalty_breakdown) != dict:
                        penalty_breakdown = {}

                return ObjectiveFunction(
                    self,
                    external_id=seq.external_id,
                    data=seq_data,
                    watercourse=seq.metadata.get("shop:watercourse", ""),
                    penalty_breakdown=penalty_breakdown,
                    field_definitions_df=column_definitions_df,
                )

        logger.error("Objective function sequence not found")
        raise ValueError("Objective function sequence not found")


class ShopRun:
    class Status(Enum):
        IN_PROGRESS = "IN_PROGRESS"
        SUCCEEDED = "SUCCEEDED"
        FAILED = "FAILED"

    def __init__(
        self,
        po_client: PowerOpsClient,
        *,
        shop_run_event: ShopRunEvent,
    ) -> None:
        self._po_client = po_client
        self.shop_run_event = shop_run_event

    @property
    def is_complete(self) -> bool:
        return self.status != ShopRun.Status.IN_PROGRESS

    @property
    def is_success(self) -> bool:
        return self.status != ShopRun.Status.SUCCEEDED

    @property
    def is_failed(self) -> bool:
        return self.status == ShopRun.Status.FAILED

    @property
    def status(self) -> ShopRun.Status:
        return self._po_client.shop.runs.get_status_for(self.shop_run_event.external_id)

    def wait_until_complete(self) -> None:
        while not self.is_complete:
            logger.debug(f"{self.shop_run_event.external_id} is still running...")
            time.sleep(3)
        logger.debug(f"{self.shop_run_event.external_id} finished.")

    def get_results(self, wait: bool = True) -> ShopRunResult:
        if wait:
            if not self.is_complete:
                logger.warning(f"{self.shop_run_event.external_id} is still running, waiting for results...")
            self.wait_until_complete()
        return ShopRunResult(self._po_client, shop_run=self)

    def __repr__(self) -> str:
        return f'<ShopRun status="{self.status}" event_external_id="{self.shop_run_event.external_id}">'

    def __str__(self) -> str:
        return self.__repr__()


class ShopModel:
    def __init__(self) -> None:
        self.model_id = random.randint(1000, 9999)

    def render_yaml(self) -> str:
        return "sintef_shop_model_yaml_representation"

    def update(self):
        raise NotImplementedError


class ShopModelsAPI:
    def __init__(self, po_client: PowerOpsClient):
        self._po_client = po_client

    def list(self) -> list[ShopModel]:
        return [ShopModel()]

    def retrieve(self, model_id):
        m = ShopModel()
        m.model_id = model_id
        return m


class ShopRunsAPI:
    def __init__(self, po_client: PowerOpsClient):
        self._po_client = po_client

    def trigger(self, case: Case) -> ShopRun:
        logger.info("Triggering SHOP run...")
        shop_run = self._upload_to_cdf(case)
        self._post_shop_run(shop_run.shop_run_event.external_id)
        return shop_run

    def _upload_to_cdf(self, case: Case) -> ShopRun:
        shop_run_event = ShopRunEvent(
            watercourse="",
            starttime=case["time.starttime"],
            endtime=case["time.endtime"],
            timeresolution=case["time.timeresolution"],
        )
        logger.debug(f"Uploading event '{shop_run_event.external_id}'.")
        case_file_meta = self._upload_input_file_bytes(case.yaml.encode(), shop_run_event, file_type="case")
        event = shop_run_event.to_event(self._po_client.write_dataset_id)
        self._connect_file_to_event(case_file_meta, event, RelationshipLabels.CASE_FILE)
        preprocessor_metadata = {"cog_shop_case_file": {"external_id": case_file_meta.external_id}}

        if case.cut_file:
            cut_file_meta = self._upload_input_file(
                case.cut_file["file"], shop_run_event=shop_run_event, input_file_type="cut"
            )
            self._connect_file_to_event(cut_file_meta, event, RelationshipLabels.CUT_FILE)
            preprocessor_metadata["cut_file"] = {"external_id": cut_file_meta.external_id}

        if case.mapping_files:
            preprocessor_metadata["mapping_files"] = []
        for mapping_file in case.mapping_files or []:
            mapping_file_meta = self._upload_input_file(
                mapping_file["file"], shop_run_event=shop_run_event, input_file_type="mapping"
            )
            self._connect_file_to_event(mapping_file_meta, event, RelationshipLabels.MAPPING_FILE)
            preprocessor_metadata["mapping_files"].append({"external_id": mapping_file_meta.external_id})

        if case.extra_files:
            preprocessor_metadata["extra_files"] = []
        for extra_file in case.extra_files or []:
            extra_file_meta = self._upload_input_file(
                extra_file["file"], shop_run_event=shop_run_event, input_file_type="extra"
            )
            self._connect_file_to_event(extra_file_meta, event, RelationshipLabels.EXTRA_FILE)
            preprocessor_metadata["extra_files"].append({"external_id": extra_file_meta.external_id})

        event.metadata["shop:preprocessor_data"] = json.dumps(preprocessor_metadata)
        event.metadata["shop:manual_run"] = "yes"
        # avoid event being picked up by sniffer
        event.metadata["processed"] = "yes"
        self._po_client.cdf.events.create(event)
        return ShopRun(self._po_client, shop_run_event=shop_run_event)

    def _upload_input_file(
        self, file: str, shop_run_event: ShopRunEvent, input_file_type: InputFileTypeT
    ) -> FileMetadata:
        _, input_file_ext = os.path.splitext(file)
        with open(file, "rb") as file_stream:
            return self._upload_input_file_bytes(file_stream.read(), shop_run_event, input_file_type, input_file_ext)

    def _upload_input_file_bytes(
        self,
        content: Union[str, bytes, TextIO, BinaryIO],
        shop_run_event: ShopRunEvent,
        file_type: InputFileTypeT,
        file_ext: str = ".yaml",
    ) -> FileMetadata:
        file_ext_id = f"{shop_run_event.external_id}_{file_type.upper()}"
        logger.debug(f"Uploading file: '{file_ext_id}'.")
        file_meta = self._po_client.cdf.files.upload_bytes(
            external_id=file_ext_id,
            content=content,
            data_set_id=self._po_client.write_dataset_id,
            name=f"{shop_run_event.external_id}_{file_type}{file_ext}",
            mime_type="application/yaml",
            metadata={
                "shop:run_event_id": shop_run_event.external_id,
                "shop:type": f"cog_shop_{file_type}",
            },
            overwrite=True,
        )
        return file_meta

    def _connect_file_to_event(self, file_meta: FileMetadata, event: Event, relationship_label: str):
        relationship = simple_relationship(
            source=event,
            target=file_meta,
            label_external_id=relationship_label,
        )
        relationship.data_set_id = self._po_client.write_dataset_id
        self._po_client.cdf.relationships.create(relationship)

    def _post_shop_run(self, shop_run_external_id: str):
        logger.info(f"Triggering run-shop endpoint, cogShopVersion: '{self._po_client._cogshop_version}'.")
        cdf_config = self._po_client.cdf.config
        project = cdf_config.project
        cluster = cdf_config.base_url.split("//")[1].split(".")[0]
        url = f"https://power-ops-api.staging.{cluster}.cognite.ai/{project}/run-shop"
        auth_header = dict([cdf_config.credentials.authorization_header()])

        response = requests.post(
            url,
            json={
                "shopEventExternalId": shop_run_external_id,
                "datasetId": self._po_client.write_dataset_id,
                "cogShopVersion": self._po_client._cogshop_version,
            },
            headers=auth_header,
        )
        response.raise_for_status()
        logger.debug(response.json())

    def list(self) -> list[ShopRun]:
        raise NotImplementedError()

    def retrieve(self, external_id: str) -> ShopRun:
        logger.info(f"Retrieving event '{external_id}'.")
        event = self._po_client.cdf.events.retrieve(external_id=external_id)
        return ShopRun(
            self._po_client,
            shop_run_event=ShopRunEvent.from_event(event),
        )

    def get_status_for(self, shop_run_external_id: str) -> ShopRun.Status:
        event = retrieve_event(self._po_client.cdf, shop_run_external_id)
        logger.debug(f"Reading status from event {event.external_id}.")

        relationships = self._po_client.cdf.relationships.list(
            data_set_ids=[self._po_client.write_dataset_id],
            source_external_ids=[shop_run_external_id],
            target_types=["event"],
        )
        related_events = []
        if relationships:
            related_events = self._po_client.cdf.events.retrieve_multiple(
                external_ids=[rel.target_external_id for rel in relationships],
                ignore_unknown_ids=True,
            )
        if not len(related_events) or all(ev.type == "POWEROPS_PROCESS_STARTED" for ev in related_events):
            return ShopRun.Status.IN_PROGRESS
        elif any(ev.type == "POWEROPS_PROCESS_FINISHED" for ev in related_events):
            return ShopRun.Status.SUCCEEDED
        else:
            return ShopRun.Status.FAILED


class ShopFilesAPI:
    def __init__(self, po_client: PowerOpsClient) -> None:
        self._po_client = po_client

    def retrieve_related(
        self, source_external_id: str, label_ext_id: Optional[Union[str, Sequence[str]]] = None
    ) -> Sequence[FileMetadata]:
        relationships = retrieve_relationships_from_source_ext_id(
            self._po_client.cdf,
            source_ext_id=source_external_id,
            label_ext_id=label_ext_id,
            target_types=["file"],
        )
        if not relationships:
            return []
        return self._po_client.cdf.files.retrieve_multiple(
            external_ids=[rel.target_external_id for rel in relationships],
            ignore_unknown_ids=True,
        )

    def download(self, shop_file: ShopResultFile, dir_path: str) -> str:
        file_path = os.path.join(dir_path, shop_file.external_id)
        self._po_client.cdf.files.download_to_path(path=file_path, external_id=shop_file.external_id)
        return file_path


class ShopAPI:
    def __init__(self, po_client: PowerOpsClient):
        self._po_client = po_client
        self.models = ShopModelsAPI(po_client=po_client)
        self.runs = ShopRunsAPI(po_client=po_client)
        self.files = ShopFilesAPI(po_client=po_client)
