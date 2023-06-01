from __future__ import annotations

import json
import logging
import os
import random
import tempfile
import time
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, BinaryIO, Literal, Optional, TextIO, Union

import requests
import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import Event, FileMetadata

from cognite.powerops.case.shop_run_event import ShopRunEvent
from cognite.powerops.utils.cdf_utils import retrieve_relationships_from_source_ext_id, simple_relationship
from cognite.powerops.utils.labels import RelationshipLabels

if TYPE_CHECKING:
    from cognite.powerops import Case, PowerOpsClient

logger = logging.getLogger(__name__)


FileTypeT = Literal["case", "cut", "mapping", "extra"]


class ShopRunLog:
    def __init__(self, shop_run_logs: "ShopRunLogs", file_metadata: FileMetadata = None, encoding="utf-8") -> None:
        self._shop_run_logs = shop_run_logs
        self._file_metadata = file_metadata
        self.encoding = encoding

    @cached_property
    def file_content(self) -> str:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = os.path.join(tmp_dir, self.file_metadata.external_id)
            self._shop_run_logs._shop_run_result._shop_run._po_client.cdf.files.download_to_path(
                path=tmp_path, external_id=self.file_metadata.external_id
            )
            with open(tmp_path, "r", encoding=self.encoding) as f:
                return f.read()

    def read(self):
        return self.file_content

    def save_to_path(self, path: str = "") -> str:
        path = path or os.path.join(os.getcwd(), self.file_metadata.external_id)
        with open(path, "w", encoding=self.encoding) as f:
            f.write(self.file_content)
        return path

    def print(self) -> None:
        print(self.read())

    @property
    def file_metadata(self) -> FileMetadata:
        return self._file_metadata


class ShopRunYaml(ShopRunLog):
    def __init__(self, shop_run_logs: "ShopRunLogs", file_metadata: FileMetadata = None) -> None:
        super().__init__(shop_run_logs, file_metadata, encoding="utf-8")

    @cached_property
    def file_content(self) -> dict:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = os.path.join(tmp_dir, self.file_metadata.external_id)
            self._shop_run_logs._shop_run_result._shop_run._po_client.cdf.files.download_to_path(
                path=tmp_path, external_id=self.file_metadata.external_id
            )
            with open(tmp_path, "r", encoding=self.encoding) as f:
                return yaml.safe_load(f)

    def save_to_path(self, path: str = "") -> str:
        path = path or os.path.join(os.getcwd(), self.file_metadata.external_id)
        with open(path, "w", encoding=self.encoding) as f:
            f.write(yaml.safe_dump(self.file_content))
        return path


class ShopRunLogs:
    def __init__(
        self,
        shop_run_result: "ShopRunResult",
        cplex_metadata: ShopRunLog = None,
        post_run_metadata: ShopRunYaml = None,
        shop_metadata: ShopRunLog = None,
    ) -> None:
        self._shop_run_result = shop_run_result
        self._cplex = ShopRunLog(self, cplex_metadata)
        self._post_run = ShopRunYaml(self, post_run_metadata)
        # Not sure why the encoding is different for shop logs
        self._shop = ShopRunLog(self, shop_metadata, encoding="latin-1")

    @property
    def cplex(self) -> Optional[ShopRunLog]:
        return self._cplex

    @property
    def post_run(self) -> Optional[ShopRunYaml]:
        return self._post_run

    @property
    def shop(self) -> Optional[ShopRunLog]:
        return self._shop


class ShopRunResult:
    def __init__(self, shop_run: "ShopRun") -> None:
        self._shop_run = shop_run

    @property
    def success(self) -> bool:
        return self._shop_run.status() == ShopRun.Status.SUCCEEDED

    @property
    def error_message(self) -> Optional[str]:
        if not self.success:
            return "(sample) Error: invalid configuration"
        return None

    @cached_property
    def logs(self) -> Optional[ShopRunLogs]:
        cdf: CogniteClient = self._shop_run._po_client.cdf
        relationships = retrieve_relationships_from_source_ext_id(
            cdf,
            self._shop_run.shop_run_event.external_id,
            RelationshipLabels.LOG_FILE,
        )
        cplex, shop, post_run = None, None, None
        for r in relationships:
            if r.target_type != "file":
                continue
            ext_id: str = r.target_external_id
            metadata = cdf.files.retrieve(external_id=ext_id)

            if ext_id.endswith(".log") and "cplex" in ext_id:
                cplex = metadata

            elif ext_id.endswith(".log") and "shop_messages" in ext_id:
                shop = metadata

            elif ext_id.endswith(".yaml"):
                post_run = metadata

            else:
                logger.error("Unknown file type")

        return ShopRunLogs(shop_run_result=self, cplex_metadata=cplex, post_run_metadata=post_run, shop_metadata=shop)

    @property
    def objective(self) -> Optional[list]:
        raise NotImplementedError


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

    def _retrieve_event(self) -> Event:
        return self._po_client.cdf.events.retrieve(external_id=self.shop_run_event.external_id)

    def is_complete(self) -> bool:
        return self.status() != ShopRun.Status.IN_PROGRESS

    def status(self) -> ShopRun.Status:
        event = self._retrieve_event()
        logger.debug(f"Reading status from event {event.external_id}.")

        relationships = self._po_client.cdf.relationships.list(
            data_set_ids=[self._po_client.write_dataset_id],
            source_external_ids=[self.shop_run_event.external_id],
            target_types=["event"],
        )
        related_events = (
            self._po_client.cdf.events.retrieve_multiple(
                external_ids=[rel.target_external_id for rel in relationships],
                ignore_unknown_ids=True,
            )
            if relationships
            else []
        )
        if not len(related_events):
            return ShopRun.Status.IN_PROGRESS
        elif any(ev.type == "POWEROPS_PROCESS_FINISHED" for ev in related_events):
            return ShopRun.Status.SUCCEEDED
        else:
            return ShopRun.Status.FAILED

    def wait_until_complete(self) -> ShopRunResult:
        while not self.is_complete():
            logger.debug(f"{self.shop_run_event.external_id} is still running...")
            time.sleep(3)
        return ShopRunResult(shop_run=self)


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
        case_file_meta = self._upload_bytes(case.yaml.encode(), shop_run_event, file_type="case")
        event = shop_run_event.to_event(self._po_client.write_dataset_id)
        self._connect_file_to_event(case_file_meta, event, RelationshipLabels.CASE_FILE)
        preprocessor_metadata = {"cog_shop_case_file": case_file_meta.external_id}

        if case.cut_file:
            cut_file_meta = self._upload_file(**case.cut_file, shop_run_event=shop_run_event, file_type="cut")
            self._connect_file_to_event(cut_file_meta, event, RelationshipLabels.CUT_FILE)
            preprocessor_metadata["cut_file"] = {"external_id": cut_file_meta.external_id}

        if case.mapping_files:
            preprocessor_metadata["mapping_files"] = []
        for mapping_file in case.mapping_files or []:
            mapping_file_meta = self._upload_file(**mapping_file, shop_run_event=shop_run_event, file_type="mapping")
            self._connect_file_to_event(mapping_file_meta, event, RelationshipLabels.MAPPING_FILE)
            preprocessor_metadata["mapping_files"].append({"external_id": mapping_file_meta.external_id})

        if case.extra_files:
            preprocessor_metadata["extra_files"] = []
        for extra_file in case.extra_files or []:
            extra_file_meta = self._upload_file(**extra_file, shop_run_event=shop_run_event, file_type="extra")
            self._connect_file_to_event(extra_file_meta, event, RelationshipLabels.EXTRA_FILE)
            preprocessor_metadata["extra_files"].append({"external_id": extra_file_meta.external_id})

        event.metadata["shop:preprocessor_data"] = json.dumps(preprocessor_metadata)
        event.metadata["shop:manual_run"] = "yes"
        # avoid event being picked up by sniffer
        event.metadata["processed"] = "yes"
        self._po_client.cdf.events.create(event)
        return ShopRun(self._po_client, shop_run_event=shop_run_event)

    def _upload_file(
        self, file: str, encoding: str, shop_run_event: ShopRunEvent, file_type: FileTypeT
    ) -> FileMetadata:
        _, file_ext = os.path.splitext(file)
        with open(file, encoding=encoding) as file_stream:
            return self._upload_bytes(file_stream.read().encode(encoding), shop_run_event, file_type, file_ext)

    def _upload_bytes(
        self,
        content: Union[str, bytes, TextIO, BinaryIO],
        shop_run_event: ShopRunEvent,
        file_type: FileTypeT,
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
        url = f"https://power-ops-api.staging.{cluster}.cognite.ai/{project}" f"/run-shop"
        auth_header = dict([cdf_config.credentials.authorization_header()])

        response = requests.post(
            url,
            json={
                "shopEventExternalId": shop_run_external_id,
                "cogShopVersion": self._po_client._cogshop_version,
            },
            headers=auth_header,
        )
        response.raise_for_status()
        logger.debug(response.json())

    def list(self) -> list[ShopRun]:
        raise NotImplementedError

    def retrieve(self, external_id: str) -> ShopRun:
        logger.info(f"Retrieving event '{external_id}'.")
        event = self._po_client.cdf.events.retrieve(external_id=external_id)
        return ShopRun(
            self._po_client,
            shop_run_event=ShopRunEvent.from_event(event),
        )


class ShopAPI:
    def __init__(self, po_client: PowerOpsClient):
        self._po_client = po_client
        self.models = ShopModelsAPI(po_client=po_client)
        self.runs = ShopRunsAPI(po_client=po_client)
