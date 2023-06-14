from __future__ import annotations

import json
import logging
import os
import time
from enum import Enum
from typing import TYPE_CHECKING, BinaryIO, Literal, TextIO, Union

import requests
from cognite.client.data_classes import Event, FileMetadata

from cognite.powerops.case.shop_run_event import ShopRunEvent
from cognite.powerops.client.shop_results import ShopRunResult
from cognite.powerops.utils.cdf_utils import retrieve_event, simple_relationship
from cognite.powerops.utils.labels import RelationshipLabels

if TYPE_CHECKING:
    from cognite.powerops import Case, PowerOpsClient

logger = logging.getLogger(__name__)


InputFileTypeT = Literal["case", "cut", "mapping", "extra"]


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
    def in_progress(self) -> bool:
        return self.status == ShopRun.Status.IN_PROGRESS

    @property
    def succeeded(self) -> bool:
        return self.status == ShopRun.Status.SUCCEEDED

    @property
    def failed(self) -> bool:
        return self.status == ShopRun.Status.FAILED

    @property
    def status(self) -> ShopRun.Status:
        return self._po_client.shop.runs.retrieve_status(self.shop_run_event.external_id)

    def wait_until_complete(self) -> None:
        while self.in_progress:
            logger.debug(f"{self.shop_run_event.external_id} is still running...")
            time.sleep(3)
        logger.debug(f"{self.shop_run_event.external_id} finished.")

    def get_results(self, wait: bool = True) -> ShopRunResult:
        if wait:
            if self.in_progress:
                logger.warning(f"{self.shop_run_event.external_id} is still running, waiting for results...")
            self.wait_until_complete()
        return self._po_client.shop.results.retrieve(self)

    def __repr__(self) -> str:
        return f'<ShopRun status="{self.status}" event_external_id="{self.shop_run_event.external_id}">'

    def __str__(self) -> str:
        return self.__repr__()


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
        logger.info(f"Triggering run-shop endpoint, cogShopVersion: '{self._po_client.cogshop_version}'.")
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
                "cogShopVersion": self._po_client.cogshop_version,
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

    def retrieve_status(self, shop_run_external_id: str) -> ShopRun.Status:
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
