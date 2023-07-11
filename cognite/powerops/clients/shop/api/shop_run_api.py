from __future__ import annotations

import json
import logging
import os
from typing import BinaryIO, Literal, TextIO, Union

import requests
from cognite.client import CogniteClient
from cognite.client.data_classes import Event, FileMetadata

from cognite.powerops.cdf_labels import RelationshipLabel
from cognite.powerops.clients.data_set_api import DataSetsAPI
from cognite.powerops.clients.shop.api.shop_results_api import ShopRunResultsAPI
from cognite.powerops.clients.shop.data_classes import Case, ShopRun, ShopRunEvent
from cognite.powerops.utils.cdf.calls import retrieve_event
from cognite.powerops.utils.cdf.resource_creation import simple_relationship

logger = logging.getLogger(__name__)


InputFileTypeT = Literal["case", "cut", "mapping", "extra"]


RUN_SHOP_URL = "https://power-ops-api.staging.{cluster}.cognite.ai/{project}/run-shop"


class ShopRunsAPI:
    def __init__(
        self, client: CogniteClient, data_set_api: DataSetsAPI, result_api: ShopRunResultsAPI, cogshop_version: str
    ):
        self._client = client
        self._data_set_api = data_set_api
        self._result_api = result_api
        self._cogshop_version = cogshop_version

    def trigger(self, case: Case) -> ShopRun:
        logger.info("Triggering SHOP run...")
        shop_run = self._upload_to_cdf(case)
        self._post_shop_run(shop_run.shop_run_event.external_id)
        return shop_run

    def _upload_to_cdf(self, case: Case) -> ShopRun:
        shop_run_event = ShopRunEvent(
            watercourse="",
            starttime=case.data["time"]["starttime"],
            endtime=case.data["time"]["endtime"],
            timeresolution=case.data["time"]["timeresolution"],
            manual_run=True,
        )
        logger.debug(f"Uploading event '{shop_run_event.external_id}'.")
        case_file_meta = self._upload_input_file_bytes(case.yaml.encode(), shop_run_event, file_type="case")
        event = shop_run_event.to_event(self._data_set_api.write_dataset_id)
        self._connect_file_to_event(case_file_meta, event, RelationshipLabel.CASE_FILE.value)
        preprocessor_metadata = {"cog_shop_case_file": {"external_id": case_file_meta.external_id}}

        if case.cut_file:
            cut_file_meta = self._upload_input_file(
                case.cut_file["file"], shop_run_event=shop_run_event, input_file_type="cut"
            )
            self._connect_file_to_event(cut_file_meta, event, RelationshipLabel.CUT_FILE.value)
            preprocessor_metadata["cut_file"] = {"external_id": cut_file_meta.external_id}

        if case.mapping_files:
            preprocessor_metadata["mapping_files"] = []
        for mapping_file in case.mapping_files or []:
            mapping_file_meta = self._upload_input_file(
                mapping_file["file"], shop_run_event=shop_run_event, input_file_type="mapping"
            )
            self._connect_file_to_event(mapping_file_meta, event, RelationshipLabel.MAPPING_FILE.value)
            preprocessor_metadata["mapping_files"].append({"external_id": mapping_file_meta.external_id})

        if case.extra_files:
            preprocessor_metadata["extra_files"] = []
        for extra_file in case.extra_files or []:
            extra_file_meta = self._upload_input_file(
                extra_file["file"], shop_run_event=shop_run_event, input_file_type="extra"
            )
            self._connect_file_to_event(extra_file_meta, event, RelationshipLabel.EXTRA_FILE.value)
            preprocessor_metadata["extra_files"].append({"external_id": extra_file_meta.external_id})

        event.metadata["shop:preprocessor_data"] = json.dumps(preprocessor_metadata)
        event.metadata["processed"] = "yes"  # avoid event being picked up by sniffer
        self._client.events.create(event)
        return ShopRun(self.retrieve_status, retrieve_results=self._result_api.retrieve, shop_run_event=shop_run_event)

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
        return self._client.files.upload_bytes(
            external_id=file_ext_id,
            content=content,
            data_set_id=self._data_set_api.write_dataset_id,
            name=f"{shop_run_event.external_id}_{file_type}{file_ext}",
            mime_type="application/yaml",
            metadata={
                "shop:run_event_id": shop_run_event.external_id,
                "shop:type": f"cog_shop_{file_type}",
            },
            overwrite=True,
        )

    def _connect_file_to_event(self, file_meta: FileMetadata, event: Event, relationship_label: str):
        relationship = simple_relationship(
            source=event,
            target=file_meta,
            label_external_id=relationship_label,
        )
        relationship.data_set_id = self._data_set_api.write_dataset_id
        self._client.relationships.create(relationship)

    def _post_shop_run(self, shop_run_external_id: str):
        logger.info(f"Triggering run-shop endpoint, cogShopVersion: '{self._cogshop_version}'.")
        cdf_config = self._client.config
        project = cdf_config.project
        cluster = cdf_config.base_url.split("//")[1].split(".")[0]
        auth_header = dict([cdf_config.credentials.authorization_header()])

        response = requests.post(
            RUN_SHOP_URL.format(cluster=cluster, project=project),
            json={
                "shopEventExternalId": shop_run_external_id,
                "datasetId": self._data_set_api.write_dataset_id,
                "cogShopVersion": self._cogshop_version,
            },
            headers=auth_header,
        )
        response.raise_for_status()
        logger.debug(response.json())

    def list(self) -> list[ShopRun]:
        raise NotImplementedError()

    def retrieve(self, external_id: str) -> ShopRun:
        event = self._client.events.retrieve(external_id=external_id)
        return ShopRun(
            self.retrieve_status,
            retrieve_results=self._result_api.retrieve,
            shop_run_event=ShopRunEvent.from_event(event),
        )

    def retrieve_status(self, shop_run_external_id: str) -> ShopRun.Status:
        event = retrieve_event(self._client, shop_run_external_id)
        logger.debug(f"Reading status from event {event.external_id}.")

        if relationships := self._client.relationships.list(
            data_set_ids=[self._data_set_api.write_dataset_id],
            source_external_ids=[shop_run_external_id],
            target_types=["event"],
        ):
            related_events = self._client.events.retrieve_multiple(
                external_ids=[rel.target_external_id for rel in relationships],
                ignore_unknown_ids=True,
            )
        else:
            related_events = []
        if not len(related_events) or all(ev.type == "POWEROPS_PROCESS_STARTED" for ev in related_events):
            return ShopRun.Status.IN_PROGRESS
        elif any(ev.type == "POWEROPS_PROCESS_FINISHED" for ev in related_events):
            return ShopRun.Status.SUCCEEDED
        else:
            return ShopRun.Status.FAILED
