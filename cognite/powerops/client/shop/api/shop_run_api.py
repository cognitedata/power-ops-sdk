from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Literal, TextIO, Union
from urllib.parse import urlparse

import requests
from cognite.client import CogniteClient
from cognite.client.data_classes import Event, FileMetadata
from cognite.client.utils._time import datetime_to_ms, time_ago_to_ms

from cognite.powerops.cdf_labels import RelationshipLabel
from cognite.powerops.client.data_set_api import DataSetsAPI
from cognite.powerops.client.shop.api.shop_results_api import ShopRunResultsAPI
from cognite.powerops.client.shop.data_classes import Case, ShopRun, ShopRunEvent
from cognite.powerops.utils.cdf.calls import retrieve_event
from cognite.powerops.utils.cdf.resource_creation import simple_relationship

logger = logging.getLogger(__name__)


InputFileTypeT = Literal["case", "cut", "mapping", "extra"]


class ShopRunsAPI:
    def __init__(
        self,
        client: CogniteClient,
        data_set_api: DataSetsAPI,
        result_api: ShopRunResultsAPI,
        cogshop_version: str,
        shop_as_a_service: bool = False,
    ):
        self._client = client
        self._data_set_api = data_set_api
        self._result_api = result_api
        self._cogshop_version = cogshop_version
        self._shop_as_a_service = shop_as_a_service

    def trigger(self, case: Case) -> ShopRun:
        """
        Trigger a SHOP run for a given case.

        Args:
            case: The case to run.

        Returns:
            The SHOP run.
        """
        logger.info("Triggering SHOP run...")
        shop_run = self._upload_to_cdf(case)
        self._trigger_shop_container(shop_run.shop_run_event.external_id)
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
        with Path(file).open("rb") as file_stream:
            return self._upload_input_file_bytes(file_stream.read(), shop_run_event, input_file_type, Path(file).suffix)

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
            metadata={"shop:run_event_id": shop_run_event.external_id, "shop:type": f"cog_shop_{file_type}"},
            overwrite=True,
        )

    def _connect_file_to_event(self, file_meta: FileMetadata, event: Event, relationship_label: str):
        relationship = simple_relationship(source=event, target=file_meta, label_external_id=relationship_label)
        relationship.data_set_id = self._data_set_api.write_dataset_id
        self._client.relationships.create(relationship)

    def _shop_url(self) -> str:
        project = self._client.config.project

        cluster = urlparse(self._client.config.base_url).netloc.split(".", 1)[0]

        environment = project.split("-")[-1]
        if environment in {"dev", "staging"}:
            stage = ".staging"
        elif environment == "prod":
            stage = ""
        else:
            raise ValueError(f"Can't detect prod/staging from project name: {project!r}")

        return f"https://power-ops-api{stage}.{cluster}.cognite.ai/{project}/run-shop"

    def _shop_url_shaas(self) -> str:

        project = self._client.config.project

        cluster = urlparse(self._client.config.base_url).netloc.split(".", 1)[0]

        if project == "power-ops-staging":
            environment = ".staging"
        elif project in {"lyse-dev", "lyse-prod", "heco-dev", "heco-prod"}:
            environment = ""
        else:
            raise ValueError(f"SHOP As A Service has not been configured for project name: {project!r}")

        return f"https://power-ops-api{environment}.{cluster}.cognite.ai/{project}/run-shop-as-service"

    def _trigger_shop_container(self, shop_run: ShopRun):
        def auth(r: requests.PreparedRequest) -> requests.PreparedRequest:
            auth_header_name, auth_header_value = self._client._config.credentials.authorization_header()
            r.headers[auth_header_name] = auth_header_value
            return r

        if self._shop_as_a_service:
            shop_url = self._shop_url_shaas()
            shop_body = {"mode": "asset", "runs": [{"event_external_id": shop_run.external_id}]}
        else:
            shop_url = self._shop_url()
            shop_body = {"shopEventExternalId": shop_run.external_id, "cogShopVersion": self.cogshop_version}

        response = requests.post(
            url=shop_url,
            json=shop_body,
            auth=auth,
        )
        response.raise_for_status()

    def list(self, created_after: int | str | None = None, limit: int | None = 25) -> list[ShopRun]:
        """
        List SHOP runs.

        Args:
            created_after: Only list runs created after this time. Can be a datetime, a time ago string, or a timestamp.
                           For more information about time ago strings, see the tip below.
            limit: The maximum number of runs to return.

        !!! tip "created_after parameter, time ago string"
            The created after parameter supports arguments given on the format `<integer>(s|m|h|d|w)-ago`.
            For example, `12h-ago`, which will be parsed to `now - 12h-ago`.

        Returns:
            List of SHOP runs.
        """

        if isinstance(created_after, str):
            timespan = time_ago_to_ms(created_after)
            now = datetime_to_ms(datetime.now(timezone.utc))
            created_after = now - timespan

        shop_run_events = self._client.events.list(
            type=ShopRunEvent.event_type,
            created_time={"min": created_after} if created_after is not None else None,
            metadata={"process_type": ShopRunEvent.process_type},
            limit=limit,
        )

        return [
            ShopRun(self.retrieve_status, retrieve_results=self._result_api.retrieve, shop_run_event=shop_run_event)
            for shop_run_event in shop_run_events
        ]

    def retrieve(self, external_id: str) -> ShopRun:
        """
        Retrieve a SHOP run.

        Args:
            external_id: The external id of the SHOP run.

        Returns:
            The SHOP run.
        """
        event = self._client.events.retrieve(external_id=external_id)
        return ShopRun(
            self.retrieve_status,
            retrieve_results=self._result_api.retrieve,
            shop_run_event=ShopRunEvent.from_event(event),
        )

    def retrieve_status(self, shop_run_external_id: str) -> ShopRun.Status:
        """
        Retrieve the status of a SHOP run.

        Args:
            shop_run_external_id: The external id of the SHOP run.

        Returns:
            The status of the SHOP run.
        """

        event = retrieve_event(self._client, shop_run_external_id)
        logger.debug(f"Reading status from event {event.external_id}.")
        if relationships := self._client.relationships.list(
            # At the moment, looking at run whose related event might not be in `uc:000:powerops`
            source_external_ids=[shop_run_external_id],
            target_types=["event"],
        ):
            related_events = self._client.events.retrieve_multiple(
                external_ids=[rel.target_external_id for rel in relationships], ignore_unknown_ids=True
            )
        else:
            related_events = []
        if not len(related_events) or all(ev.type == "POWEROPS_PROCESS_STARTED" for ev in related_events):
            return ShopRun.Status.IN_PROGRESS
        elif any(ev.type == "POWEROPS_PROCESS_FINISHED" for ev in related_events):
            return ShopRun.Status.SUCCEEDED
        else:
            return ShopRun.Status.FAILED
