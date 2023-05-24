from __future__ import annotations

import random
import time
from enum import Enum
from typing import TYPE_CHECKING, Optional

from cognite.client.data_classes import Event, FileMetadata

from cognite.powerops.case.shop_run_event import ShopRunEvent
from cognite.powerops.utils.cdf_utils import simple_relationship
from cognite.powerops.utils.labels import RelationshipLabels

if TYPE_CHECKING:
    from cognite.powerops import Case, PowerOpsClient


class ShopRunLog:
    def __init__(self, shop_run_logs: "ShopRunLogs") -> None:
        self._shop_run_logs = shop_run_logs

    def file(self) -> FileMetadata:
        return FileMetadata(external_id="log-123", name="shop_123.log")

    def read(self) -> str:
        return "\n".join(f"log line {i}" for i in range(10))

    def print(self) -> None:
        print(self.read())


class ShopRunLogs:
    def __init__(self, shop_run_result: "ShopRunResult") -> None:
        self._shop_run_result = shop_run_result

    def cplex(self) -> ShopRunLog:
        return ShopRunLog(self)

    def post_run(self) -> ShopRunLog:
        return ShopRunLog(self)

    def shop(self) -> ShopRunLog:
        return ShopRunLog(self)


class ShopRunResult:
    def __init__(self, shop_run: "ShopRun") -> None:
        self._shop_run = shop_run

    @property
    def success(self) -> bool:
        if random.random() > 0.5:
            return True
        return False

    @property
    def error_message(self) -> Optional[str]:
        if not self.success:
            return "(sample) Error: invalid configuration"
        return None

    @property
    def logs(self) -> Optional[ShopRunLogs]:
        return ShopRunLogs(shop_run_result=self)

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
        return self._po_client.cdf.events.retrieve(self.shop_run_event.external_id)

    def is_complete(self) -> bool:
        """
        Query CDF and fetch the "processed" metadata value.
        "false" while still working, "true" when it succeeds or fails.
        """
        return self.status() != ShopRun.Status.IN_PROGRESS

    def status(self) -> ShopRun.Status:
        event = self._retrieve_event()
        if not event.metadata.get("processed", False):
            return ShopRun.Status.IN_PROGRESS

        relationships = self._po_client.cdf.relationships.list(
            source_external_ids=self.shop_run_event.external_id,
            target_types="Event",
        )
        related_events = self._po_client.cdf.events.retrieve_multiple(
            external_ids=[rel.target_external_id for rel in relationships],
        )
        if any(ev.type == "POWEROPS_PROCESS_FINISHED" for ev in related_events):
            return ShopRun.Status.SUCCEEDED
        else:
            return ShopRun.Status.FAILED

    def wait_until_complete(self) -> ShopRunResult:
        while not self.is_complete():
            print("SHOP is still running...")
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

    def trigger(self, watercourse_name: str, case: Case) -> ShopRun:
        shop_run_event = ShopRunEvent(
            watercourse=watercourse_name,
            starttime=case["time.starttime"],
            endtime=case["time.endtime"],
            timeresolution=case["time.timeresolution"],
        )
        event = self._po_client.cdf.events.create(
            shop_run_event.to_event(self._po_client.write_dataset_id),
        )
        file_meta = self._po_client.cdf.files.upload_bytes(
            external_id=f"{event.external_id}_CASE_FILE",
            content=case.yaml.encode(),
            data_set_id=self._po_client.write_dataset_id,
            name=f"{watercourse_name}_case.yaml",
            mime_type="application/yaml",
            metadata={
                "shop:run_event_id": shop_run_event.external_id,
                "shop:type": "cog_shop_case",
            },
            overwrite=True,
        )
        self._po_client.cdf.relationships.create(
            simple_relationship(
                source=event,
                target=file_meta,
                label_external_id=RelationshipLabels.CASE_FILE,
            )
        )
        return ShopRun(self._po_client, shop_run_event=shop_run_event)

    def list(self) -> list[ShopRun]:
        raise NotImplementedError

    def retrieve(self, external_id: str) -> ShopRun:
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
