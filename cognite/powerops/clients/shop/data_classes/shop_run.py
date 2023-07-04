from __future__ import annotations

import logging
import time
from enum import Enum
from typing import TYPE_CHECKING

from cognite.powerops.client.data_classes.shop_results import ShopRunResult
from cognite.powerops.client.data_classes.shop_run_event import ShopRunEvent

if TYPE_CHECKING:
    from cognite.powerops import PowerOpsClient

logger = logging.getLogger(__name__)


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
