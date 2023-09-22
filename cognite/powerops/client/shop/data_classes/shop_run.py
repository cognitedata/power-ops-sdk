from __future__ import annotations

import logging
import time
from enum import Enum
from typing import TYPE_CHECKING, Callable

from cognite.powerops.client.shop.data_classes.shop_run_event import ShopRunEvent

if TYPE_CHECKING:
    from cognite.powerops.client.shop.data_classes.shop_results import ShopRunResult

logger = logging.getLogger(__name__)


class ShopRun:
    """
    This represents a SHOP run.

    """

    class Status(Enum):
        IN_PROGRESS = "IN_PROGRESS"
        SUCCEEDED = "SUCCEEDED"
        FAILED = "FAILED"

    def __init__(
        self,
        check_status: Callable[[str], ShopRun.Status],
        retrieve_results: Callable[[ShopRun], ShopRunResult],
        *,
        shop_run_event: ShopRunEvent,
    ) -> None:
        self._check_status = check_status
        self._retrieve_results = retrieve_results
        self.shop_run_event = shop_run_event

    @property
    def in_progress(self) -> bool:
        """
        Returns whether the SHOP run is still running.

        Returns:
            True if the SHOP run is still running, False otherwise.
        """
        return self.status == ShopRun.Status.IN_PROGRESS

    @property
    def succeeded(self) -> bool:
        """
        Returns whether the SHOP run succeeded.

        Returns:
            True if the SHOP run succeeded, False otherwise.
        """
        return self.status == ShopRun.Status.SUCCEEDED

    @property
    def failed(self) -> bool:
        """
        Returns whether the SHOP run failed.

        Returns:
            True if the SHOP run failed, False otherwise.
        """
        return self.status == ShopRun.Status.FAILED

    @property
    def status(self) -> ShopRun.Status:
        """

        Queries the SHOP API for the status of the SHOP run.

        Returns:
            The status of the SHOP run.
        """
        return self._check_status(self.shop_run_event.external_id)

    def wait_until_complete(self) -> None:
        """
        Blocking call that waits until the until the SHOP run is complete.

        """
        while self.in_progress:
            logger.debug(f"{self.shop_run_event.external_id} is still running...")
            time.sleep(3)
        logger.debug(f"{self.shop_run_event.external_id} finished.")

    def get_results(self, wait: bool = True) -> ShopRunResult:
        """
        Retrieve the results of the SHOP run.

        Args:
            wait: If True, wait until the SHOP run is complete before retrieving the results.

        Returns:
            The results of the SHOP run.
        """
        if wait:
            if self.in_progress:
                logger.warning(f"{self.shop_run_event.external_id} is still running, waiting for results...")
            self.wait_until_complete()
        return self._retrieve_results(self)

    def __repr__(self) -> str:
        return f'<ShopRun status="{self.status}" event_external_id="{self.shop_run_event.external_id}">'

    def __str__(self) -> str:
        return self.__repr__()
