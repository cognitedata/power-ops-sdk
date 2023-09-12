from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client.data_classes import filters

from .shop_run import SHOPRun, ShopRunEvent, SHOPRunList

DEFAULT_READ_LIMIT = 25


class SHOPRunAPI:
    def __init__(self, client: CogniteClient):
        self._cdf = client

    def trigger_case(self, case_file: str) -> SHOPRun:
        # Create the SHOP Run event
        # need to set manual_run to True
        raise NotImplementedError()

    def list(self, watercourse: str | list[str] | None = None, limit: int = DEFAULT_READ_LIMIT) -> SHOPRunList:
        """List the filtered SHOP runs.

        Args:
            watercourse: The watercourse to filter on.
            limit: The maximum number of SHOP runs to return. D

        Returns:
            A list of SHOP runs.
        """

        is_type = filters.Equals("type", ShopRunEvent.event_type)
        extra_filters = []
        if watercourse:
            watercourses = watercourse if isinstance(watercourse, list) else [watercourse]
            is_watercourse = filters.ContainsAny(["metadata", ShopRunEvent.watercourse], [watercourses])
            extra_filters.append(is_watercourse)

        selected = filters.And(is_type, *extra_filters)
        events = self._cdf.events.filter(selected, limit=limit)

        return SHOPRunList.load(events)

    @overload
    def retrieve(self, external_id: str) -> SHOPRun | None:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> SHOPRunList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], ignore_unknown_ids: bool = True
    ) -> SHOPRun | SHOPRunList | None:
        """
        Retrieve a SHOP run.

        Args:
            external_id: The external id(s) of the SHOP run(s) to retrieve.
            ignore_unknown_ids: Whether to ignore unknown ids or raise an error.

        Returns:
            The SHOP run(s). None if it is a single external id, and it is not found.
        """
        if isinstance(external_id, str):
            event = self._cdf.events.retrieve(external_id=external_id)
            if event is None:
                return None
            return SHOPRun.load(event)
        elif isinstance(external_id, Sequence):
            return SHOPRunList.load(
                self._cdf.events.retrieve_multiple(external_ids=external_id, ignore_unknown_ids=ignore_unknown_ids)
            )
        else:
            raise TypeError(f"Invalid type {type(external_id)} for external_id.")
