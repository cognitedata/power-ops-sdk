from __future__ import annotations

import json
from collections import UserList
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar, overload

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes.events import Event, EventList
from cognite.client.utils import ms_to_datetime
from typing_extensions import Self


class ShopRunEvent:
    event_type: ClassVar[str] = "POWEROPS_SHOP_RUN"
    watercourse: ClassVar[str] = "shop:watercourse"


@dataclass
class SHOPRun:
    """
    This represents a single SHOP run.

    A SHOP run is represented by an event in CDF. This class is a wrapper around the event.

    Args:
        external_id: The external ID of the SHOP run. This matches the external ID of the event in CDF.
        watercourse: The watercourse of the SHOP run.
        start: The start time of the SHOP run.
        end: The end time of the SHOP run.
    """

    external_id: str
    watercourse: str
    start: datetime
    end: datetime
    shop_version: str
    _case_file_external_id: str
    _shop_files_external_ids: list[str]
    _client: CogniteClient = field(repr=False)

    @classmethod
    def load(cls, event: Event) -> Self:
        """
        Load a SHOP run from an event.

        Args:
            event: The event to load from.

        Returns:

        """
        metadata = event.metadata or {}
        if event.type != ShopRunEvent.event_type or "shop:preprocessor_data" not in metadata:
            raise ValueError(f"Event {event.external_id} is not a SHOP run event!")

        if event._cognite_client is None:
            raise ValueError(f"Event {event.external_id} is not loaded with a cognite client!")

        preprocessor_data = json.loads(metadata["shop:preprocessor_data"])

        # TODO: Validate the preprocessor data
        return cls(
            external_id=event.external_id,
            watercourse=metadata.get(ShopRunEvent.watercourse, ""),
            start=ms_to_datetime(event.start_time),
            end=ms_to_datetime(event.end_time),
            shop_version=preprocessor_data["shop_version"],
            _case_file_external_id=preprocessor_data["cog_shop_case_file"]["external_id"],
            _shop_files_external_ids=[item["external_id"] for item in preprocessor_data["cog_shop_file_list"]],
            _client=event._cognite_client,
        )

    def dump(self) -> dict[str, Any]:
        return {
            "external_id": self.external_id,
            "watercourse": self.watercourse,
            "start": self.start,
            "end": self.end,
            "case_file_external_id": self._case_file_external_id,
            "_shop_files_external_ids": self._shop_files_external_ids,
        }

    def get_case_file(self) -> str:
        bytes = self._client.files.download_bytes(external_id=self._case_file_external_id)
        return bytes.decode("utf-8")

    def get_shop_files(self) -> Iterable[str]:
        for shop_file in self._shop_files_external_ids:
            bytes = self._client.files.download_bytes(external_id=shop_file)
            yield bytes.decode("utf-8")


class SHOPRunList(UserList):
    """
    This represents a list of SHOP runs.
    """

    @overload
    def __getitem__(self, item: int) -> SHOPRun:
        ...

    @overload
    def __getitem__(self, item: slice) -> SHOPRunList:
        ...

    def __getitem__(self, item: int | slice) -> SHOPRunList | SHOPRun:
        if isinstance(item, slice):
            return type(self)(self.data[item])
        return self.data[item]

    @classmethod
    def load(cls, events: EventList) -> Self:
        return cls([SHOPRun.load(event) for event in events])

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame([run.dump() for run in self.data])

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()
