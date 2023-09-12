from __future__ import annotations

import json
from collections import UserList
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, ClassVar, overload

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes.events import Event, EventList
from cognite.client.utils import datetime_to_ms, ms_to_datetime
from typing_extensions import Self


class ShopRunEvent:
    event_type: ClassVar[str] = "POWEROPS_SHOP_RUN"
    watercourse: ClassVar[str] = "shop:watercourse"
    manual_run: str = "shop:manual_run"
    preprocessor_data: str = "shop:preprocessor_data"
    shop_version: str = "shop_version"
    case_file: str = "cog_shop_case_file"
    shop_files: str = "cog_shop_file_list"
    shopstart: str = "shop:starttime"
    shopend: str = "shop:endtime"


@dataclass
class SHOPFile:
    external_id: str
    file_type: str

    @classmethod
    def load(cls, data: dict[str, Any]) -> Self:
        return cls(external_id=data["external_id"], file_type=data["file_type"])

    def dump(self) -> dict[str, Any]:
        return {"external_id": self.external_id, "file_type": self.file_type}


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
    end: datetime | None
    shop_version: str
    _case_file_external_id: str
    _shop_files: list[SHOPFile]
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
        if event.type != ShopRunEvent.event_type or ShopRunEvent.preprocessor_data not in metadata:
            raise ValueError(f"Event {event.external_id} is not a SHOP run event!")

        if event._cognite_client is None:
            raise ValueError(f"Event {event.external_id} is not loaded with a cognite client!")

        preprocessor_data = json.loads(metadata[ShopRunEvent.preprocessor_data])

        # TODO: Validate the preprocessor data
        return cls(
            external_id=event.external_id,
            watercourse=metadata.get(ShopRunEvent.watercourse, ""),
            start=ms_to_datetime(event.start_time),
            end=ms_to_datetime(event.end_time),
            shop_version=preprocessor_data[ShopRunEvent.shop_version],
            _case_file_external_id=preprocessor_data[ShopRunEvent.case_file]["external_id"],
            _shop_files=[SHOPFile.load(item) for item in preprocessor_data[ShopRunEvent.shop_files]],
            _client=event._cognite_client,
        )

    def as_cdf_event(self, data_set_id: int) -> Event:
        return Event(
            external_id=self.external_id,
            type=ShopRunEvent.event_type,
            data_set_id=data_set_id,
            start_time=datetime_to_ms(self.start),
            end_time=datetime_to_ms(self.end) if self.end else None,
            metadata={
                ShopRunEvent.watercourse: self.watercourse,
                ShopRunEvent.manual_run: "",
                ShopRunEvent.preprocessor_data: json.dumps(
                    {
                        ShopRunEvent.shop_version: self.shop_version,
                        ShopRunEvent.case_file: {"external_id": self._case_file_external_id},
                        ShopRunEvent.shop_files: [shop_file.dump() for shop_file in self._shop_files],
                    }
                ),
                # These are required by the SHOP container
                # In the functions, create_bid_process_event the end is by default 2 weeks into the future.
                ShopRunEvent.shopstart: self.start.isoformat(),
                ShopRunEvent.shopend: (self.start + timedelta(days=14)).isoformat(),
            },
        )

    def dump(self) -> dict[str, Any]:
        return {
            "external_id": self.external_id,
            "watercourse": self.watercourse,
            "start": self.start,
            "end": self.end,
            "case_file_external_id": self._case_file_external_id,
            "shop_files_external_ids": [shop_file.dump() for shop_file in self._shop_files],
        }

    def get_case_file(self) -> str:
        bytes = self._client.files.download_bytes(external_id=self._case_file_external_id)
        return bytes.decode("utf-8")

    def get_shop_files(self) -> Iterable[str]:
        for shop_file in self._shop_files:
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
