from collections import UserList
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

import pandas as pd
from cognite.client.data_classes.events import Event, EventList
from cognite.client.utils import ms_to_datetime
from typing_extensions import Self


class ShopRunEvent:
    event_type: ClassVar[str] = "POWEROPS_PROCESS_REQUESTED"
    event_subtype: ClassVar[str] = "POWEROPS_SHOP_RUN"
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

    @classmethod
    def load(cls, event: Event) -> Self:
        """
        Load a SHOP run from an event.

        Args:
            event: The event to load from.

        Returns:

        """
        metadata = event.metadata or {}
        return cls(
            external_id=event.external_id,
            watercourse=metadata.get(ShopRunEvent.watercourse, ""),
            start=ms_to_datetime(event.start_time),
            end=ms_to_datetime(event.end_time),
        )

    def dump(self) -> dict[str, str]:
        return {"external_id": self.external_id, "watercourse": self.watercourse, "start": self.start, "end": self.end}


class SHOPRunList(UserList):
    """
    This represents a list of SHOP runs.
    """

    @classmethod
    def load(cls, events: EventList) -> Self:
        return cls([SHOPRun.load(event) for event in events])

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame([run.dump() for run in self.data])

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()
