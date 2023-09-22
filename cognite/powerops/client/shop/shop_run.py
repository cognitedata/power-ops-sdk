from __future__ import annotations

import json
from collections import UserList
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, cast, overload

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes.events import Event
from cognite.client.utils import datetime_to_ms, ms_to_datetime
from typing_extensions import Self

try:
    from enum import StrEnum
except ImportError:
    from strenum import StrEnum


class SHOPRunStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    IN_PROGRESS = "in_progress"


class SHOPProcessEvents:
    finished: str = "POWEROPS_PROCESS_FINISHED"
    failed: str = "POWEROPS_PROCESS_FAILED"
    started: str = "POWEROPS_PROCESS_STARTED"


class ShopRunEvent:
    event_type: str = "POWEROPS_SHOP_RUN"
    watercourse: str = "shop:watercourse"
    manual_run: str = "shop:manual_run"
    preprocessor_data: str = "shop:preprocessor_data"
    shop_version: str = "shop_version"
    case_file: str = "cog_shop_case_file"
    shop_files: str = "cog_shop_file_list"
    shopstart: str = "shop:starttime"
    shopend: str = "shop:endtime"
    user_id: str = "user:identifier"


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
        shop_version: The version of SHOP used for the SHOP run.
        user_id: The user ID of the user that triggered the SHOP run.
    """

    external_id: str
    watercourse: str
    start: datetime | None
    end: datetime | None
    shop_version: str
    source: str | None
    _case_file_external_id: str
    _shop_files: list[SHOPFile]
    _client: CogniteClient = field(repr=False)
    _run_event_types: set[str] = field(init=False, default_factory=set)

    @classmethod
    def load(cls, event: Event) -> Self:
        """
        Load a SHOP run from an event.

        Args:
            event: The event to load from.

        Returns:

        """
        metadata = event.metadata or {}
        if event.type != ShopRunEvent.event_type:
            raise ValueError(f"Event {event.external_id} is not a SHOP run event!")

        if event._cognite_client is None:
            raise ValueError(f"Event {event.external_id} is not loaded with a cognite client!")

        if preprocessor_raw := metadata.get(ShopRunEvent.preprocessor_data, None):
            preprocessor_data = json.loads(preprocessor_raw)
        else:
            preprocessor_data = {}

        return cls(
            external_id=event.external_id,
            watercourse=metadata.get(ShopRunEvent.watercourse, ""),
            start=ms_to_datetime(event.start_time) if event.start_time else None,
            end=ms_to_datetime(event.end_time) if event.end_time else None,
            shop_version=preprocessor_data.get(ShopRunEvent.shop_version, ""),
            _case_file_external_id=preprocessor_data.get(ShopRunEvent.case_file, {}).get("external_id"),
            _shop_files=[SHOPFile.load(item) for item in preprocessor_data.get(ShopRunEvent.shop_files, [])],
            _client=event._cognite_client,
            source=event.source,
        )

    @property
    def case_file_external_id(self) -> str:
        return self._case_file_external_id

    @property
    def shop_files_external_ids(self) -> list[dict[str, Any]]:
        return [shop_file.dump() for shop_file in self._shop_files]

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
            source=self.source,
        )

    def dump(self) -> dict[str, Any]:
        """
        Dump the SHOP run to a dictionary.

        Returns:
            A dictionary representation of the SHOP run.
        """
        return {
            "external_id": self.external_id,
            "watercourse": self.watercourse,
            "start": self.start,
            "end": self.end,
            "case_file_external_id": self.case_file_external_id,
            "shop_files_external_ids": self.shop_files_external_ids,
            "shop_version": self.shop_version,
        }

    def _download_file(self, external_id: str) -> str:
        bytes = self._client.files.download_bytes(external_id=external_id)
        try:
            return bytes.decode("utf-8")
        except UnicodeDecodeError:
            # SHOP sometimes writes files with Windows-1252 encoding (ASCII)
            # Trying to recover.
            return bytes.decode("Windows-1252")

    def get_case_file(self) -> str:
        """
        Get the case file for the SHOP run.

        Returns:
            The case file as a string.
        """
        return self._download_file(self._case_file_external_id)

    def get_shop_files(self) -> Iterable[str]:
        """
        Get the SHOP files for the SHOP run.

        Returns:
            A generator of strings, where each string is a SHOP file.
        """
        for shop_file in self._shop_files:
            yield self._download_file(shop_file.external_id)

    def get_log_files(self) -> Iterable[tuple[str, str]]:
        """
        Get the log files for the SHOP run. This is, for exampl,e the CPLEX output.

        Returns:
            A generator of tuples of the form (external_id, file_content).
        """
        if self.check_status() is not SHOPRunStatus.SUCCESS:
            raise ValueError("Cannot retrieve result files for a SHOP run that has not finished successfully.")
        relationship = self._client.relationships.list(source_external_ids=[self.external_id], target_types=["file"])
        for rel in relationship:
            yield rel.target_external_id, self._download_file(rel.target_external_id)

    def check_status(self) -> SHOPRunStatus:
        """
        Check the status of the SHOP run.

        This does a request to CDF to check whether the SHOP run has finished or failed.

        Returns:
            A SHOP run status.

        """
        return self._check_status(update_events=True)

    def _check_status(self, update_events: bool = False) -> SHOPRunStatus:
        # If the run is finished or failed, we don't need to check the events.
        if SHOPProcessEvents.failed in self._run_event_types:
            return SHOPRunStatus.FAILED
        elif SHOPProcessEvents.finished in self._run_event_types:
            return SHOPRunStatus.SUCCESS

        if not update_events:
            return SHOPRunStatus.IN_PROGRESS

        self._update_run_events()
        return self._check_status(update_events=False)

    def _update_run_events(self) -> None:
        relationships = self._client.relationships.list(
            source_external_ids=[self.external_id], target_types=["event"], fetch_resources=True
        )
        self._run_event_types |= {cast(Event, rel.target).type for rel in relationships}

    def _repr_html_(self) -> str:
        return pd.Series(self.dump()).to_frame().rename(columns={0: "Value"})._repr_html_()


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

    # The dunder implementations is to get proper type hints
    def __getitem__(self, item: int | slice) -> SHOPRunList | SHOPRun:
        if isinstance(item, slice):
            return type(self)(self.data[item])
        return self.data[item]

    def __iter__(self) -> Iterator[SHOPRun]:
        return super().__iter__()

    @classmethod
    def load(cls, events: Sequence[Event]) -> Self:
        """
        Load a SHOP run list from a list of events.

        Args:
            events: The events to load from. Must be SHOP run events.

        Returns:
            A SHOP run list.
        """
        return cls([SHOPRun.load(event) for event in events])

    def to_pandas(self) -> pd.DataFrame:
        """
        Convert the SHOP run list to a pandas DataFrame.

        Returns:
            A pandas DataFrame.
        """
        return pd.DataFrame([run.dump() for run in self.data])

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()
