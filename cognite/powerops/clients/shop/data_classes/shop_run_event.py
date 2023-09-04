from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cached_property
from typing import ClassVar, Optional
from uuid import uuid4

from cognite.client.data_classes import Event


@dataclass
class ShopRunEvent:
    """
    A cut-down variant of `common.workflow_utils.ShopRun` from
    power-ops-functions repo. This variant has no workflow event and
    no mappings.
    """

    event_type: ClassVar[str] = "POWEROPS_PROCESS_REQUESTED"
    event_subtype: ClassVar[str] = "POWEROPS_SHOP_RUN"
    process_type: ClassVar[str] = "POWEROPS_SHOP_RUN"
    # Add event timestamps
    event_start_time: int
    event_end_time: int

    watercourse: str
    # metadata remains unchanged
    starttime: str
    endtime: str

    source: str
    timeresolution: Optional[dict[str, int]] = None
    dynamic_minute_offset: Optional[int] = None
    dm_case: Optional[str] = None
    dm_space: Optional[str] = None
    manual_run: bool = False

    def __post_init__(self):
        if self.starttime:
            self.starttime = str(self.starttime)
        if self.endtime:
            self.endtime = str(self.endtime)
        if self.timeresolution:
            self.timeresolution = {str(k): v for k, v in self.timeresolution.items()}
        self.source = "manual" if self.manual_run else "workflow"

    @cached_property
    def external_id(self) -> str:
        return f"{self.process_type}_{uuid4()}"

    @property
    def metadata(self) -> dict:
        specific_metadata = {
            "shop:watercourse": self.watercourse,
            "shop:starttime": self.starttime,
            "shop:endtime": self.endtime,
            "process_type": self.process_type,
            "shop:manual_run": self.manual_run,
        }
        if self.dm_case is not None:
            specific_metadata["dm:case"] = self.dm_case
        if self.dm_space is not None:
            specific_metadata["dm:space"] = self.dm_space
        if self.timeresolution is not None:
            specific_metadata["shop:timeresolution"] = json.dumps(self.timeresolution)
        if self.dynamic_minute_offset is not None:
            specific_metadata["shop:dynamic_minute_offset"] = str(self.dynamic_minute_offset)
        return specific_metadata

    def to_dict(self, dataset_id: int) -> dict:
        return {
            "external_id": self.external_id,
            "data_set_id": dataset_id,
            "type": self.event_type,
            "subtype": self.event_subtype,
            "event_start_time": self.event_start_time,
            "event_end_time": self.event_end_time,
            "metadata": self.metadata,
            "source": self.source or ("manual" if self.manual_run else "workflow"),
        }

    def to_event(self: ShopRunEvent, dataset_id: int) -> Event:
        return Event(**self.to_dict(dataset_id))

    @classmethod
    def from_event(cls, event: Event) -> ShopRunEvent:
        instance = ShopRunEvent(
            event_start_time=event.start_time,
            event_end_time=event.end_time,
            watercourse=event.metadata["shop:watercourse"],
            starttime=event.metadata["shop:starttime"],
            endtime=event.metadata["shop:endtime"],
            timeresolution=json.loads(event.metadata.get("shop:timeresolution", "null")),
            dynamic_minute_offset=event.metadata.get("shop:dynamic_minute_offset"),
            manual_run=event.metadata.get("manual_run", False),
        )
        if event.external_id:
            instance.external_id = event.external_id
        return instance
