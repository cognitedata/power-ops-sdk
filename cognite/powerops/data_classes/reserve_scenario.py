from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Literal, Optional, Tuple

from cognite.powerops.data_classes.common import AggregationMethod, RetrievalType
from cognite.powerops.data_classes.time_series_mapping import TimeSeriesMapping, TimeSeriesMappingEntry
from cognite.powerops.data_classes.transformation import Transformation, TransformationType


class Auction(str, Enum):
    week = "week"
    weekend = "weekend"


# TODO: Switch to class inheriting from str and Enum, or 3.11 strEnum?
Product = Literal["up", "down"]
Block = Literal["day", "night"]


# ! TODO: what about daylight saving ??
def generate_reserve_schedule(volume: int, n_days: int, night: bool = False) -> Dict[str, int]:
    """Creates the argument for the mapping transformation function for RKOM reserve obligation.
        Some examples:
            {"0": 10, "60": 0}
                --> Obligation of 10MW the first hour, then no obligation
            {"60": 10, "120": 0, "60": 10}
                --> No obligation the first hour (implicit), then 10 MW for one hour,
                    then no obligation for one hour, and finally 10 MW for the rest of the *period*

    Args:
        volume (int): volume in MW
        n_days (int): the number of days to generate the schedule for.
        night (bool, optional): True if the obligation is during the night. Defaults to False.

    Returns:
        Dict[str, int]: The "obligation schedule".
    """
    if volume == 0:
        return {"0": 0}

    res = {}
    minutes_five_hours = 5 * 60
    minutes_day = 24 * 60
    for day in range(n_days):
        midnight = day * minutes_day
        at_5AM = day * minutes_day + minutes_five_hours
        res[f"{midnight}"] = volume if night else 0
        res[f"{at_5AM}"] = 0 if night else volume
    if not night:
        # only needed if last value was != 0 (i.e. during day)
        res[f"{minutes_day * n_days}"] = 0
    return res


@dataclass
class ReserveScenario:
    volume: int
    auction: Auction
    product: Product
    block: Block
    reserve_group: str
    mip_plant_time_series: List[
        Tuple[str, Optional[str]]
    ]  # (plant, mip_flag_time_series) for plants with generators that are in the reserve group
    obligation_external_id: Optional[str] = None

    def __post_init__(self):
        if len(self.mip_plant_time_series) == 0:
            raise ValueError("No `mip_plants` specified!")

    @property
    def obligation_transformations(self) -> List[Transformation]:
        # TODO: move some of this logic
        n_days = 5 if self.auction == "week" else 2
        night = self.block == "night"
        kwargs = generate_reserve_schedule(volume=self.volume, n_days=n_days, night=night)
        if self.obligation_external_id:
            return [Transformation(transformation=TransformationType.DYNAMIC_ADD_FROM_OFFSET, kwargs=kwargs)]
        else:
            return [Transformation(transformation=TransformationType.DYNAMIC_STATIC, kwargs=kwargs)]

    def mip_flag_transformations(self, mip_time_series: Optional[str]) -> List[Transformation]:
        if not mip_time_series:
            # Just run with_mip flag the entire period
            return [Transformation(transformation=TransformationType.STATIC, kwargs={"0": 1})]

        # Run with mip_flag during bid, in addition to what is already set
        n_days = 5 if self.auction == "week" else 2
        n_minutes = n_days * 24 * 60
        kwargs = {0: 1, n_minutes: 0}
        return [
            Transformation(transformation=TransformationType.DYNAMIC_ADD_FROM_OFFSET, kwargs=kwargs),
            Transformation(transformation=TransformationType.TO_BOOL),
        ]

    @property
    def obligation_object_type(self) -> str:
        return "reserve_group"

    @property
    def obligation_attribute_name(self) -> str:
        if self.product == "up":
            return "rr_up_obligation"
        elif self.product == "down":
            return "rr_down_obligation"

    def to_time_series_mapping(self) -> TimeSeriesMapping:
        obligation = TimeSeriesMappingEntry(
            object_type=self.obligation_object_type,
            object_name=self.reserve_group,
            attribute_name=self.obligation_attribute_name,
            time_series_external_id=self.obligation_external_id,
            transformations=self.obligation_transformations,
            retrieve=RetrievalType.RANGE if self.obligation_external_id else None,
        )

        mip_flags = [
            TimeSeriesMappingEntry(
                object_type="plant",
                object_name=plant_name,
                attribute_name="mip_flag",
                time_series_external_id=mip_time_series,
                transformations=self.mip_flag_transformations(mip_time_series),
                retrieve=RetrievalType.RANGE if mip_time_series else None,
                aggregation=AggregationMethod.max,  # TODO: or `AggregationMethod.first`?
            )
            for plant_name, mip_time_series in self.mip_plant_time_series
        ]

        return TimeSeriesMapping(rows=[obligation, *mip_flags])
