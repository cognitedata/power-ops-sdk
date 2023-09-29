import inspect
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from logging import getLogger
from typing import Any, Literal

import numpy as np
import pandas as pd
from cognite.client import CogniteClient
from cognite.client.utils import ms_to_datetime
from pydantic import BaseModel
from typing_extensions import Self

from cognite.powerops.utils.cdf.calls import retrieve_range

logger = getLogger(__name__)


class RelativeDatapoint(BaseModel):
    offset_minute: float
    offset_value: float


def _relative_datapoints_to_series(
    relative_datapoints: list[RelativeDatapoint], start_time: datetime, shift_minutes: int = 0
) -> pd.Series:
    """Converts 'relative datapoints' to a pandas Series

    Example:
        >>> start_time = datetime(2000, 1, 1, 12)
        >>> relative_datapoints = [
        ...     RelativeDatapoint(offset_minute=0, offset_value=42),
        ...     RelativeDatapoint(offset_minute=60, offset_value=420),
        ...     RelativeDatapoint(offset_minute=1440, offset_value=4200),
        ... ]
        >>> _relative_datapoints_to_series(relative_datapoints, start_time)
        2000-01-01 12:00:00      42.0
        2000-01-01 13:00:00     420.0
        2000-01-02 12:00:00    4200.0
        dtype: float64
    """
    datapoint_values = [dp.offset_value for dp in relative_datapoints]  # consider creating class method for this
    return pd.Series(
        datapoint_values,
        index=[start_time + timedelta(minutes=int(dp.offset_minute) + shift_minutes) for dp in relative_datapoints],
    )


class Transformation(BaseModel, ABC):
    @classmethod
    def load(cls, transformation_: dict[str, Any]) -> Self:
        (transformation_name,) = transformation_
        if transformation_body := transformation_.get(transformation_name):
            return _TRANSFORMATIONS_BY_CLASS_NAME[transformation_name].__call__(**transformation_body["input"])
        else:
            return _TRANSFORMATIONS_BY_CLASS_NAME[transformation_name].__call__()

    @abstractmethod
    def apply(
        self,
        time_series_data: pd.Series,
    ):
        ...


class DynamicTransformation(Transformation):
    @abstractmethod
    def pre_apply(self, client: CogniteClient, shop_model: dict, start: int, end: int):
        ...


class Add(Transformation):
    value: float

    def apply(
        self,
        time_series_data: pd.Series,
    ):
        return time_series_data + self.value


class Multiply(Transformation):
    value: float

    def apply(
        self,
        time_series_data: pd.Series,
    ):
        return time_series_data * self.value


class StaticValues(DynamicTransformation):
    shift_minutes: int = 0  # This could be given at runtime - looks like default value of 0 is always used for now
    relative_datapoints: list[RelativeDatapoint]
    pre_apply_has_run: bool = False
    _start: int

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, value: int):
        self._start = value

    def pre_apply(self, client: CogniteClient, shop_model: dict, start: int, end: int):
        self.start = start
        self.pre_apply_has_run = True

    def apply(self, _: pd.Series) -> pd.Series:
        if not self.pre_apply_has_run:
            raise ValueError("pre_apply function has not run - missing neccessary properties to run transformation")
        return _relative_datapoints_to_series(self.relative_datapoints, ms_to_datetime(self.start), self.shift_minutes)


class ToBool(Transformation):
    def apply(self, time_series_data: pd.Series) -> pd.Series:
        return (time_series_data > 0).astype(int)


class ZeroIfNotOne(Transformation):
    def apply(self, time_series_data: pd.Series) -> pd.Series:
        return (time_series_data == 1).astype(int)


class OneIfTwo(Transformation):
    def apply(self, time_series_data: pd.Series) -> pd.Series:
        return (time_series_data == 2).astype(int)


class HeightToVolume(DynamicTransformation):
    object_type: str
    object_name: str
    pre_apply_has_run: bool = False
    _volumes: list[float]
    _heights: list[float]

    @property
    def volumes(self):
        return self._volumes

    @volumes.setter
    def volumes(self, value: list[float]):
        self._volumes = value

    @property
    def heights(self):
        return self._volumes

    @heights.setter
    def heights(self, value: list[float]):
        self._heights = value

    @staticmethod
    def height_to_volume(time_series_data: pd.Series, heights: list[float], volumes: list[float]) -> pd.Series:
        def interpolate(height: float) -> float:
            """Height to volume"""
            if height < min(heights) or max(heights) < height:
                logger.warning(f"Outside interpoaltion bounds [{min(heights)}, {max(heights)}]. Got {height}.")
            return float(np.interp(height, heights, volumes))

        return time_series_data.map(interpolate)

    def pre_apply(self, client: CogniteClient, shop_model: dict, start: int, end: int):
        self.volumes = shop_model[self.object_type][self.object_name]["vol_head"]["x"]
        self.heights = shop_model[self.object_type][self.object_name]["vol_head"]["y"]
        self.pre_apply_has_run = True

    def apply(self, time_series_data: pd.Series) -> pd.Series:
        if self.pre_apply_has_run:
            return self.height_to_volume(time_series_data, self.heights, self.volumes)
        else:
            raise ValueError("pre_apply function has not run - missing neccessary properties to run transformation")


class DoNothing(Transformation):
    def apply(self, time_series_data: pd.Series) -> pd.Series:
        return time_series_data


class AddFromOffset(Transformation):
    shift_minutes: int = 0
    relative_datapoints: list[RelativeDatapoint]

    def apply(self, time_series_data: pd.Series) -> pd.Series:
        first_timestamp = min(time_series_data.index)
        non_relative_datapoints = _relative_datapoints_to_series(
            self.relative_datapoints, first_timestamp, self.shift_minutes
        )
        union_index = time_series_data.index.union(non_relative_datapoints.index)
        # fillna(0) since we are adding
        non_relative_datapoints = non_relative_datapoints.reindex(union_index).ffill().fillna(0)
        # If a timestamp does not exist in the original datapoints we add that timestamp with a forwardfilled value
        return time_series_data.reindex(union_index).ffill() + non_relative_datapoints


class MultiplyFromOffset(Transformation):
    shift_minutes: int = 0
    relative_datapoints: list[RelativeDatapoint]

    def apply(self, time_series_data: pd.Series) -> pd.Series:
        first_timestamp = min(time_series_data.index)
        non_relative_datapoints = _relative_datapoints_to_series(
            self.relative_datapoints, first_timestamp, self.shift_minutes
        )
        union_index = time_series_data.index.union(non_relative_datapoints.index)
        # fillna(1) since we are multiplying
        non_relative_datapoints = non_relative_datapoints.reindex(union_index).ffill().fillna(1)
        # If a timestamp does not exist in the original datapoints we add that timestamp with a forwardfilled value
        return time_series_data.reindex(union_index).ffill() * non_relative_datapoints


class AddWaterInTransit(DynamicTransformation, arbitrary_types_allowed=True):
    discharge_ts_external_id: str
    transit_object_type: Literal["plant", "gate"]
    transit_object_name: str
    pre_apply_has_run: bool = False
    _start: int
    _end: int
    _shape: dict[int, float]
    _discharge: pd.Series

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, value: int):
        self._start = value

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, value: int):
        self._end = value

    @property
    def shape(self):
        return self._shape

    @shape.setter
    def shape(self, value: dict[int, float]):
        self._shape = value

    @property
    def discharge(self):
        return self._discharge

    @discharge.setter
    def discharge(self, value: pd.Series):
        self._discharge = value

    @staticmethod
    def get_shape(model: dict, transit_object_type: str, transit_object_name: str) -> dict[int, float]:
        """object_type must be plant or gate"""
        gate_or_plant = model[transit_object_type][transit_object_name]  # Get description of gate/plant

        # Get shape and time_delay values as Dict[delay,water_percentage]
        if "shape_discharge" in gate_or_plant:
            shape = dict(zip(gate_or_plant["shape_discharge"]["x"], gate_or_plant["shape_discharge"]["y"]))
        elif "time_delay" in gate_or_plant:
            shape = {gate_or_plant["time_delay"]: 1}
        else:
            raise ValueError(
                f"{transit_object_type}.{transit_object_name} does not have a shape_discharge or time_delay attribute"
            )

        return shape

    def pre_apply(self, client: CogniteClient, shop_model: dict, start: int, end: int):
        self.start = start
        self.end = end

        self.shape = self.get_shape(
            model=shop_model, object_type=self.transit_object_type, object_name=self.transit_object_name
        )

        longest_delay = max(self.shape)  # longest delay in minutes
        longest_delay_ms = 60 * 1000 * longest_delay  # longest delay in milliseconds

        discharge = retrieve_range(  # Get discharge datapoints from time-series
            client=client,
            external_ids=[self.extra_ts_external_id],
            start=self.start - longest_delay_ms,  # Shift start time based on longest delay
            end=self.start,
        )[self.discharge_ts_external_id]

        if discharge.empty:
            logger.warning("Cannot add 'water in transit' - did not get any 'discharge' datapoints!")

        self.discharge = discharge
        self.pre_apply_has_run = True

    @staticmethod
    def add_water_in_transit(
        inflow: pd.Series,
        discharge: pd.Series,
        shape: dict[int, float],
        start: datetime,
        end: datetime,
    ) -> pd.Series:
        """Adds water in transit (previously discharged water) to the inflow time series.

        Args:
            shape (Dict[int, float]): Description of "how much water is delayed by what amount of time".
                e.g.
                shape = {
                    0: 0,       # 0% instantly
                    120: 0.5,   # 50% delayed for 2h
                    1320: 0.5,  # 50% delayed for 22h
                }

        """
        # Forward fill discharge for all (hour) timestamps until start
        one_hour = pd.Timedelta("1h")
        if start - one_hour not in discharge.index:
            discharge[start - one_hour] = np.NaN
        discharge = discharge.resample(one_hour).ffill().ffill()

        # Make sure inflow has values for all (hour) timestamps up until end (exclusive)
        if end not in inflow.index:
            inflow[end] = np.NaN
        inflow = inflow.resample(one_hour).ffill().ffill()

        for delay, water_percentage in shape.items():
            delayed_discharge = discharge.shift(delay, freq="min") * water_percentage
            inflow = inflow.add(delayed_discharge, fill_value=0)

        # Only include delayed discharge that will affect the selected time range
        between_start_and_end = (start <= inflow.index) & (inflow.index < end)
        return inflow.loc[between_start_and_end]

    def apply(
        self,
        time_series_data: pd.Series,
    ) -> pd.Series:
        if time_series_data.empty:
            return time_series_data

        if self.pre_apply_has_run:
            return self.add_water_in_transit(
                inflow=time_series_data,
                discharge=self.discharge,
                shape=self.shape,
                start=ms_to_datetime(self.start),
                end=ms_to_datetime(self.end),
            )
        else:
            raise ValueError("pre_apply function has not run - missing neccessary properties to run transformation")


_TRANSFORMATIONS_BY_CLASS_NAME = dict(
    inspect.getmembers(sys.modules[__name__], lambda member: inspect.isclass(member) and member.__module__ == __name__)
)
