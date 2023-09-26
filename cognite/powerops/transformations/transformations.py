import abc
import arrow
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, ClassVar

import numpy as np
import pandas as pd
import yaml
from cognite.client import CogniteClient
from cognite.client.utils import ms_to_datetime
from cognite.powerops.utils.cdf.calls import retrieve_range

from pydantic import BaseModel

from logging import getLogger

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
        >>> relative_datapoints = {
        ...     0: 42,
        ...     60: 420,
        ...     1440: 4200,
        ... }
        >>> _relative_datapoints_to_series(relative_datapoints, start_time)
        2000-01-01 12:00:00      42
        2000-01-01 13:00:00     420
        2000-01-02 12:00:00    4200
        dtype: int64
    """
    datapoint_values = [dp.offset_value for dp in relative_datapoints] # consider creating class method for this
    return pd.Series(
        datapoint_values,
        index=[
            start_time + timedelta(minutes=int(dp.offset_minute) + shift_minutes) for dp in relative_datapoints
        ],
    )


class Transformation(BaseModel, abc.ABC):
    transformation_name: ClassVar[str]

    @classmethod
    def load(cls, transformation_: dict[str, Any]) -> "Transformation":
        # sourcery skip: move-assign
        (transformation_name,) = transformation_
        class_index = [cls.__name__ for cls in Transformation.__subclasses__()].index(transformation_name)
        if transformation_input := transformation_[transformation_name]["input"]:
            return Transformation.__subclasses__()[class_index].__call__(**transformation_input)
        else:
            return Transformation.__subclasses__()[class_index].__call__()

    @abc.abstractmethod
    def apply(self,
              time_series_data: pd.Series,
              ):
        ...

class Add(Transformation):
    transformation_name = "add" # using the class name instead or keep?
    value: float

    def apply(self,
              time_series_data: pd.Series,
              ):
        return time_series_data + self.value


class Multiply(Transformation):
    transformation_name = "multiply"
    value: float

    def apply(self,
              time_series_data: pd.Series,
              ):
        return time_series_data * self.value


class StaticValues(Transformation):
    transformation_name = "static_values"
    shift_minutes: int = 0  # This is given at runtime - have a set method for this? looks like default value of 0 is always used
    relative_datapoints: list[RelativeDatapoint]
    _start: Optional[int] = None

    @property
    def start(self) -> int:
        return self._start

    def set_start(self, value: int) -> None:
        self._start = value

    def apply(self, _: pd.Series) -> pd.Series:
        if not self.start:
            raise ValueError("start time has not been set")
        return _relative_datapoints_to_series(self.relative_datapoints, ms_to_datetime(self.start), self.shift_minutes)


class ToBool(Transformation):
    transformation_name = "to_bool"

    def apply(self, time_series_data: pd.Series) -> pd.Series:
        return (time_series_data > 0).astype(int)


class ZeroIfNotOne(Transformation):
    transformation_name = "zero_if_not_none"

    def apply(self, time_series_data: pd.Series) -> pd.Series:
        return (time_series_data == 1).astype(int)


class OneIfTwo(Transformation):
    transformation_name = "one_if_two"

    def apply(self, time_series_data: pd.Series) -> pd.Series:
        return (time_series_data == 2).astype(int)


class HeightToVolume(Transformation):
    transformation_name = "height_to_volume"
    _volumes: Optional[list[float]] = None
    _heights: Optional[list[float]] = None

    @property
    def volumes(self) -> list[float]:
        return self._volumes

    @property
    def heights(self) -> list[float]:
        return self._heights

    # TODO: run this function in preprocessor before running apply. Need a check on transformation name
    def set_and_prepare_volumes(self, model: dict, object_type: str, instance: str | int):
        self._volumes = model[object_type][instance]["vol_head"]["x"]
        self._heights = model[object_type][instance]["vol_head"]["y"]

    @staticmethod
    def height_to_volume(time_series_data: pd.Series, heights: list[float], volumes: list[float]) -> pd.Series:
        def interpolate(height: float) -> float:
            """Height to volume"""
            if height < min(heights) or max(heights) < height:
                logger.warning(f"Outside interpoaltion bounds [{min(heights)}, {max(heights)}]. Got {height}.")
            return float(np.interp(height, heights, volumes))

        return time_series_data.map(interpolate)

    def apply(self, time_series_data: pd.Series) -> pd.Series:
        if self.volumes and self.heights:
            return self.height_to_volume(time_series_data, self.heights, self.volumes)
        else:
            raise ValueError(f"Volumes and Heights properties had not "
                             f"been set for transformation {self.transformation_name}.")


class DoNothing(Transformation):
    transformation_name = "do_nothing"

    def apply(self, time_series_data: pd.Series) -> pd.Series:
        return time_series_data

class AddFromOffset(Transformation):
    transformation_name = "add_from_offset"
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
    transformation_name = "multiply_from_offset"
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


class AddWaterInTransit(Transformation):
    transformation_name = "add_water_in_transit"
    extra_ts_external_id: str
    transit_object_name: str  # either plant or gate (not to be confused with object name)
    _start: Optional[int] = None
    _end: Optional[int] = None
    _shape: Optional[Dict[int, float]] = None
    _discharge: Optional[pd.Series] = None

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end

    @property
    def shape(self):
        return self._shape

    @property
    def discharge(self):
        return self._discharge

    # run this first, then set_and_prepare_discharge_data
    def set_shop_times(self, start: int, end: int):
        self._start = start
        self._end = end

    def set_shape(self, value: Dict[int, float]):
        self._shape = value

    def set_discharge(self, value: pd.Series):
        self._discharge = value

    @staticmethod
    def get_shape(model: dict, object_type: str, object_name: str) -> Dict[int, float]:
        """object_type must be plant or gate"""
        gate_or_plant = model[object_type][object_name]  # Get description of gate/plant

        # Get shape and time_delay values as Dict[delay,water_percentage]
        if "shape_discharge" in gate_or_plant:
            shape = dict(zip(gate_or_plant["shape_discharge"]["x"], gate_or_plant["shape_discharge"]["y"]))
        elif "time_delay" in gate_or_plant:
            shape = {gate_or_plant["time_delay"]: 1}
        else:
            raise ValueError(f"{object_type}.{object_name} does not have a shape_discharge or time_delay attribute")

        return shape

    # TODO: run this function in preprocessor before running apply. Need a check on transformation name
    def prepare_discharge_data(self,
                     client: CogniteClient,
                     model: dict,
                     object_type: str,
                    ):

        if self.end and self.start:
            shape = self.get_shape(model=model, object_type=object_type, object_name=self.transit_object_name)
            self.set_shape(shape)  # not sure if this is the best way of dynamically changing the class attibutes..

            longest_delay = max(self.shape)  # longest delay in minutes
            longest_delay_ms = 60 * 1000 * longest_delay  # longest delay in milliseconds

            discharge = retrieve_range(  # Get discharge datapoints from time-series
                    client=client,
                    external_ids=[self.extra_ts_external_id],
                    start=self.start - longest_delay_ms,  # Shift start time based on longest delay
                    end=self.start,
            )[self.extra_ts_external_id]

            if discharge.empty:
                logger.warning("Cannot add 'water in transit' - did not get any 'discharge' datapoints!")

            self.set_discharge(discharge)

        else:
            raise ValueError("start and end times have not been set")


    @staticmethod
    def add_water_in_transit(
        inflow: pd.Series,
        discharge: pd.Series,
        shape: Dict[int, float],
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

    def apply(self,
              time_series_data: pd.Series,
    ) -> pd.Series:
        if time_series_data.empty:
            return time_series_data

        if hasattr(self, "discharge") and hasattr(self, "shape"):
            return self.add_water_in_transit(
                inflow=time_series_data,
                discharge=self.discharge,
                shape=self.shape,
                start=ms_to_datetime(self.start),
                end=ms_to_datetime(self.end),
            )

        else:
            raise ValueError(f"Preprocessing step for {self.transformation_name} has not run. Aborting")

#TODO: utility to test, remove later
def t(hour: int, minutes: int = 0) -> pd.Timestamp:
    """Simplify creation of datetimes"""
    return pd.Timestamp(2000, 1, 1, hour, minutes)


if __name__ == "__main__":

    ### A small test script ###

    dct = yaml.safe_load(
    '''
    transformations:
    - "Add":
          "input":
            value: 3.4
    - "AddFromOffset":
          "input":
            "relative_datapoints":
                - "offset_minute": 0
                  "offset_value": 50
                - "offset_minute": 60
                  "offset_value": 23
    - "MultiplyFromOffset":
          "input":
            "relative_datapoints":
                - "offset_minute": 0
                  "offset_value": 1
                - "offset_minute": 60
                  "offset_value": 0.5
    - "StaticValues":
          "input":
            "relative_datapoints":
                - "offset_minute": 0
                  "offset_value": 1
    - "ToBool":
          "input":
    - "Add":
        "input":
            value: 1.2
    ''')

    t_r = [t(1), t(2), t(2, 50), t(3, 10), t(4), t(5)]
    a_r = [1.2, 5, np.NaN, 6, np.NaN, 7]

    t_s = pd.Series(a_r, index=t_r)

    transformations = [Transformation.load(t) for t in dct["transformations"]]

    for t in transformations:
        if t.transformation_name == "static_values": # for some transformations, setter methods must run before "apply" in preprocessor
            t.set_start(arrow.get(t_r[0]).timestamp()*1000)
        res = t.apply(t_s)
        print(t.transformation_name, "\n", res)
