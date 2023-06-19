from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union

import numpy as np
import pandas as pd
from cognite.client import CogniteClient

from cognite.powerops.preprocessor import knockoff_logging as logging
from cognite.powerops.preprocessor.exceptions import CogShopError
from cognite.powerops.preprocessor.utils import ms_to_datetime, retrieve_range

logger = logging.getLogger(__name__)

RelativeDatapoints = Dict[str, float]


def _relative_datapoints_to_series(
    relative_datapoints: RelativeDatapoints, start_time: datetime, shift_minutes: int = 0
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
    return pd.Series(
        relative_datapoints.values(),
        index=[
            start_time + timedelta(minutes=int(minute_offset) + shift_minutes) for minute_offset in relative_datapoints
        ],
    )


# TODO: can we reduce the number of parameters in `apply`?
# ? SimpleTransformation.apply(datatapoints) and ComplexTransformation.apply(<..all params..>)
class Transformation(ABC):
    def __init__(self, kwargs: Optional[dict] = None) -> None:
        pass

    @abstractmethod
    def apply(
        self,
        *_: Any,
        datapoints: pd.Series,
        model: dict,
        object_type: str,
        instance: Union[str, int],
        start: int,
        end: int,
        client: CogniteClient,
        shift_minutes: int = 0,
    ) -> pd.Series:
        ...


class Add(Transformation):
    def __init__(self, kwargs: dict) -> None:
        self.value = kwargs["value"]

    def apply(self, *_: Any, datapoints: pd.Series, **__: Any) -> pd.Series:
        return datapoints + self.value


class Multiply(Transformation):
    def __init__(self, kwargs: dict) -> None:
        self.value = kwargs["value"]

    def apply(self, *_: Any, datapoints: pd.Series, **__: Any) -> pd.Series:
        return datapoints * self.value


class StaticValues(Transformation):
    def __init__(self, kwargs: dict) -> None:
        self.relative_datapoints = kwargs

    def apply(self, *_: Any, start: int, shift_minutes: int = 0, **__: Any) -> pd.Series:
        return _relative_datapoints_to_series(self.relative_datapoints, ms_to_datetime(start), shift_minutes)


class ToBool(Transformation):
    @staticmethod
    def apply(*_: Any, datapoints: pd.Series, **__: Any) -> pd.Series:
        return (datapoints > 0).astype(int)


class ZeroIfNotOne(Transformation):
    @staticmethod
    def apply(*_: Any, datapoints: pd.Series, **__: Any) -> pd.Series:
        return (datapoints == 1).astype(int)


class OneIfTwo(Transformation):
    @staticmethod
    def apply(*_: Any, datapoints: pd.Series, **__: Any) -> pd.Series:
        return (datapoints == 2).astype(int)


class HeightToVolume(Transformation):
    @staticmethod
    def height_to_volume(datapoints, heights, volumes) -> pd.Series:
        def interpolate(height: float) -> float:
            """Height to volume"""
            if height < min(heights) or max(heights) < height:
                logger.warning(f"Outside interpoaltion bounds [{min(heights)}, {max(heights)}]. Got {height}.")
            return float(np.interp(height, heights, volumes))

        return datapoints.map(interpolate)

    def apply(
        self,
        *_: Any,
        datapoints: pd.Series,
        model: dict,
        object_type: str,
        instance: Union[str, int],
        **__: Any,
    ) -> pd.Series:
        volumes = model[object_type][instance]["vol_head"]["x"]
        heights = model[object_type][instance]["vol_head"]["y"]
        return self.height_to_volume(datapoints, heights, volumes)


class DoNothing(Transformation):
    @staticmethod
    def apply(*_: Any, datapoints: pd.Series, **__: Any) -> pd.Series:
        return datapoints


class AddFromOffset(Transformation):
    def __init__(self, kwargs) -> None:
        self.relative_datapoints = kwargs

    def apply(
        self,
        *_: Any,
        datapoints: pd.Series,
        shift_minutes: int = 0,
        **__: Any,
    ) -> pd.Series:
        first_timestamp = min(datapoints.index)
        non_relative_datapoints = _relative_datapoints_to_series(
            self.relative_datapoints, first_timestamp, shift_minutes
        )
        union_index = datapoints.index.union(non_relative_datapoints.index)
        # fillna(0) since we are adding
        non_relative_datapoints = non_relative_datapoints.reindex(union_index).ffill().fillna(0)
        # If a timestamp does not exist in the original datapoints we add that timestamp with a forwardfilled value
        return datapoints.reindex(union_index).ffill() + non_relative_datapoints


class MultiplyFromOffset(Transformation):
    def __init__(self, kwargs) -> None:
        self.relative_datapoints = kwargs

    def apply(
        self,
        *_: Any,
        datapoints: pd.Series,
        shift_minutes: int = 0,
        **__: Any,
    ) -> pd.Series:
        first_timestamp = min(datapoints.index)
        non_relative_datapoints = _relative_datapoints_to_series(
            self.relative_datapoints, first_timestamp, shift_minutes
        )
        union_index = datapoints.index.union(non_relative_datapoints.index)
        # fillna(1) since we are multiplying
        non_relative_datapoints = non_relative_datapoints.reindex(union_index).ffill().fillna(1)
        # If a timestamp does not exist in the original datapoints we add that timestamp with a forwardfilled value
        return datapoints.reindex(union_index).ffill() * non_relative_datapoints


class AddWaterInTransit(Transformation):
    def __init__(self, kwargs) -> None:
        self.kwargs = kwargs

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

    @staticmethod
    def get_shape(model: Dict, object_type: str, object_name: str) -> Dict[int, float]:
        """object_type must be plant or gate"""
        gate_or_plant = model[object_type][object_name]  # Get description of gate/plant

        # TODO: HANDLE REF VALUES

        # Get shape and time_delay values as Dict[delay,water_percentage]
        if "shape_discharge" in gate_or_plant:
            shape = dict(zip(gate_or_plant["shape_discharge"]["x"], gate_or_plant["shape_discharge"]["y"]))
        elif "time_delay" in gate_or_plant:
            shape = {gate_or_plant["time_delay"]: 1}
        else:
            raise ValueError(f"{object_type}.{object_name} does not have a shape_discharge or time_delay attribute")

        return shape

    def apply(
        self,
        *_: Any,
        datapoints: pd.Series,
        model: dict,
        start: int,
        end: int,
        client: CogniteClient,
        **__: Any,
    ) -> pd.Series:
        if datapoints.empty:
            return datapoints

        if "gate_name" in self.kwargs:
            shape = self.get_shape(model=model, object_type="gate", object_name=self.kwargs["gate_name"])
        else:  # ! Assumes that this is the only other option
            shape = self.get_shape(model=model, object_type="plant", object_name=self.kwargs["plant_name"])

        longest_delay = max(shape)  # longest delay in minutes
        longest_delay_ms = 60 * 1000 * longest_delay  # longest delay in milliseconds

        external_id = self.kwargs["external_id"]
        discharge = retrieve_range(  # Get discharge datapoints from time-series
            client=client,
            external_ids=[external_id],
            start=start - longest_delay_ms,  # Shift start time based on longest delay
            end=start,
        )[external_id]

        if discharge.empty:
            logger.warning("Cannot add 'water in transit' - did not get any 'discharge' datapoints!")
            return datapoints

        return self.add_water_in_transit(
            inflow=datapoints,
            discharge=discharge,
            shape=shape,
            start=ms_to_datetime(start),
            end=ms_to_datetime(end),
        )


_TRANSFORMATIONS = {
    "ADD": Add,
    "ADD_FROM_OFFSET": AddFromOffset,
    "DYNAMIC_ADD_FROM_OFFSET": AddFromOffset,
    "ADD_WATER_IN_TRANSIT": AddWaterInTransit,
    "MULTIPLY": Multiply,
    "MULTIPLY_FROM_OFFSET": MultiplyFromOffset,
    "TO_BOOL": ToBool,
    "ZERO_IF_NOT_ONE": ZeroIfNotOne,
    "GATE_SCHEDULE_FLAG_VALUE_MAPPING": ZeroIfNotOne,
    "GENERATOR_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING": ZeroIfNotOne,
    "PLANT_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING": ZeroIfNotOne,
    "ONE_IF_TWO": OneIfTwo,
    "STATIC": StaticValues,
    "DYNAMIC_STATIC": StaticValues,
    "RESERVOIR_LEVEL_TO_VOLUME": HeightToVolume,
    "DO_NOTHING": DoNothing,
    "GATE_OPENING_METER_TO_PERCENT": DoNothing,
}


def transformation_factory(transformation_type: str, kwargs: dict) -> Transformation:
    try:
        return _TRANSFORMATIONS[transformation_type](kwargs)
    except KeyError as e:
        raise CogShopError(f"{transformation_type} not recognized as a 'Transformation'!") from e
