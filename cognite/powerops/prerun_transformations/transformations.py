import inspect
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from logging import getLogger
from typing import Any, Literal

import arrow
import numpy as np
import pandas as pd
from cognite.client import CogniteClient
from pydantic import BaseModel
from typing_extensions import Self

from cognite.powerops.utils.cdf.calls import retrieve_range

logger = getLogger(__name__)


def ms_to_datetime_tz_naive(timestamp: int) -> datetime:
    """
    Milliseconds since Epoch to datetime.
    #TODO: Consider switching to cognite-sdk official one, but using this one for now as it is time zone naive
    """
    return arrow.get(timestamp).datetime.replace(tzinfo=None)


class RelativeDatapoint(BaseModel):
    """
    A relative datapoint that states from what minute to apply an offset value. To be used when for instance adding
    different offset values at different points in time from start time of an array of time series data.

    Args:
        offset_minute: The number of minutes from start time of an input array of time series data to apply a value
        offset_value: The value to apply to the existing value at the offset_minute point in time
    """

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
    @property
    def name(self):
        return self.__repr_name__()

    @classmethod
    def load(cls, transformation_: dict[str, Any]) -> Self:
        (transformation_name,) = transformation_
        if transformation_body := transformation_.get(transformation_name):
            return _TRANSFORMATIONS_BY_CLASS_NAME[transformation_name].__call__(**transformation_body["parameters"])
        elif transformation_name in _TRANSFORMATIONS_BY_CLASS_NAME:
            return _TRANSFORMATIONS_BY_CLASS_NAME[transformation_name].__call__()
        else:
            raise NotImplementedError(f"Unknown transformation {transformation_name}")

    # TODO: have  validator that checks that this dict (later json)
    # does not exceed the DM limit of 255 characters for a string?
    def parameters_to_dict(self) -> dict:
        return {}

    @classmethod
    def load_from_fdm(cls, transformation_type: str, transformation_parameters: dict) -> Self:
        if transformation_type in _TRANSFORMATIONS_RELATIVE_DATAPOINTS:
            transformation_parameters = {
                "relative_datapoints": [
                    {"offset_minute": m, "offset_value": v} for m, v in transformation_parameters.items()
                ]
            }
        return cls.load({transformation_type: {"parameters": transformation_parameters}})

    @abstractmethod
    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series: ...


class DynamicTransformation(Transformation):
    @abstractmethod
    def pre_apply(self, client: CogniteClient, shop_model: dict, start: int, end: int): ...


class AddConstant(Transformation):
    """
    Args:
        constant: The value to add to the time series data
    """

    constant: float

    def parameters_to_dict(self) -> dict:
        return {"constant": self.constant}

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """Add value to input time series

        Args:
            time_series_data: A tuple of time series data. Only the first one, time_series_data[0],
                                is used - the constant value will be added to every value of this time series

        Returns:
            The first pd.Series from the input tuple with the constant added to all values
        """
        single_ts = time_series_data[0]
        return single_ts + self.constant


class Round(Transformation):
    """
    Args:
        digits: The number of decimal places to round to
    """

    digits: int

    def parameters_to_dict(self) -> dict:
        return {"digits": self.digits}

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """Round the time series values to the specified number of decimals, using the "round half to even" method

        Args:
            time_series_data: A tuple of time series data. Only the first one, time_series_data[0],
                                is used - the round will be applied to every value of this time series

        Returns:
            The first pd.Series from the input tuple with the rounding applied to all values
        """

        single_ts = time_series_data[0]
        return single_ts.round(decimals=self.digits)


class SumTimeseries(Transformation):
    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """Sum two or more time series together in place without any interpolation between points

        Args:
            time_series_data: A tuple of time series data. Where all the time series in the tuple are
                                summed together

        Returns:
            Concatenated timeseries with values added together

        Example:
        ```python
        >>> timestamps = [
        ... datetime(2022, 1, 1, 0),
        ... datetime(2022, 1, 1, 1),
        ... datetime(2022, 1, 1, 2),
        ... datetime(2022, 1, 1, 3),
        ... datetime(2022, 1, 1, 4),
        ... datetime(2022, 1, 1, 5),
        ... ]
        >>> values = [42.0] * len(timestamps)
        >>> time_series_data1 = pd.Series(values, index=timestamps)
        >>> time_series_data1
        2022-01-01 00:00:00    42.0
        2022-01-01 01:00:00    42.0
        2022-01-01 02:00:00    42.0
        2022-01-01 03:00:00    42.0
        2022-01-01 04:00:00    42.0
        2022-01-01 05:00:00    42.0
        dtype: float64
        >>> timestamps2 = [t + timedelta(hours=timestamps.index(t)) for t in timestamps]
        >>> values2 = [20.0] * len(timestamps)
        >>> time_series_data2 = pd.Series(values2, index=timestamps2)
        >>> time_series_data2
        2022-01-01 00:00:00    20.0
        2022-01-01 02:00:00    20.0
        2022-01-01 04:00:00    20.0
        2022-01-01 06:00:00    20.0
        2022-01-01 08:00:00    20.0
        2022-01-01 10:00:00    20.0
        dtype: float64
        >>> time_series_data = (pd.Series(values, index=timestamps), pd.Series(values2, index=timestamps2))
        >>> c = SumTimeseries()
        >>> c.apply(time_series_data)
        2022-01-01 00:00:00    62.0
        2022-01-01 01:00:00    42.0
        2022-01-01 02:00:00    62.0
        2022-01-01 03:00:00    42.0
        2022-01-01 04:00:00    62.0
        2022-01-01 05:00:00    42.0
        2022-01-01 06:00:00    20.0
        2022-01-01 08:00:00    20.0
        2022-01-01 10:00:00    20.0
        dtype: float64

        ```
        """
        concatenated_df = pd.concat(time_series_data, axis=1).fillna(0)
        return concatenated_df.sum(axis=1)


class MultiplyConstant(Transformation):
    """
    Args:
        constant: The value to multiply the time series data with
    """

    constant: float

    def parameters_to_dict(self) -> dict:
        return {"constant": self.constant}

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """Multiply value to input time series

        Args:
            time_series_data: A tuple of time series data. Only the first one, time_series_data[0],
                                is used - the constant value will be multiplied to every value of this time series

        Returns:
            The resulting series when taking the first series in time_series_data and multiplying
            all values by a constant
        """
        single_ts = time_series_data[0]
        return single_ts * self.constant


class StaticValues(DynamicTransformation):
    """Provides a list of static values from SHOP start time.

    Args:
        relative_datapoints: The relative datapoints to apply to
    """

    relative_datapoints: list[RelativeDatapoint]
    _shift_minutes: int = 0  # This could be given at runtime - looks like default value of 0 is always used for now
    _pre_apply_has_run: bool = False
    _start: datetime

    def parameters_to_dict(self) -> dict:
        return {f"{int(r_point.offset_minute)}": f"{r_point.offset_value}" for r_point in self.relative_datapoints}

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, value: datetime):
        self._start = value

    @property
    def pre_apply_has_run(self):
        return self._pre_apply_has_run

    @pre_apply_has_run.setter
    def pre_apply_has_run(self, value: bool):
        self._pre_apply_has_run = value

    def pre_apply(self, client: CogniteClient, shop_model: dict, start: int, end: int):
        """Preprocessing step that needs to run before `apply()` to set the shop start time.

        Args:
            client: _ not used in this transformation, but needs to be provided
            shop_model: _ not used in this transformation, but needs to be provided
            start: datetime of SHOP start time

        Example:
        ```python
        from cognite.client import CogniteClient
        start_time = datetime.timestamp(datetime(2000, 1, 1, 12))
        end_time = datetime.timestamp(datetime(2000, 1, 5, 12))
        client = CogniteClient()
        model = {}
        relative_datapoints = [
        ...     RelativeDatapoint(offset_minute=0, offset_value=42),
        ...     RelativeDatapoint(offset_minute=1440, offset_value=4200),
        ... ]
        s = StaticValues(relative_datapoints=relative_datapoints)
        s.pre_apply(client=client, shop_model=model, start=start_time, end=end_time)
        ```
        """
        self.start = ms_to_datetime_tz_naive(start)
        self.pre_apply_has_run = True

    def apply(self, _: tuple[pd.Series]) -> pd.Series:
        """
        Returns:
            Pandas Series based from SHOP start time

        Example:
        ```python
        from cognite.client import CogniteClient
        start_time = datetime.timestamp(datetime(2000, 1, 1, 12))
        end_time = datetime.timestamp(datetime(2000, 1, 5, 12))
        client = CogniteClient()
        model = {}
        relative_datapoints = [
        ...     RelativeDatapoint(offset_minute=0, offset_value=42),
        ...     RelativeDatapoint(offset_minute=1440, offset_value=4200),
        ... ]
        s = StaticValues(relative_datapoints=relative_datapoints)
        s.pre_apply(client=client, shop_model=model, start=start_time, end=end_time)
        s.apply(_)
        2000-01-01 12:00:00      42.0
        2000-01-01 13:00:00      42.0
        2000-01-02 12:00:00    4200.0
        dtype: float64
        ```
        """
        if not self.pre_apply_has_run:
            raise ValueError("pre_apply function has not run - missing necessary properties to run transformation")
        return _relative_datapoints_to_series(self.relative_datapoints, self.start, self._shift_minutes)


class ToBool(Transformation):
    """
    "Greater than 0" transformation of time series data to a series of 0s and 1s. 1s if the value is > 0.
    """

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """
        Args:
            time_series_data: A tuple of time series data. Only the first one, time_series_data[0],
                                is used - every value of this time series will be converted to 0 or 1

        Returns:
            The transformed time series

        Example:
        ```python
        >>> values = [0, 1, 2, -1]
        >>> time_series_data = (pd.Series(
        ...            values,
        ...            index=pd.date_range(start='25/05/2021', periods = len(values)),
        ...        ),)
        >>> b = ToBool()
        >>> b.apply(time_series_data=time_series_data)
        2021-05-25    0
        2021-05-26    1
        2021-05-27    1
        2021-05-28    0
        Freq: D, dtype: int64

        ```
        """
        single_ts = time_series_data[0]
        return (single_ts > 0).astype("int64")


class ToInt(Transformation):
    """
    Transformation to round all values in a time series to get an Int using the "round half to even" method
    """

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """
        Args:
            time_series_data: A tuple of time series data. Only the first one, time_series_data[0],
                                is used - every value of this time series will be rounded to zero decimal places

        Returns:
            The transformed time series with all values rounded into integers given the
            default "round half to even" method
        """
        single_ts = time_series_data[0]
        return single_ts.apply(round)


class ZeroIfNotOne(Transformation):
    """
    Transforms time series data to a series of 0s and 1s. 1s if the value is exactly 1.
    """

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """
        Args:
            time_series_data: A tuple of time series data. Only the first one, time_series_data[0],
                                is used - every value of this time series will be converted to 0 or 1

        Returns:
            The transformed time series

        Example:
        ```python
        >>> values = [0, 1, 2, -1]
        >>> time_series_data = (pd.Series(
        ...            values,
        ...            index=pd.date_range(start='25/05/2021', periods = len(values)),
        ...        ),)
        >>> b = ZeroIfNotOne()
        >>> b.apply(time_series_data=time_series_data)
        2021-05-25    0
        2021-05-26    1
        2021-05-27    0
        2021-05-28    0
        Freq: D, dtype: int64

        ```
        """
        single_ts = time_series_data[0]
        return (single_ts == 1).astype("int64")


class OneIfTwo(Transformation):
    """
    Transforms time series data to a series of 0s and 1s. 1s if the value is exactly 2.
    """

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """
        Args:
            time_series_data: A tuple of time series data. Only the first one, time_series_data[0],
                                is used - every value of this time series will be converted to 0 or 1

        Returns:
            The transformed time series

        Example:
        ```python
        >>> values = [0, 1, 2, -1]
        >>> time_series_data = (pd.Series(
        ...            values,
        ...            index=pd.date_range(start='25/05/2021', periods = len(values)),
        ...        ),)
        >>> b = OneIfTwo()
        >>> b.apply(time_series_data=time_series_data)
        2021-05-25    0
        2021-05-26    0
        2021-05-27    1
        2021-05-28    0
        Freq: D, dtype: int64

        ```
        """
        single_ts = time_series_data[0]
        return (single_ts == 2).astype("int64")


class HeightToVolume(DynamicTransformation):
    """
    TODO
    """

    object_type: str
    object_name: str
    _pre_apply_has_run: bool = False
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
        return self._heights

    @heights.setter
    def heights(self, value: list[float]):
        self._heights = value

    @property
    def pre_apply_has_run(self):
        return self._pre_apply_has_run

    @pre_apply_has_run.setter
    def pre_apply_has_run(self, value: bool):
        self._pre_apply_has_run = value

    def parameters_to_dict(self) -> dict:
        return {"object_type": self.object_type, "object_name": self.object_name}

    @staticmethod
    def height_to_volume(time_series_data: pd.Series, heights: list[float], volumes: list[float]) -> pd.Series:
        def interpolate(height: float) -> float:
            """Height to volume"""
            if height < min(heights) or max(heights) < height:
                logger.warning(f"Outside interpolation bounds [{min(heights)}, {max(heights)}]. Got {height}.")
            return float(np.interp(height, heights, volumes))

        return time_series_data.map(interpolate)

    def pre_apply(self, client: CogniteClient, shop_model: dict, start: int, end: int):
        """Preprocessing step that needs to run before `apply()` to set the volumes and heights from shop case file.

        Args:
            client: _ not used in this transformation
            shop_model: SHOP model file
            start: _ not used in this transformation
            end: _ not used in this transformation

        Example:
        ```python
        from cognite.client import CogniteClient

        start_time = datetime.timestamp(datetime(2000, 1, 1, 12))
        end_time = datetime.timestamp(datetime(2000, 1, 10, 12))
        model = {"reservoir": {"Lundevatn": {"vol_head": {"x": [10, 20, 40, 80, 160], "y": [2, 4, 6, 8, 10]}}}}
        client = CogniteClient()
        h = HeightToVolume(object_type="reservoir", object_name="Lundevatn")
        h.pre_apply(client=client, shop_model=model, start=start_time, end=end_time)
        h.volumes
        Out[4]: [10, 20, 40, 80, 160]
        ```
        """
        self.volumes = shop_model[self.object_type][self.object_name]["vol_head"]["x"]
        self.heights = shop_model[self.object_type][self.object_name]["vol_head"]["y"]
        self.pre_apply_has_run = True

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """
        Args:
            time_series_data: A tuple of time series data. Only the first one, time_series_data[0],
                                is used - the height to volume will be applied to each value of this time series

        Returns:
            The transformed time series

        Example:
        ```python
        from cognite.client import CogniteClient
        start_time = datetime.timestamp(datetime(2000, 1, 1, 12))
        end_time = datetime.timestamp(datetime(2000, 1, 10, 12))
        model = {"reservoir": {"Lundevatn": {"vol_head": {"x": [10, 20, 40, 80, 160], "y": [2, 4, 6, 8, 10]}}}}
        client = CogniteClient()
        time_series_data = (pd.Series(
                {
                    1: 1,  # below interpolation bounds
                    2: 4,
                    3: 6,
                    4: 7,  # interpolated
                    5: 11,  # above interpolation bounds
                }
            ),)
        h = HeightToVolume(object_type="reservoir", object_name="Lundevatn")
        h.pre_apply(client=client, shop_model=model, start=start_time, end=end_time)
        h.apply(time_series_data=time_series_data)
        1     10.0
        2     20.0
        3     40.0
        4     60.0
        5    160.0
        dtype: float64
        ```
        """
        if self.pre_apply_has_run:
            single_ts = time_series_data[0]
            return self.height_to_volume(single_ts, self.heights, self.volumes)
        else:
            raise ValueError("pre_apply function has not run - missing necessary properties to run transformation")


class DoNothing(Transformation):
    """
    Don't apply any transformations, just return the unchanged Series
    """

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        return time_series_data[0]


class AddFromOffset(Transformation):
    """
    Adds values to input timeseries based on a list of relative datapoints with values to be added to corresponding
    offset minute from start time

    Args:
        relative_datapoints: The values to add to existing time series based at offset minute times from time series
    """

    relative_datapoints: list[RelativeDatapoint]
    _shift_minutes: int = 0

    def parameters_to_dict(self) -> dict:
        return {f"{int(r_point.offset_minute)}": f"{r_point.offset_value}" for r_point in self.relative_datapoints}

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """
        Args:
            time_series_data: A tuple of time series data. Only the first one, time_series_data[0],
                                is used - the add from offset will be applied to each value in this time series

        Example:
        ```python
        >>> timestamps = [
        ...        datetime(2022, 1, 1, 0),
        ...        datetime(2022, 1, 1, 1),
        ...        datetime(2022, 1, 1, 2),
        ...        datetime(2022, 1, 1, 3),
        ...        datetime(2022, 1, 1, 4),
        ...        datetime(2022, 1, 1, 5),
        ...    ]
        >>> values = [42.0] * len(timestamps)
        >>> time_series_data = (pd.Series(values, index=timestamps),)
        >>> time_series_data
        (2022-01-01 00:00:00    42.0
        2022-01-01 01:00:00    42.0
        2022-01-01 02:00:00    42.0
        2022-01-01 03:00:00    42.0
        2022-01-01 04:00:00    42.0
        2022-01-01 05:00:00    42.0
        dtype: float64,)
        >>> relative_datapoints = [
        ...     RelativeDatapoint(offset_minute=0, offset_value=1),
        ...     RelativeDatapoint(offset_minute=20, offset_value=-2),
        ...     RelativeDatapoint(offset_minute=230, offset_value=3),
        ... ]
        >>> a = AddFromOffset(relative_datapoints=relative_datapoints)
        >>> a.apply(time_series_data)
        2022-01-01 00:00:00    43.0
        2022-01-01 00:20:00    40.0
        2022-01-01 01:00:00    40.0
        2022-01-01 02:00:00    40.0
        2022-01-01 03:00:00    40.0
        2022-01-01 03:50:00    45.0
        2022-01-01 04:00:00    45.0
        2022-01-01 05:00:00    45.0
        dtype: float64

        ```
        """
        single_ts = time_series_data[0]
        first_timestamp = min(single_ts.index)
        non_relative_datapoints = _relative_datapoints_to_series(
            self.relative_datapoints, first_timestamp, self._shift_minutes
        )
        union_index = single_ts.index.union(non_relative_datapoints.index)
        # fillna(0) since we are adding
        non_relative_datapoints = non_relative_datapoints.reindex(union_index).ffill().fillna(0)
        # If a timestamp does not exist in the original datapoints we add that timestamp with a forwardfilled value
        return single_ts.reindex(union_index).ffill() + non_relative_datapoints


class MultiplyFromOffset(Transformation):
    """Multiplies values to input timeseries based on a list of relative datapoints

    Args:
        relative_datapoints: A tuple of time series data. Only the first one, time_series_data[0],
                                is used - the multiply from offset will be applied to each value in this time series
    """

    relative_datapoints: list[RelativeDatapoint]
    _shift_minutes: int = 0

    def parameters_to_dict(self) -> dict:
        return {f"{int(r_point.offset_minute)}": f"{r_point.offset_value}" for r_point in self.relative_datapoints}

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """
        Example:
        ```python
        >>> timestamps = [datetime(2022, 1, 1) + timedelta(minutes=i) for i in range(6)]
        >>> values = [10.0] * 6
        >>> time_series_data = (pd.Series(values, index=timestamps),)
        >>> time_series_data
        (2022-01-01 00:00:00    10.0
        2022-01-01 00:01:00    10.0
        2022-01-01 00:02:00    10.0
        2022-01-01 00:03:00    10.0
        2022-01-01 00:04:00    10.0
        2022-01-01 00:05:00    10.0
        dtype: float64,)
        >>> relative_datapoints = [
        ...     RelativeDatapoint(offset_minute=1, offset_value=2),
        ...     RelativeDatapoint(offset_minute=2, offset_value=0),
        ...     RelativeDatapoint(offset_minute=4, offset_value=1.5),
        ... ]
        >>> m = MultiplyFromOffset(relative_datapoints=relative_datapoints)
        >>> m.apply(time_series_data)
        2022-01-01 00:00:00    10.0
        2022-01-01 00:01:00    20.0
        2022-01-01 00:02:00     0.0
        2022-01-01 00:03:00     0.0
        2022-01-01 00:04:00    15.0
        2022-01-01 00:05:00    15.0
        Freq: min, dtype: float64

        ```
        """
        single_ts = time_series_data[0]
        first_timestamp = min(single_ts.index)
        non_relative_datapoints = _relative_datapoints_to_series(
            self.relative_datapoints, first_timestamp, self._shift_minutes
        )
        union_index = single_ts.index.union(non_relative_datapoints.index)
        # fillna(1) since we are multiplying
        non_relative_datapoints = non_relative_datapoints.reindex(union_index).ffill().fillna(1)
        # If a timestamp does not exist in the original datapoints we add that timestamp with a forwardfilled value
        return single_ts.reindex(union_index).ffill() * non_relative_datapoints


class AddWaterInTransit(DynamicTransformation, arbitrary_types_allowed=True):
    """Adds water in transit (previously discharged water) to the inflow time series.

    Args:
        discharge_ts_external_id: external id of discharge timeseries to retrieve from CDF
        transit_object_type: gate or plant
        transit_object_name: name of gate or plant
    """

    discharge_ts_external_id: str
    transit_object_type: Literal["plant", "gate"]
    transit_object_name: str
    _pre_apply_has_run: bool = False
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

    @property
    def pre_apply_has_run(self):
        return self._pre_apply_has_run

    @pre_apply_has_run.setter
    def pre_apply_has_run(self, value: bool):
        self._pre_apply_has_run = value

    def parameters_to_dict(self) -> dict:
        return {
            "discharge_ts_external_id": self.discharge_ts_external_id,
            "transit_object_type": self.transit_object_type,
            "transit_object_name": self.transit_object_name,
        }

    @staticmethod
    def get_shape(model: dict, transit_object_type: str, transit_object_name: str) -> dict[int, float]:
        gate_or_plant = model[transit_object_type][transit_object_name]  # Get description of gate/plant

        # Get shape and time_delay values as Dict[delay,water_percentage]
        if "shape_discharge" in gate_or_plant:
            shape = dict(
                zip(gate_or_plant["shape_discharge"]["x"], gate_or_plant["shape_discharge"]["y"], strict=False)
            )
        elif "time_delay" in gate_or_plant:
            shape = {gate_or_plant["time_delay"]: 1}
        else:
            raise ValueError(
                f"{transit_object_type}.{transit_object_name} does not have a shape_discharge or time_delay attribute"
            )

        return shape

    def pre_apply(self, client: CogniteClient, shop_model: dict, start: int, end: int):
        """Preprocessing step that needs to run before `apply()` to set the shape,
           retrieve and set discharge time series data, and set SHOP start and end times

        Args:
            client: CogniteClient authenticated to project to retrieve discharge timeseries from
            shop_model: SHOP model dict
            start: SHOP start time in milliseconds since epoch
            end: SHOP end time in milliseconds since epoch

        Example:
        ```python
        from cognite.client import CogniteClient
        start_time = datetime.timestamp(datetime(2000, 1, 1, 12))
        end_time = datetime.timestamp(datetime(2000, 1, 5, 12))
        client = CogniteClient()
        model = {"gate": {"gate1": {"shape_discharge": {"ref": 0, "x": [0, 60, 120], "y": [0.1, 0.5, 0.4]}}}}
        t = AddWaterInTransit(discharge_ts_external_id="discharge_ts",
        ...                       transit_object_type="gate",
        ...                       transit_object_name="gate1")
        t.pre_apply(client=client, shop_model=model, start=start_time, end=end_time)
        t.shape
        {0: 0.1, 60: 0.5, 120: 0.4}
        ```
        """
        self.start = start
        self.end = end

        self.shape = self.get_shape(
            model=shop_model, transit_object_type=self.transit_object_type, transit_object_name=self.transit_object_name
        )

        longest_delay = max(self.shape)  # longest delay in minutes
        longest_delay_ms = 60 * 1000 * longest_delay  # longest delay in milliseconds

        discharge = retrieve_range(  # Get discharge datapoints from time-series
            client=client,
            external_ids=[self.discharge_ts_external_id],
            start=self.start - longest_delay_ms,  # Shift start time based on longest delay
            end=self.start,
        )[self.discharge_ts_external_id]

        if discharge.empty:
            logger.warning("Cannot add 'water in transit' - did not get any 'discharge' datapoints!")

        self.discharge = discharge
        self.pre_apply_has_run = True

    @staticmethod
    def add_water_in_transit(
        inflow: pd.Series, discharge: pd.Series, shape: dict[int, float], start: datetime, end: datetime
    ) -> pd.Series:
        # Forward fill discharge for all (hour) timestamps until start
        one_hour = pd.Timedelta("1h")
        if start - one_hour not in discharge.index:
            discharge[start - one_hour] = np.nan
        discharge = discharge.resample(one_hour).ffill().ffill()

        # Make sure inflow has values for all (hour) timestamps up until end (exclusive)
        if end not in inflow.index:
            inflow[end] = np.nan
        inflow = inflow.resample(one_hour).ffill().ffill()

        for delay, water_percentage in shape.items():
            delayed_discharge = discharge.shift(delay, freq="min") * water_percentage
            inflow = inflow.add(delayed_discharge, fill_value=0)

        # Only include delayed discharge that will affect the selected time range
        between_start_and_end = (start <= inflow.index) & (inflow.index < end)

        return inflow.loc[between_start_and_end]

    def apply(self, time_series_data: tuple[pd.Series]) -> pd.Series:
        """Run `apply()` after preprocessing step to add water in transit to add water in transit (discharge water) to
           inflow time series

        Args:
            time_series_data: A tuple of time series data representing inflow. Only the first one, time_series_data[0],
                                is used

        Example:
        ```python
        from cognite.client import CogniteClient
        start = datetime(year=2022, month=5, day=20, hour=22)
        end = start + timedelta(days=5)
        start_time = datetime.timestamp(start) * 1000
        end_time = datetime.timestamp(end) * 1000
        client = CogniteClient()
        model = {"gate": {"gate1": {"shape_discharge": {"ref": 0, "x": [0, 60, 120], "y": [0.1, 0.5, 0.4]}}}}
        t = AddWaterInTransit(discharge_ts_external_id="discharge_ts",
        ...                       transit_object_type="gate",
        ...                       transit_object_name="gate1")
        t.pre_apply(client=client, shop_model=model, start=start_time, end=end_time)
        inflow = [1, 2, 3, 2, 4, 5, 3, 1, 2, 0, 7, 5, 9, 0, 0, 9, 8, 7, 6, 5, 4, 7, 8, 9]
        timestamps = [start + timedelta(hours=2 * i) for i in range(len(inflow))]
        time_series_data = (pd.Series(inflow, index=timestamps),)
        t.apply(time_series_data)
        Out[1]:
        2022-05-20 22:00:00    3.5
        2022-05-20 23:00:00    3.5
        2022-05-21 00:00:00    3.0
        2022-05-21 01:00:00    3.0
        2022-05-21 02:00:00    4.5
                              ...
        2022-05-25 17:00:00    9.0
        2022-05-25 18:00:00    9.0
        2022-05-25 19:00:00    9.0
        2022-05-25 20:00:00    9.0
        2022-05-25 21:00:00    9.0
        Freq: H, Length: 120, dtype: float64

        ```
        """
        single_ts = time_series_data[0]
        if single_ts.empty or self.discharge.empty:
            return single_ts

        if self.pre_apply_has_run:
            return self.add_water_in_transit(
                inflow=single_ts,
                discharge=self.discharge,
                shape=self.shape,
                start=ms_to_datetime_tz_naive(self.start),
                end=ms_to_datetime_tz_naive(self.end),
            )
        else:
            raise ValueError("pre_apply function has not run - missing necessary properties to run transformation")


_TRANSFORMATIONS_BY_CLASS_NAME = dict(
    inspect.getmembers(sys.modules[__name__], lambda member: inspect.isclass(member) and member.__module__ == __name__)
)

_TRANSFORMATIONS_RELATIVE_DATAPOINTS = ["StaticValues", "AddFromOffset", "MultiplyFromOffset"]
