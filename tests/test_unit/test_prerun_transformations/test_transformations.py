from datetime import datetime, timedelta, timezone

from contextlib import nullcontext
import pandas as pd
import pytest
import pytz
from typing import Any
from dataclasses import dataclass, field
from cognite.client.data_classes import (
    Datapoints,
    TimeSeries,
)
from pydantic import ValidationError

from cognite.powerops.prerun_transformations.transformations import (
    AddConstant,
    AddFromOffset,
    AddWaterInTransit,
    DoNothing,
    HeightToVolume,
    MultiplyConstant,
    MultiplyFromOffset,
    OneIfTwo,
    RelativeDatapoint,
    Round,
    StaticValues,
    SumTimeseries,
    ToBool,
    ToInt,
    ZeroIfNotOne,
    ms_to_datetime_tz_naive,
)


@dataclass
class TransformationTestCase:
    """Test case for testing different transformation classes"""

    case_id: str
    input_values: list[int]
    expected_values: list[int]
    start_date: datetime
    relative_datapoints: list[RelativeDatapoint] | None = None


def test_ms_to_datetime_tz_naive():

    epoch_timezone = pytz.timezone("Etc/UTC")
    input_epoch_time = datetime(2022, 1, 1, 12, tzinfo=epoch_timezone)

    oslo_timezone = pytz.timezone("Europe/Oslo")
    input_oslo_time = datetime(2022, 1, 1, 12, tzinfo=oslo_timezone)

    input_ms_time = 1641038400000

    expected_time = datetime(2022, 1, 1, 12).replace(tzinfo=None)

    output_epoch_time = ms_to_datetime_tz_naive(input_epoch_time)
    output_oslo_time = ms_to_datetime_tz_naive(input_oslo_time)
    output_ms_time = ms_to_datetime_tz_naive(input_ms_time)

    assert output_epoch_time == expected_time == output_oslo_time == output_ms_time
    assert isinstance(output_epoch_time, datetime)
    assert isinstance(output_oslo_time, datetime)
    assert isinstance(output_ms_time, datetime)


def test_add_constant():

    constant = 10.5
    transformation = AddConstant(constant=constant)

    input_values = [10, 20, 30, 40, 50]
    expected_values = [value + constant for value in input_values]
    incremental_dates = pd.date_range(start="2022-01-01", periods=len(input_values), freq="D")

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_round():

    digits = 2
    transformation = Round(digits=digits)

    input_values = [10.1111, 20.2222, 30.3333, 40.4444, 50.5555]
    expected_values = [round(value, digits) for value in input_values]
    incremental_dates = pd.date_range(start="2022-01-01", periods=len(input_values), freq="D")

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_sum_timeseries_two_timeseries():

    input_values_1 = [42.0] * 5
    input_values_2 = [20.0] * 5

    start_time = "2022-01-01"
    incremental_dates_1 = pd.date_range(start=start_time, periods=len(input_values_1), freq="H")
    incremental_dates_2 = pd.date_range(start=start_time, periods=len(input_values_2), freq="2H")

    input_data_1 = pd.Series(input_values_1, index=incremental_dates_1)
    input_data_2 = pd.Series(input_values_2, index=incremental_dates_2)

    expected_dates = (
        pd.concat([pd.Series(incremental_dates_1), pd.Series(incremental_dates_2)]).sort_values().drop_duplicates()
    )
    expected_data = pd.Series([62, 42, 62, 42, 62, 20, 20], index=expected_dates)

    transformation = SumTimeseries()
    output_data = transformation.apply(time_series_data=(input_data_1, input_data_2))

    assert (output_data == expected_data).all()


def test_sum_timeseries_one_timeseries():

    input_values_1 = [42.0] * 5

    start_time = "2022-01-01"
    incremental_dates_1 = pd.date_range(start=start_time, periods=len(input_values_1), freq="H")

    input_data_1 = pd.Series(input_values_1, index=incremental_dates_1)

    expected_data = input_data_1

    transformation = SumTimeseries()
    output_data = transformation.apply(time_series_data=(input_data_1,))

    assert (output_data == expected_data).all()


def test_multiply_constant():

    constant = 10
    transformation = MultiplyConstant(constant=constant)

    input_values = [10, 20, 30, 40, 50]
    expected_values = [value * constant for value in input_values]

    incremental_dates = pd.date_range(start="2022-01-01", periods=len(input_values), freq="D")

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_static_values(cognite_client_mock):
    start_time = datetime(2022, 1, 1, 12, tzinfo=None)

    relative_datapoints = [
        RelativeDatapoint(offset_minute=0, offset_value=42),
        RelativeDatapoint(offset_minute=60, offset_value=420),
        RelativeDatapoint(offset_minute=1440, offset_value=4200),
    ]
    transformation = StaticValues(relative_datapoints=relative_datapoints)
    transformation.pre_apply(client=cognite_client_mock, shop_model={}, start=start_time, end=start_time)
    result = transformation.apply(_=pd.Series(range(10)))

    expected = pd.Series(
        [42, 420, 4200],
        index=[
            datetime(2022, 1, 1, 12, tzinfo=None),
            datetime(2022, 1, 1, 13, tzinfo=None),
            datetime(2022, 1, 2, 12, tzinfo=None),
        ],
    )

    assert (result == expected).all()


def test_to_bool():

    transformation = ToBool()

    input_values = [-1, -0.5, 0, 0.5, 1, 2]
    expected_values = [0, 0, 0, 1, 1, 1]

    incremental_dates = pd.date_range(start="2022-01-01", periods=len(input_values), freq="D")

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_to_int():

    transformation = ToInt()

    input_values = [-1.2, -0.5, 0.23, 0.5, 1, 2.75]
    expected_values = [-1, 0, 0, 0, 1, 3]

    incremental_dates = pd.date_range(start="2022-01-01", periods=len(input_values), freq="D")

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_zero_if_not_one():

    transformation = ZeroIfNotOne()

    input_values = [0, 1, 2, -1]
    expected_values = [0, 1, 0, 0]

    incremental_dates = pd.date_range(start="2022-01-01", periods=len(input_values), freq="D")

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_one_if_two():

    transformation = OneIfTwo()

    input_values = [0, 1, 2, -1]
    expected_values = [0, 0, 1, 0]

    incremental_dates = pd.date_range(start="2022-01-01", periods=len(input_values), freq="D")

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_height_to_volume(cognite_client_mock):
    heights = [2, 4, 6, 8, 10]
    volumes = [10, 20, 40, 80, 160]

    datapoints = pd.Series(
        {
            1: 1,  # below interpolation bounds
            2: 4,
            3: 6,
            4: 7,  # interpolated
            5: 11,  # above interpolation bounds
        }
    )

    transformation = HeightToVolume(object_type="reservoir", object_name="Nyhellervann")
    transformation.pre_apply(
        client=cognite_client_mock,
        shop_model={"reservoir": {"Nyhellervann": {"vol_head": {"x": volumes, "y": heights}}}},
        start=0,
        end=0,
    )

    output_data = transformation.apply(time_series_data=(datapoints,))

    expected_values = [10, 20, 40, 60, 160]
    expected_data = pd.Series(expected_values, index=range(1, len(expected_values) + 1))

    assert (output_data == expected_data).all()


def test_do_nothing():

    transformation = DoNothing()

    input_values = [-1.2, -0.5, 0.23, 0.5, 1, 2.75]

    incremental_dates = pd.date_range(start="2022-01-01", periods=len(input_values), freq="D")

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = input_data

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()

ADD_OFFSET_TEST_CASES = [
    TransformationTestCase(
        case_id="case_1",
        input_values=[42.0] * 6,
        expected_values=[43, 40, 40, 40, 40, 45, 45, 45],
        start_date=datetime(year=2022, month=1, day=1, hour=0, tzinfo=None),
        relative_datapoints = [
            RelativeDatapoint(offset_minute=0, offset_value=1),
            RelativeDatapoint(offset_minute=20, offset_value=-2),
            RelativeDatapoint(offset_minute=230, offset_value=3),
        ]
    ),
    # TransformationTestCase(
    #     case_id="case_2",
    #     input_values=[10.0] * 10,
    #     expected_values=[10, 30, 30, 5, 5, 5, 25, 25, 25, 25],
    #     start_date=datetime(year=2022, month=1, day=1, hour=0, tzinfo=None),
    #     relative_datapoints = [
    #         RelativeDatapoint(offset_minute=1, offset_value=3),
    #         RelativeDatapoint(offset_minute=3, offset_value=0.5),
    #         RelativeDatapoint(offset_minute=6, offset_value=2.5),
    #     ]
    # ),
    # TransformationTestCase(
    #     case_id="case_3",
    #     input_values=[10.0] * 6,
    #     expected_values=[10, 20, 0, 0, 0, 0],
    #     start_date=datetime(year=2022, month=1, day=1, hour=0, tzinfo=None),
    #     relative_datapoints = [
    #         RelativeDatapoint(offset_minute=1, offset_value=2),
    #         RelativeDatapoint(offset_minute=2, offset_value=0),
    #         RelativeDatapoint(offset_minute=6, offset_value=1.5),
    #     ]
    # ),
]

@pytest.mark.parametrize(
    "test_case",
    [
        pytest.param(test_case, id=test_case.case_id)
        for test_case in ADD_OFFSET_TEST_CASES
    ],
)
def test_add_from_offset(test_case):

    transformation = AddFromOffset(relative_datapoints=test_case.relative_datapoints)

    incremental_dates = pd.date_range(start=test_case.start_date, periods=len(test_case.input_values), freq="H")

    input_data = pd.Series(test_case.input_values, index=incremental_dates)

    expected_data = pd.Series(
        test_case.expected_values,
        index=[
            datetime(2022, 1, 1, 0, tzinfo=None),
            datetime(2022, 1, 1, 0, 20, tzinfo=None),
            datetime(2022, 1, 1, 1, tzinfo=None),
            datetime(2022, 1, 1, 2, tzinfo=None),
            datetime(2022, 1, 1, 3, tzinfo=None),
            datetime(2022, 1, 1, 3, 50, tzinfo=None),
            datetime(2022, 1, 1, 4, tzinfo=None),
            datetime(2022, 1, 1, 5, tzinfo=None),
        ],
    )

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


MULTIPLY_OFFSET_TEST_CASES = [
    TransformationTestCase(
        case_id="case_1",
        input_values=[10.0] * 6,
        expected_values=[10, 20, 0, 0, 15, 15],
        start_date=datetime(year=2022, month=1, day=1, hour=0, tzinfo=None),
        relative_datapoints = [
            RelativeDatapoint(offset_minute=1, offset_value=2),
            RelativeDatapoint(offset_minute=2, offset_value=0),
            RelativeDatapoint(offset_minute=4, offset_value=1.5),
        ]
    ),
    TransformationTestCase(
        case_id="case_2",
        input_values=[10.0] * 10,
        expected_values=[10, 30, 30, 5, 5, 5, 25, 25, 25, 25],
        start_date=datetime(year=2022, month=1, day=1, hour=0, tzinfo=None),
        relative_datapoints = [
            RelativeDatapoint(offset_minute=1, offset_value=3),
            RelativeDatapoint(offset_minute=3, offset_value=0.5),
            RelativeDatapoint(offset_minute=6, offset_value=2.5),
        ]
    ),
    TransformationTestCase(
        case_id="case_3",
        input_values=[10.0] * 6,
        expected_values=[10, 20, 0, 0, 0, 0],
        start_date=datetime(year=2022, month=1, day=1, hour=0, tzinfo=None),
        relative_datapoints = [
            RelativeDatapoint(offset_minute=1, offset_value=2),
            RelativeDatapoint(offset_minute=2, offset_value=0),
            RelativeDatapoint(offset_minute=6, offset_value=1.5),
        ]
    ),
]

@pytest.mark.parametrize(
    "test_case",
    [
        pytest.param(test_case, id=test_case.case_id)
        for test_case in MULTIPLY_OFFSET_TEST_CASES
    ],
)
def test_multiply_from_offset(test_case):
    transformation = MultiplyFromOffset(relative_datapoints=test_case.relative_datapoints)

    incremental_dates = pd.date_range(start=test_case.start_date, periods=len(test_case.input_values), freq="min")

    input_data = pd.Series(test_case.input_values, index=incremental_dates)

    expected_data = pd.Series(test_case.expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


@dataclass
class AddWaterInTransitTestCase:
    """Test case for testing the AddWaterInTransit class"""

    case_id: str
    discharge_ts_external_id: str = "discharge_ts"
    transit_object_type: str = "gate"
    transit_object_name: str = "gate1"
    shape_type: str = "shape_discharge"
    time_frequency: str = "h"
    shop_start_time: datetime = datetime(year=2022, month=5, day=20, hour=0, tzinfo=None)
    end_time: datetime | None = None  # defaults to x hours after shop_start_time where x is the len(inflow_values)
    discharge_start_time: datetime | None = None  # defaults to x hours before shop_start_time where x is the len(discharge_values)
    discharge_shape: Any = field(default_factory=lambda: {"x": [0, 180, 240, 300], "y": [0, 0.2, 0.6, 0.2]})
    discharge_values: list[int] = field(default_factory=lambda: [0, 0, 0, 10, 10, 0])
    inflow_values: list[int] = field(default_factory=lambda: [5] * 11)
    expected_shape: dict[int, float] = field(default_factory=lambda: {0: 0, 180: 0.2, 240: 0.6, 300: 0.2})
    expected_values: list[int] = field(default_factory=lambda: [7, 13, 13, 7] + [5] * 6)
    model: dict[Any, Any] | None = None
    error: type[Exception] | None = None


ADD_WATER_TEST_CASES = [
    AddWaterInTransitTestCase(case_id="default"),  # uses all the default values from the dataclass
    AddWaterInTransitTestCase(
        case_id="default_output_as_inflow",
        discharge_shape={"x": [0, 780, 840, 900, 960, 1020, 1080], "y": [0, 0.1, 0.25, 0.3, 0.2, 0.1, 0.05]},
        discharge_values=[3] * 19,
        inflow_values=[7, 13, 13, 7] + [5] * 22,
        expected_shape={0: 0, 780: 0.1, 840: 0.25, 900: 0.3, 960: 0.2, 1020: 0.1, 1080: 0.05},
        expected_values=[10, 16, 16, 10] + [8] * 9 + [7.7, 6.95, 6.05, 5.45, 5.15] + [5] * 7,
    ),
    AddWaterInTransitTestCase(
        case_id="no_discharge_applied",
        discharge_start_time=datetime(year=2022, month=5, day=18, hour=0, tzinfo=None),
        expected_values=[5] * 11  # same as the default inflow values
    ),
    AddWaterInTransitTestCase(
        case_id="frequency_2_hours",
        time_frequency="2h",
        end_time=datetime(year=2022, month=5, day=25, hour=0, tzinfo=None),
        discharge_start_time=datetime(year=2022, month=5, day=19, hour=2, tzinfo=None),
        discharge_shape={"x": [0, 120, 1320], "y": [0, 0.5, 0.5]},
        discharge_values=[1, 2, 3, 2, 4],
        inflow_values=[1, 2, 3, 2, 4, 5, 3, 1, 2, 0, 7, 5, 9, 0, 0, 9, 8, 7, 6, 5, 4, 7, 8, 9],
        expected_shape={0: 0, 120: 0.5, 1320: 0.5},
        expected_values=[3.5, 3.5, 3, 3, 4.5, 4.5, 3, 3, 6, 6, 7, 7, 5, 5, 3, 3, 4, 4, 2, 2, 9, 9, 5, 5, 9, 9, 0, 0, 0, 0, 9, 9, 8, 8, 7, 7, 6, 6, 5, 5, 4, 4, 7, 7, 8, 8, 9],
    ),
    AddWaterInTransitTestCase(
        case_id="basic",
        shape_type="shape_discharge",
        discharge_shape={"x": [0, 60, 120, 180], "y": [0, 0.5, 0.3, 0.2]},
        discharge_values=list(range(1, 7)),
        expected_shape={0: 0, 60: 0.5, 120: 0.3, 180: 0.2},
        expected_values=[10.3, 7.8, 6.2] + [5] * 7
    ),
    AddWaterInTransitTestCase(
        case_id="time_delay",
        shape_type="time_delay",
        discharge_shape=180,
        discharge_values=list(range(1, 7)),
        expected_shape={180: 1},
        expected_values=[9, 10, 11] + [5] * 7
    ),
    AddWaterInTransitTestCase(case_id="shape_validation_error", transit_object_type="foo", model={"foo": 0}, error=ValidationError),
    AddWaterInTransitTestCase(case_id="shape_type_error", model={"gate": 0}, error=TypeError),
    AddWaterInTransitTestCase(case_id="shape_type_error_2", model={"gate": {"gate1": 0}}, error=TypeError),
    AddWaterInTransitTestCase(case_id="shape_key_error", model={"gate": {"foo": 0}}, error=KeyError),
    AddWaterInTransitTestCase(case_id="shape_key_error_2", model={"foo": {"gate1": {"foo": 0}}}, error=KeyError),
    AddWaterInTransitTestCase(case_id="shape_value_error", model={"gate": {"gate1": {"foo": 0}}}, error=ValueError),
]


@pytest.mark.parametrize(
    "test_case",
    [
        pytest.param(test_case, id=test_case.case_id)
        for test_case in ADD_WATER_TEST_CASES
    ],
)
def test_add_water_in_transit(cognite_client_mock, test_case):

    with (pytest.raises(test_case.error) if test_case.error else nullcontext()):
        transformation = AddWaterInTransit(
            discharge_ts_external_id=test_case.discharge_ts_external_id,
            transit_object_type=test_case.transit_object_type,
            transit_object_name=test_case.transit_object_name,
        )

        if test_case.discharge_start_time:
            discharge_start_time = test_case.discharge_start_time
        else:
            discharge_start_time = test_case.shop_start_time - timedelta(hours=len(test_case.discharge_values))
        discharge_times = pd.date_range(start=discharge_start_time, periods=len(test_case.discharge_values), freq=test_case.time_frequency)
        discharge_times_ms = [time.replace(tzinfo=timezone.utc).timestamp() * 1000 for time in discharge_times]

        inflow_times = pd.date_range(start=test_case.shop_start_time, periods=len(test_case.inflow_values), freq=test_case.time_frequency)
        inflow_input_data = (pd.Series(test_case.inflow_values, index=inflow_times),)

        start_time_ms = inflow_times[0].replace(tzinfo=timezone.utc).timestamp() * 1000
        if not test_case.end_time:
            test_case.end_time = inflow_times[-1]
        end_time_ms = test_case.end_time.replace(tzinfo=timezone.utc).timestamp() * 1000

        if test_case.model:
            model = test_case.model
        else:
            model = {test_case.transit_object_type: {test_case.transit_object_name: {test_case.shape_type: test_case.discharge_shape}}}

        cognite_client_mock.time_series.data.retrieve.return_value = Datapoints(
            external_id=test_case.discharge_ts_external_id, value=test_case.discharge_values, timestamp=discharge_times_ms
        )
        cognite_client_mock.time_series.data.retrieve_latest.return_value = Datapoints(
            external_id=test_case.discharge_ts_external_id, value=[test_case.discharge_values[-1]], timestamp=[discharge_times_ms[-1]]
        )
        cognite_client_mock.time_series.retrieve_multiple.return_value = [TimeSeries(external_id=test_case.discharge_ts_external_id, is_step=True)]

        transformation.pre_apply(client=cognite_client_mock, shop_model=model, start=start_time_ms, end=end_time_ms)
        output_shape = transformation.shape
        output_data = transformation.apply(inflow_input_data)

        expected_timestamps = pd.date_range(start=test_case.shop_start_time, end=test_case.end_time - timedelta(hours=1), freq="1h")
        expected_data = pd.Series(test_case.expected_values, index=expected_timestamps[: len(test_case.expected_values)]).reindex(expected_timestamps).ffill()

        pd.testing.assert_series_equal(expected_data, output_data, check_dtype=False)
        assert output_shape == test_case.expected_shape
