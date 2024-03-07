from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import pytest
import pytz
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


@pytest.mark.parametrize(
    "input,expected",
    [
        (datetime(2022, 1, 1, 12, tzinfo=pytz.timezone("Etc/UTC")), datetime(2022, 1, 1, 12).replace(tzinfo=None)),
        (datetime(2022, 1, 1, 12, tzinfo=pytz.timezone("Europe/Oslo")), datetime(2022, 1, 1, 12).replace(tzinfo=None)),
        (1641038400000, datetime(2022, 1, 1, 12).replace(tzinfo=None)),
    ],
)
def test_ms_to_datetime_tz_naive(input, expected):
    output = ms_to_datetime_tz_naive(input)

    assert output == expected
    assert isinstance(output, datetime)


@dataclass
class TransformationTestCase:
    """Test case for testing different transformation classes"""

    case_id: str
    expected_values: list[float]
    transformation: (
        AddConstant
        | Round
        | MultiplyConstant
        | ToBool
        | ToInt
        | ZeroIfNotOne
        | OneIfTwo
        | DoNothing
        | MultiplyFromOffset
        | AddFromOffset
    )
    input_values: list[float] = field(default_factory=list)
    start_date: datetime = datetime(year=2022, month=1, day=1, hour=0, tzinfo=None)
    frequency: str = "D"
    expected_times: list[datetime] = field(default_factory=list)


TRANSFORMATION_TEST_CASES = [
    # AddConstant test cases
    TransformationTestCase(
        case_id="AddConstant: constant 10.5",
        transformation=AddConstant(constant=10.5),
        input_values=[10, 20, 30, 40, 50],
        expected_values=[20.5, 30.5, 40.5, 50.5, 60.5],
    ),
    TransformationTestCase(
        case_id="AddConstant: empty input",
        transformation=AddConstant(constant=10.5),
        input_values=[],
        expected_values=[],
    ),
    TransformationTestCase(
        case_id="AddConstant: constant -15.25, length 1",
        transformation=AddConstant(constant=-15.25),
        input_values=[5],
        expected_values=[-10.25],
    ),
    # Round test cases
    TransformationTestCase(
        case_id="Round: digits 2, no 5s",
        transformation=Round(digits=2),
        input_values=[10.1111, 20.2222, 30.3333, 40.4444],
        expected_values=[10.11, 20.22, 30.33, 40.44],
    ),
    TransformationTestCase(
        case_id="Round: digits 0, no 5s",
        transformation=Round(digits=0),
        input_values=[10.1111, 20.2222, 30.3333, 40.4444],
        expected_values=[10, 20, 30, 40],
    ),
    TransformationTestCase(
        case_id="Round: digits -1, with 5s",
        transformation=Round(digits=-1),
        input_values=[15, 15.5, 10.4, 14.4],
        expected_values=[20, 20, 10, 10],
    ),
    TransformationTestCase(
        case_id="Round: digits 0, with 5s",
        transformation=Round(digits=0),
        input_values=[0.5, 1.5, 2.5, 3.5],
        expected_values=[0, 2, 2, 4],
    ),
    TransformationTestCase(
        case_id="Round: digits 1, with 5s",
        transformation=Round(digits=1),
        input_values=[1.25, 1.35],
        expected_values=[1.2, 1.4],
    ),
    # MultiplyConstant test cases
    TransformationTestCase(
        case_id="MultiplyConstant: constant 10",
        transformation=MultiplyConstant(constant=10),
        input_values=[10, 20, 30, 40, 50],
        expected_values=[100, 200, 300, 400, 500],
    ),
    TransformationTestCase(
        case_id="MultiplyConstant: empty input",
        transformation=MultiplyConstant(constant=10.5),
        input_values=[],
        expected_values=[],
    ),
    TransformationTestCase(
        case_id="MultiplyConstant: constant -15.25",
        transformation=MultiplyConstant(constant=-15.25),
        input_values=[5],
        expected_values=[-76.25],
    ),
    # ToBool test cases
    TransformationTestCase(
        case_id="ToBool: default",
        transformation=ToBool(),
        input_values=[-1, -0.5, 0, 0.5, 1, 2],
        expected_values=[0, 0, 0, 1, 1, 1],
    ),
    TransformationTestCase(
        case_id="ToBool: empty input",
        transformation=ToBool(),
        input_values=[],
        expected_values=[],
    ),
    # ToInt test cases
    TransformationTestCase(
        case_id="ToInt: default",
        transformation=ToInt(),
        input_values=[-1.2, -0.5, 0.23, 0.5, 1, 2.75],
        expected_values=[-1, 0, 0, 0, 1, 3],
    ),
    # ZeroIfNotOne test cases
    TransformationTestCase(
        case_id="ZeroIfNotOne: default",
        transformation=ZeroIfNotOne(),
        input_values=[0, 1, 2, -1],
        expected_values=[0, 1, 0, 0],
    ),
    # OneIfTwo test cases
    TransformationTestCase(
        case_id="OneIfTwo: default",
        transformation=OneIfTwo(),
        input_values=[0, 1, 2, -1, -2],
        expected_values=[0, 0, 1, 0, 0],
    ),
    TransformationTestCase(
        case_id="OneIfTwo: empty input",
        transformation=OneIfTwo(),
        input_values=[],
        expected_values=[],
    ),
    # DoNothing test cases
    TransformationTestCase(
        case_id="DoNothing: default",
        transformation=DoNothing(),
        input_values=[-1.2, -0.5, 0.23, 0.5, 1, 2.75],
        expected_values=[-1.2, -0.5, 0.23, 0.5, 1, 2.75],
    ),
    # MultiplyFromOffset test cases
    TransformationTestCase(
        case_id="MultiplyFromOffset: case_1",
        transformation=MultiplyFromOffset(
            relative_datapoints=[
                RelativeDatapoint(offset_minute=1, offset_value=2),
                RelativeDatapoint(offset_minute=2, offset_value=0),
                RelativeDatapoint(offset_minute=4, offset_value=1.5),
            ]
        ),
        input_values=[10.0] * 6,
        expected_values=[10, 20, 0, 0, 15, 15],
        frequency="min",
    ),
    TransformationTestCase(
        case_id="MultiplyFromOffset: case_2",
        transformation=MultiplyFromOffset(
            relative_datapoints=[
                RelativeDatapoint(offset_minute=1, offset_value=3),
                RelativeDatapoint(offset_minute=3, offset_value=-0.5),
                RelativeDatapoint(offset_minute=6, offset_value=2.5),
            ]
        ),
        input_values=[10.0] * 10,
        expected_values=[10, 30, 30, -5, -5, -5, 25, 25, 25, 25],
        frequency="min",
    ),
    TransformationTestCase(
        case_id="MultiplyFromOffset: case_3",
        transformation=MultiplyFromOffset(
            relative_datapoints=[
                RelativeDatapoint(offset_minute=1, offset_value=2),
                RelativeDatapoint(offset_minute=2, offset_value=0),
                RelativeDatapoint(offset_minute=7, offset_value=1.5),
            ]
        ),
        input_values=[10.0] * 6,
        expected_values=[10, 20, 0, 0, 0, 0, 15],
        expected_times=[
            datetime(2022, 1, 1, 0, 0, tzinfo=None),
            datetime(2022, 1, 1, 0, 1, tzinfo=None),
            datetime(2022, 1, 1, 0, 2, tzinfo=None),
            datetime(2022, 1, 1, 0, 3, tzinfo=None),
            datetime(2022, 1, 1, 0, 4, tzinfo=None),
            datetime(2022, 1, 1, 0, 5, tzinfo=None),
            datetime(2022, 1, 1, 0, 7, tzinfo=None),  # TODO: are we supposed to include the datapoint outside of input
        ],
        frequency="min",
    ),
    # AddFromOffset test cases
    TransformationTestCase(
        case_id="AddFromOffset: case_1",
        transformation=AddFromOffset(
            relative_datapoints=[
                RelativeDatapoint(offset_minute=0, offset_value=1),
                RelativeDatapoint(offset_minute=20, offset_value=-2),
                RelativeDatapoint(offset_minute=230, offset_value=3),
            ]
        ),
        input_values=[42.0] * 6,
        expected_values=[43, 40, 40, 40, 40, 45, 45, 45],
        expected_times=[
            datetime(2022, 1, 1, 0, tzinfo=None),
            datetime(2022, 1, 1, 0, 20, tzinfo=None),
            datetime(2022, 1, 1, 1, tzinfo=None),
            datetime(2022, 1, 1, 2, tzinfo=None),
            datetime(2022, 1, 1, 3, tzinfo=None),
            datetime(2022, 1, 1, 3, 50, tzinfo=None),
            datetime(2022, 1, 1, 4, tzinfo=None),
            datetime(2022, 1, 1, 5, tzinfo=None),
        ],
        frequency="h",
    ),
]


@pytest.mark.parametrize(
    "test_case",
    [pytest.param(test_case, id=test_case.case_id) for test_case in TRANSFORMATION_TEST_CASES],
)
def test_basic_transformations(test_case: TransformationTestCase):
    """
    Test for testing different transformations (AddConstant, Round, MultiplyConstant, ToBool, ToInt,
        ZeroIfNotOne, OneIfTwo, DoNothing, MultiplyFromOffset, AddFromOffset)
    """

    incremental_dates = pd.date_range(
        start=test_case.start_date, periods=len(test_case.input_values), freq=test_case.frequency
    )

    input_data = pd.Series(test_case.input_values, index=incremental_dates)

    expected_data_index = incremental_dates if not test_case.expected_times else test_case.expected_times
    expected_data = pd.Series(test_case.expected_values, index=expected_data_index)

    output_data = test_case.transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_sum_timeseries():

    input_values_1 = [42.0] * 5
    input_values_2 = [20.0] * 5
    input_values_3 = [15] * 10

    start_time = "2022-01-01"
    incremental_dates_1 = pd.date_range(start=start_time, periods=len(input_values_1), freq="h")
    incremental_dates_2 = pd.date_range(start=start_time, periods=len(input_values_2), freq="2h")
    incremental_dates_3 = pd.date_range(start=start_time, periods=len(input_values_3), freq="30min")

    input_data_1 = pd.Series(input_values_1, index=incremental_dates_1)
    input_data_2 = pd.Series(input_values_2, index=incremental_dates_2)
    input_data_3 = pd.Series(input_values_3, index=incremental_dates_3)

    input_dates = [pd.Series(incremental_dates_1), pd.Series(incremental_dates_2), pd.Series(incremental_dates_3)]
    input_data = (input_data_1, input_data_2, input_data_3)

    expected_data_1 = input_values_1
    expected_data_2 = [62, 42, 62, 42, 62, 20, 20]
    expected_data_3 = [77, 15, 57, 15, 77, 15, 57, 15, 77, 15, 20, 20]
    all_expected_data = [expected_data_1, expected_data_2, expected_data_3]

    # checks the following scenarios:
    # SumTimseries with only input_1
    # SumTimseries with input_1 and input_2
    # SumTimseries with input_1, input_2, and input_3
    for length in range(len(input_data)):
        expected_dates = pd.concat(input_dates[: length + 1]).sort_values().drop_duplicates()
        expected_data = pd.Series(all_expected_data[length], index=expected_dates)

        transformation = SumTimeseries()
        output_data = transformation.apply(time_series_data=input_data[: length + 1])

        assert (output_data == expected_data).all()


STATIC_VALUES_TEST_CASES = [
    # StaticValues test cases
    TransformationTestCase(
        case_id="StaticValues: case_1",
        transformation=StaticValues(
            relative_datapoints=[
                RelativeDatapoint(offset_minute=0, offset_value=42),
                RelativeDatapoint(offset_minute=60, offset_value=420),
                RelativeDatapoint(offset_minute=1440, offset_value=4200),
            ]
        ),
        expected_values=[42, 420, 4200],
        expected_times=[
            datetime(2022, 1, 1, 0, tzinfo=None),
            datetime(2022, 1, 1, 1, tzinfo=None),
            datetime(2022, 1, 2, 0, tzinfo=None),
        ],
    ),
    TransformationTestCase(
        case_id="StaticValues: case_2",
        transformation=StaticValues(
            relative_datapoints=[
                RelativeDatapoint(offset_minute=0, offset_value=42),
                RelativeDatapoint(offset_minute=1, offset_value=-420),
                RelativeDatapoint(offset_minute=-120, offset_value=4200),  # TODO: do we want to allow this?
            ]
        ),
        expected_values=[42, -420, 4200],
        expected_times=[
            datetime(2022, 1, 1, 0, tzinfo=None),
            datetime(2022, 1, 1, 0, 1, tzinfo=None),
            datetime(2021, 12, 31, 22, tzinfo=None),
        ],
    ),
]


@pytest.mark.parametrize(
    "test_case",
    [pytest.param(test_case, id=test_case.case_id) for test_case in STATIC_VALUES_TEST_CASES],
)
def test_static_values(cognite_client_mock, test_case: TransformationTestCase):

    test_case.transformation.pre_apply(
        client=cognite_client_mock, shop_model={}, start=test_case.start_date, end=test_case.start_date
    )
    output_data = test_case.transformation.apply((pd.Series(),))

    expected_data = pd.Series(test_case.expected_values, index=test_case.expected_times)

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
    discharge_start_time: datetime | None = (
        None  # defaults to x hours before shop_start_time where x is the len(discharge_values)
    )
    discharge_shape: Any = field(default_factory=lambda: {"x": [0, 180, 240, 300], "y": [0, 0.2, 0.6, 0.2]})
    discharge_values: list[int] = field(default_factory=lambda: [0, 0, 0, 10, 10, 0])
    inflow_values: list[int] = field(default_factory=lambda: [5] * 11)
    expected_shape: dict[int, float] = field(default_factory=lambda: {0: 0, 180: 0.2, 240: 0.6, 300: 0.2})
    expected_values: list[float] = field(default_factory=lambda: [7, 13, 13, 7] + [5.0] * 6)
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
        expected_values=[5] * 11,  # same as the default inflow values
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
        expected_values=[
            3.5,
            3.5,
            3,
            3,
            4.5,
            4.5,
            3,
            3,
            6,
            6,
            7,
            7,
            5,
            5,
            3,
            3,
            4,
            4,
            2,
            2,
            9,
            9,
            5,
            5,
            9,
            9,
            0,
            0,
            0,
            0,
            9,
            9,
            8,
            8,
            7,
            7,
            6,
            6,
            5,
            5,
            4,
            4,
            7,
            7,
            8,
            8,
            9,
        ],
    ),
    AddWaterInTransitTestCase(
        case_id="basic",
        shape_type="shape_discharge",
        discharge_shape={"x": [0, 60, 120, 180], "y": [0, 0.5, 0.3, 0.2]},
        discharge_values=list(range(1, 7)),
        expected_shape={0: 0, 60: 0.5, 120: 0.3, 180: 0.2},
        expected_values=[10.3, 7.8, 6.2] + [5] * 7,
    ),
    AddWaterInTransitTestCase(
        case_id="time_delay",
        shape_type="time_delay",
        discharge_shape=180,
        discharge_values=list(range(1, 7)),
        expected_shape={180: 1},
        expected_values=[9, 10, 11] + [5.0] * 7,
    ),
    AddWaterInTransitTestCase(
        case_id="shape_validation_error", transit_object_type="foo", model={"foo": 0}, error=ValidationError
    ),
    AddWaterInTransitTestCase(case_id="shape_type_error", model={"gate": 0}, error=TypeError),
    AddWaterInTransitTestCase(case_id="shape_type_error_2", model={"gate": {"gate1": 0}}, error=TypeError),
    AddWaterInTransitTestCase(case_id="shape_key_error", model={"gate": {"foo": 0}}, error=KeyError),
    AddWaterInTransitTestCase(case_id="shape_key_error_2", model={"foo": {"gate1": {"foo": 0}}}, error=KeyError),
    AddWaterInTransitTestCase(case_id="shape_value_error", model={"gate": {"gate1": {"foo": 0}}}, error=ValueError),
]


@pytest.mark.parametrize(
    "test_case",
    [pytest.param(test_case, id=test_case.case_id) for test_case in ADD_WATER_TEST_CASES],
)
def test_add_water_in_transit(cognite_client_mock, test_case):

    with pytest.raises(test_case.error) if test_case.error else nullcontext():
        transformation = AddWaterInTransit(
            discharge_ts_external_id=test_case.discharge_ts_external_id,
            transit_object_type=test_case.transit_object_type,
            transit_object_name=test_case.transit_object_name,
        )

        if test_case.discharge_start_time:
            discharge_start_time = test_case.discharge_start_time
        else:
            discharge_start_time = test_case.shop_start_time - timedelta(hours=len(test_case.discharge_values))
        discharge_times = pd.date_range(
            start=discharge_start_time, periods=len(test_case.discharge_values), freq=test_case.time_frequency
        )
        discharge_times_ms = [time.replace(tzinfo=timezone.utc).timestamp() * 1000 for time in discharge_times]

        inflow_times = pd.date_range(
            start=test_case.shop_start_time, periods=len(test_case.inflow_values), freq=test_case.time_frequency
        )
        inflow_input_data = (pd.Series(test_case.inflow_values, index=inflow_times),)

        start_time_ms = inflow_times[0].replace(tzinfo=timezone.utc).timestamp() * 1000
        if not test_case.end_time:
            test_case.end_time = inflow_times[-1]
        end_time_ms = test_case.end_time.replace(tzinfo=timezone.utc).timestamp() * 1000

        if test_case.model:
            model = test_case.model
        else:
            model = {
                test_case.transit_object_type: {
                    test_case.transit_object_name: {test_case.shape_type: test_case.discharge_shape}
                }
            }

        cognite_client_mock.time_series.data.retrieve.return_value = Datapoints(
            external_id=test_case.discharge_ts_external_id,
            value=test_case.discharge_values,
            timestamp=discharge_times_ms,
        )
        cognite_client_mock.time_series.data.retrieve_latest.return_value = Datapoints(
            external_id=test_case.discharge_ts_external_id,
            value=[test_case.discharge_values[-1]],
            timestamp=[discharge_times_ms[-1]],
        )
        cognite_client_mock.time_series.retrieve_multiple.return_value = [
            TimeSeries(external_id=test_case.discharge_ts_external_id, is_step=True)
        ]

        transformation.pre_apply(client=cognite_client_mock, shop_model=model, start=start_time_ms, end=end_time_ms)
        output_shape = transformation.shape
        output_data = transformation.apply(inflow_input_data)

        expected_timestamps = pd.date_range(
            start=test_case.shop_start_time, end=test_case.end_time - timedelta(hours=1), freq="1h"
        )
        expected_data = (
            pd.Series(test_case.expected_values, index=expected_timestamps[: len(test_case.expected_values)])
            .reindex(expected_timestamps)
            .ffill()
        )

        pd.testing.assert_series_equal(expected_data, output_data, check_dtype=False)
        assert output_shape == test_case.expected_shape
