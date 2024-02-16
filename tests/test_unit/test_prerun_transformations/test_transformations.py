from datetime import datetime, timedelta
import pytz

import pandas as pd

from cognite.powerops.prerun_transformations.transformations import (
    ms_to_datetime_tz_naive,
    StaticValues,
    RelativeDatapoint,
    AddConstant,
    Round,
    SumTimeseries,
    MultiplyConstant,
    ToBool,
    ToInt,
    ZeroIfNotOne,
    OneIfTwo,
    HeightToVolume,
    DoNothing,
    AddFromOffset,
)



def test_ms_to_datetime_tz_naive():

    epoch_timezone = pytz.timezone('Etc/UTC')
    input_epoch_time = datetime(2022, 1, 1, 12, tzinfo=epoch_timezone)

    oslo_timezone = pytz.timezone('Europe/Oslo')
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
    incremental_dates = pd.date_range(start='2022-01-01', periods=len(input_values), freq='D')

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_round():

    digits = 2
    transformation = Round(digits=digits)

    input_values = [10.1111, 20.2222, 30.3333, 40.4444, 50.5555]
    expected_values = [round(value, digits) for value in input_values]
    incremental_dates = pd.date_range(start='2022-01-01', periods=len(input_values), freq='D')

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_sum_timeseries_two_timeseries():

    input_values_1 = [42.0] * 5
    input_values_2 = [20.0] * 5

    start_time = "2022-01-01"
    incremental_dates_1 = pd.date_range(start=start_time, periods=len(input_values_1), freq='H')
    incremental_dates_2 = pd.date_range(start=start_time, periods=len(input_values_2), freq='2H')

    input_data_1 = pd.Series(input_values_1, index=incremental_dates_1)
    input_data_2 = pd.Series(input_values_2, index=incremental_dates_2)

    expected_dates = pd.concat([pd.Series(incremental_dates_1), pd.Series(incremental_dates_2)]).sort_values().drop_duplicates()
    expected_data = pd.Series([62, 42, 62, 42, 62, 20, 20], index=expected_dates)

    transformation = SumTimeseries()
    output_data = transformation.apply(time_series_data=(input_data_1, input_data_2))

    assert (output_data == expected_data).all()


def test_sum_timeseries_one_timeseries():

    input_values_1 = [42.0] * 5

    start_time = "2022-01-01"
    incremental_dates_1 = pd.date_range(start=start_time, periods=len(input_values_1), freq='H')

    input_data_1 = pd.Series(input_values_1, index=incremental_dates_1)

    expected_data = input_data_1

    transformation = SumTimeseries()
    output_data = transformation.apply(time_series_data=(input_data_1, ))

    assert (output_data == expected_data).all()


def test_multiply_constant():

    constant = 10
    transformation = MultiplyConstant(constant=constant)

    input_values = [10, 20, 30, 40, 50]
    expected_values = [value * constant for value in input_values]

    incremental_dates = pd.date_range(start='2022-01-01', periods=len(input_values), freq='D')

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
    
    expected = pd.Series([42, 420, 4200], index=[
        datetime(2022, 1, 1, 12, tzinfo=None),
        datetime(2022, 1, 1, 13, tzinfo=None),
        datetime(2022, 1, 2, 12, tzinfo=None)
    ])

    assert (result == expected).all()


def test_to_bool():

    transformation = ToBool()

    input_values = [-1, -0.5, 0, 0.5, 1, 2]
    expected_values = [0, 0, 0, 1, 1, 1]

    incremental_dates = pd.date_range(start='2022-01-01', periods=len(input_values), freq='D')

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_to_int():

    transformation = ToInt()

    input_values = [-1.2, -0.5, 0.23, 0.5, 1, 2.75]
    expected_values = [-1, 0, 0, 0, 1, 3]

    incremental_dates = pd.date_range(start='2022-01-01', periods=len(input_values), freq='D')

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_zero_if_not_one():

    transformation = ZeroIfNotOne()

    input_values = [0, 1, 2, -1]
    expected_values = [0, 1, 0, 0]

    incremental_dates = pd.date_range(start='2022-01-01', periods=len(input_values), freq='D')

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_one_if_two():
        
    transformation = OneIfTwo()

    input_values = [0, 1, 2, -1]
    expected_values = [0, 0, 1, 0]

    incremental_dates = pd.date_range(start='2022-01-01', periods=len(input_values), freq='D')

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

    incremental_dates = pd.date_range(start='2022-01-01', periods=len(input_values), freq='D')

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = input_data

    output_data = transformation.apply(time_series_data=(input_data,))

    assert (output_data == expected_data).all()


def test_add_from_offset():

    relative_datapoints = [
        RelativeDatapoint(offset_minute=0, offset_value=1),
        RelativeDatapoint(offset_minute=20, offset_value=-2),
        RelativeDatapoint(offset_minute=230, offset_value=3),
    ]
        
    transformation = AddFromOffset(relative_datapoints=relative_datapoints)

    input_values = [42.0] * 6

    incremental_dates = pd.date_range(start='2022-01-01', periods=len(input_values), freq='H')

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series([43, 40, 40, 40, 40, 45, 45, 45], index=[
        datetime(2022, 1, 1, 0, tzinfo=None),
        datetime(2022, 1, 1, 0, 20, tzinfo=None),
        datetime(2022, 1, 1, 1, tzinfo=None),
        datetime(2022, 1, 1, 2, tzinfo=None),
        datetime(2022, 1, 1, 3, tzinfo=None),
        datetime(2022, 1, 1, 3, 50, tzinfo=None),
        datetime(2022, 1, 1, 4, tzinfo=None),
        datetime(2022, 1, 1, 5, tzinfo=None),
    ])

    output_data = transformation.apply(time_series_data=(input_data,))

    print("----------------------")
    print(output_data)
    print(expected_data)

    assert (output_data == expected_data).all()