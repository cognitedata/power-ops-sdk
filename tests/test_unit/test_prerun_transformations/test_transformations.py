from datetime import datetime, timedelta
import pytz

import pandas as pd

from cognite.powerops.prerun_transformations.transformations import (
    ms_to_datetime_tz_naive,
    StaticValues,
    RelativeDatapoint,
    AddConstant
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
    expected_values = [x + constant for x in input_values]
    incremental_dates = pd.date_range(start='2022-01-01', periods=len(input_values), freq='D')

    input_data = pd.Series(input_values, index=incremental_dates)

    expected_data = pd.Series(expected_values, index=incremental_dates)

    output_data = transformation.apply(time_series_data=input_data)

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
