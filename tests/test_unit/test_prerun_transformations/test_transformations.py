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
