from datetime import datetime, timedelta

import pandas as pd

from cognite.powerops.preprocessor.transformation_functions import (
    AddFromOffset,
    AddWaterInTransit,
    DoNothing,
    HeightToVolume,
    MultiplyFromOffset,
    OneIfTwo,
    StaticValues,
    ToBool,
    ZeroIfNotOne,
)


def test_static_values():
    start = datetime(2022, 1, 1)
    relative_datapoints = {"0": 42, "1440": 69}
    expected = pd.Series([42, 69], index=[datetime(2022, 1, 1), datetime(2022, 1, 2)])
    res = StaticValues(kwargs=relative_datapoints).apply(start=start)
    assert (res == expected).all()


def test_height_to_volume():

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

    expected = pd.Series(
        {
            1: 10,  # set tp min volume
            2: 20,
            3: 40,
            4: 60,  # interpolated between 40 and 80
            5: 160,  # set to max volume
        }
    )
    # NOTE: not testing `apply`
    res = HeightToVolume().height_to_volume(datapoints=datapoints, heights=heights, volumes=volumes)
    assert (res == expected).all()


def test_add_from_offset():
    timestamps = [datetime(2022, 1, 1) + timedelta(minutes=i) for i in range(6)]
    values = [10] * 6
    datapoints = pd.Series(values, index=timestamps)

    # Add 10 on offset 0 and 1
    # Subtract 10 on offset 2 and 3
    # Do nothing with offsets above 4
    # minute_offset: value_to_add
    relative_datapoints = {"0": 10, "4": 0, "2": -10}

    expected_values = [20, 20, 0, 0, 10, 10]
    expected_datapoints = pd.Series(expected_values, index=timestamps)

    res = AddFromOffset(kwargs=relative_datapoints).apply(datapoints=datapoints)
    assert (res == expected_datapoints).all()


def test_add_from_offset_timestamp_mismatch():
    # Only even minutes
    timestamps = [datetime(2022, 1, 1) + timedelta(minutes=2 * i) for i in range(6)]
    values = [0, 2, 4, 6, 8, 10]
    datapoints = pd.Series(values, index=timestamps)

    # Expect datapoints to be added at 1, 3 and 5
    relative_datapoints = {"1": 10, "3": 99, "5": -10}

    # Expect len(datapoints) 6 + len(releative_datapoints) 3 -> 9 values
    expected_timestamps = sorted(timestamps + [datetime(2022, 1, 1, 0, int(minute)) for minute in relative_datapoints])
    expected_values = [  # sourcery skip: bin-op-identity
        0,
        0 + 10,
        2 + 10,
        2 + 99,
        4 + 99,
        4 - 10,
        6 - 10,
        8 - 10,
        10 - 10,
    ]
    expected = pd.Series(expected_values, index=expected_timestamps)

    res = AddFromOffset(kwargs=relative_datapoints).apply(datapoints=datapoints)
    assert (res == expected).all()


def test_dynamic_add_from_offset():
    timestamps = [
        datetime(2022, 1, 1, 0),
        datetime(2022, 1, 1, 1),
        datetime(2022, 1, 1, 2),
        datetime(2022, 1, 1, 3),
        datetime(2022, 1, 1, 4),
        datetime(2022, 1, 1, 5),
    ]
    values = [42] * len(timestamps)
    datapoints = pd.Series(values, index=timestamps)
    relative_datapoints = {"0": 1, "20": -2, "230": 3}
    res = AddFromOffset(kwargs=relative_datapoints).apply(datapoints=datapoints, shift_minutes=10)
    # With `shift_minutes=10` we will add from 10mins, 30mins and 240mins
    expected = pd.Series(
        data=[42, 42 + 1, 42 - 2, 42 - 2, 42 - 2, 42 - 2, 42 + 3, 42 + 3],
        index=[
            datetime(2022, 1, 1, 0),
            datetime(2022, 1, 1, 0, 10),  # 10 mins
            datetime(2022, 1, 1, 0, 30),  # 30 mins
            datetime(2022, 1, 1, 1),
            datetime(2022, 1, 1, 2),
            datetime(2022, 1, 1, 3),
            datetime(2022, 1, 1, 4),  # 240 mins
            datetime(2022, 1, 1, 5),
        ],
    )
    assert (res == expected).all()


def test_multiply_from_offset():
    timestamps = [datetime(2022, 1, 1) + timedelta(minutes=i) for i in range(6)]
    values = [10] * 6
    datapoints = pd.Series(values, index=timestamps)

    # Do nothing on offset 0
    # Multiply 2 on offset  1
    # Multiply 0 on offset 2 and 3
    # Multiply 1.5 with offsets above 4
    # minute_offset: value_to_add
    relative_datapoints = {"2": 0, "4": 1.5, "1": 2}

    expected_datapoints = pd.Series([10, 20, 0, 0, 15, 15], index=timestamps)

    res = MultiplyFromOffset(kwargs=relative_datapoints).apply(datapoints=datapoints)
    assert (res == expected_datapoints).all()


def test_multiply_from_offset_timestamp_mismatch():
    # Only even minutes
    timestamps = [datetime(2022, 1, 1) + timedelta(minutes=2 * i) for i in range(6)]
    values = [0, 2, 4, 6, 8, 10]
    datapoints = pd.Series(values, index=timestamps)
    # Expect datapoints to be added at 1, 3 and 5
    relative_datapoints = {"1": 10, "3": 99, "5": -10}

    # Expect len(datapoints) 6 + len(relative_datapoints) 3 -> 9 values
    expected_timestamps = sorted(timestamps + [datetime(2022, 1, 1, 0, int(minute)) for minute in relative_datapoints])
    expected_values = [0, 0 * 10, 2 * 10, 2 * 99, 4 * 99, 4 * -10, 6 * -10, 8 * -10, 10 * -10]
    expected = pd.Series(expected_values, index=expected_timestamps)

    res = MultiplyFromOffset(kwargs=relative_datapoints).apply(datapoints=datapoints)
    assert (res == expected).all()


def test_get_shape():
    model = {"gate": {"gate1": {"shape_discharge": {"ref": 0, "x": [0, 60, 120], "y": [0.1, 0.5, 0.4]}}}}

    shape = AddWaterInTransit(kwargs={"dummy": "dummy"}).get_shape(model=model, object_type="gate", object_name="gate1")

    expected_result = {0: 0.1, 60: 0.5, 120: 0.4}
    assert shape == expected_result


def test_add_water_in_transit():
    start = datetime(year=2022, month=5, day=20, hour=22)
    end = start + timedelta(days=5)

    # Inflow with **2h** granularity
    inflow = [1, 2, 3, 2, 4, 5, 3, 1, 2, 0, 7, 5, 9, 0, 0, 9, 8, 7, 6, 5, 4, 7, 8, 9]
    timestamps = [start + timedelta(hours=2 * i) for i in range(len(inflow))]
    datapoints = pd.Series(inflow, index=timestamps)

    # Discharge from 22h before inflow timeseries / start
    discharge_start = datetime(year=2022, month=5, day=20, hour=0)
    discharge_timestamps = [discharge_start + timedelta(hours=2 * i) for i in range(5)]
    discharge = pd.Series(inflow[:5], index=discharge_timestamps)  # Note: reusing inflow values

    # Shape of discharge to be added
    shape = {
        0: 0,  # 0% instantly
        120: 0.5,  # 50% delayed for 2h
        1320: 0.5,  # 50% delayed for 22h (i.e. from start of inflow time series)
    }

    # Expected values with **1h** granularity
    expected = [
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
    ]
    # NOTE: do not include the `end` timestamp
    expected_timestamps = pd.date_range(start=start, end=end - timedelta(hours=1), freq="1h")
    # Note: have to reindex and ffill to get all values and all timestamps in the series
    expected_series = (
        pd.Series(expected, index=expected_timestamps[: len(expected)]).reindex(expected_timestamps).ffill()
    )

    # NOTE: not testing `apply`
    res = AddWaterInTransit(kwargs={"dummy": "dummy"}).add_water_in_transit(
        inflow=datapoints, discharge=discharge, shape=shape, start=start, end=end
    )
    assert (res == expected_series).all()


def test_do_nothing():
    datapoints = pd.Series(range(10))
    res = DoNothing().apply(datapoints=datapoints)
    # Check that datapoints has not been mutated
    assert (datapoints == pd.Series(range(10))).all()
    assert (res == datapoints).all()


def test_one_if_two():
    datapoints = pd.Series([1, 1, 2, 2, 1, 1])
    res = OneIfTwo().apply(datapoints=datapoints)
    expected = pd.Series([0, 0, 1, 1, 0, 0])
    assert (res == expected).all()


def test_zero_if_not_one():
    datapoints = pd.Series([1, 1, 2, 2, 1, 1])
    res = ZeroIfNotOne().apply(datapoints=datapoints)
    expected = pd.Series([1, 1, 0, 0, 1, 1])
    assert (res == expected).all()


def test_to_bool():
    datapoints = pd.Series([0.1, 1, 0, -1, 100, 42])
    res = ToBool().apply(datapoints=datapoints)
    expected = pd.Series([1, 1, 0, 0, 1, 1])
    assert (res == expected).all()
