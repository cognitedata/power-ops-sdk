import tempfile
from datetime import datetime
from itertools import chain

import arrow
import numpy as np
import pandas as pd
import pytest
import yaml
from cognite.client.data_classes import Asset, Event, FileMetadata, TimeSeries
from cognite.client.exceptions import CogniteException

from cognite.powerops.preprocessor.exceptions import CogShopConfigError, CogShopError
from cognite.powerops.preprocessor.utils import (
    ShopMetadata,
    _overlapping_keys,
    _retrieve_range,
    arrow_to_ms,
    create_relationship,
    datetime_from_str,
    download_file,
    group_files_by_metadata,
    log_and_reraise,
    log_missing,
    merge_dicts,
    ms_to_datetime,
    now,
    remove_duplicates,
    rename_dict_keys,
    retrieve_sequence_rows_as_dicts,
    retrieve_yaml_file,
    save_dict_as_yaml,
    shift_datetime_str,
    simple_relationship,
)


def test_timestamp_to_datetime():
    assert ms_to_datetime(0) == datetime(1970, 1, 1)
    assert ms_to_datetime(1640995200000) == datetime(2022, 1, 1)


def test_arrow_to_ms():
    assert arrow_to_ms(arrow.get("1970-1-1")) == 0
    assert arrow_to_ms(arrow.get("2022-1-1")) == 1640995200000


def test_datetime_from_str():
    assert datetime_from_str("2022-05-08 22:00:00") == datetime(2022, 5, 8, 22)


def test_shift_datetime_str():
    assert shift_datetime_str("2022-05-08 22:00:00", days=0) == "2022-05-08 22:00:00"
    assert shift_datetime_str("2022-05-08 22:00:00", days=6) == "2022-05-14 22:00:00"


def test_rename_dict_keys():
    MAPPING = {"old_key": "new_key"}
    d = {"old_key": 42}

    rename_dict_keys(d, key_mapping=MAPPING)

    assert list(d.keys()) == ["new_key"]
    assert d["new_key"] == 42


def test_log_and_reraise():
    @log_and_reraise(CogShopError)
    def some_func():
        return 1 / 0

    with pytest.raises(CogShopError):
        some_func()

    # Raise subclass
    @log_and_reraise(CogShopConfigError)
    def another_func():  # sourcery skip: raise-specific-error
        raise Exception()

    with pytest.raises(CogShopError):
        another_func()


@pytest.mark.parametrize(
    "first,second,expected",
    [
        ({}, {}, []),
        ({"a": 1}, {}, []),
        ({"a": 1}, {"b": 2}, []),
        ({"a": 1}, {"a": 2}, ["a"]),
        ({"a": 1, "b": 3}, {"a": 2}, ["a"]),
        ({"a": 1, "b": 3}, {"a": 2, "c": 4}, ["a"]),
        ({"a": 1, "b": 3}, {"a": 2, "b": 4}, ["a", "b"]),
    ],
)
def test_overlapping_keys(first, second, expected):
    res = _overlapping_keys(first, second)
    assert set(res) == set(expected)


def test_merge_dicts():
    first = {"a": 1, "b": 2}
    second = {"c": 3, "d": 4}
    third = {"e": 5, "f": 6}

    res = merge_dicts(first, second, third)
    assert sorted(list(res.keys())) == ["a", "b", "c", "d", "e", "f"]


def test_merge_dicts_key_collision():
    first = {"a": 1, "b": 2}
    second = {"b": 3, "c": 4}

    with pytest.raises(Exception):
        merge_dicts(first, second)


def test_retrieve_sequence_rows_as_dicts(cognite_client_mock):
    dummy_df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    cognite_client_mock.sequences.data.retrieve_dataframe.return_value = dummy_df
    expected = [{"a": 1, "b": 3}, {"a": 2, "b": 4}]
    res = retrieve_sequence_rows_as_dicts(client=cognite_client_mock, external_id="dummy")
    assert res == expected


def test_remove_duplicates():
    assert remove_duplicates([1, 2, 3]) == [1, 2, 3]
    assert remove_duplicates([1, 1, 3]) == [1, 3]
    assert remove_duplicates([1, 1, 1]) == [1]
    assert remove_duplicates([]) == []


def test_now():
    assert isinstance(now(), int)


def test_retrieve_yaml_file(cognite_client_mock):
    cognite_client_mock.files.download_bytes.return_value = "{'hello': 123}"
    res = retrieve_yaml_file(cognite_client_mock, file_external_id="dummy")
    assert isinstance(res, dict)
    assert res == {"hello": 123}


def test_save_dict_as_yaml():
    temp_dir = tempfile.mkdtemp()
    path = f"{temp_dir}/tmp.yaml"
    d = {"hello": 123}
    save_dict_as_yaml(file_path=path, d=d)

    with open(path, "r") as f:
        d_loaded = yaml.safe_load(f)

    assert isinstance(d_loaded, dict)
    assert d_loaded == d


@pytest.fixture
def dummy_event() -> Event:
    return Event(external_id="dummy_event")


@pytest.fixture
def dummy_asset() -> Asset:
    return Asset(external_id="dummy_asset")


def test_simple_relationship(dummy_event, dummy_asset):
    res = simple_relationship(source=dummy_event, target=dummy_asset, label_external_id="dummy_label", data_set_id=42)
    assert res.source_type.lower() == "event"
    assert res.target_type.lower() == "asset"
    assert res.source_external_id == dummy_event.external_id
    assert res.target_external_id == dummy_asset.external_id
    assert isinstance(res.labels, list)


def test_create_relationship(cognite_client_mock, dummy_event, dummy_asset):
    create_relationship(
        client=cognite_client_mock,
        source=dummy_event,
        target=dummy_asset,
        label_external_id="dummy_label",
        data_set_id=42,
    )
    assert cognite_client_mock.relationships.create.call_count == 1


def test_create_relationship_label_does_not_exist(cognite_client_mock, dummy_event, dummy_asset):
    cognite_client_mock.relationships.create.side_effect = [CogniteException("assume_label_does_not_exist"), None]
    create_relationship(
        client=cognite_client_mock,
        source=dummy_event,
        target=dummy_asset,
        label_external_id="dummy_label",
        data_set_id=42,
    )
    assert cognite_client_mock.relationships.create.call_count == 2  # called before and after label creation
    assert cognite_client_mock.labels.create.call_count == 1


def test_shop_metadata():
    assert isinstance(ShopMetadata(), dict)
    # keys should be prefixed with "shop:"
    assert ShopMetadata(type="dummy", watercourse="dummy") == {"shop:type": "dummy", "shop:watercourse": "dummy"}


def test_download_file(cognite_client_mock):
    file = FileMetadata(external_id="dummy", name="dummy")
    temp_dir = "temp"
    cognite_client_mock.files.download.retrun_value = None
    file_path = download_file(client=cognite_client_mock, file=file, download_directory=temp_dir)
    assert file.name in file_path
    assert temp_dir in file_path


@pytest.mark.skip(reason="logging not working for some reason")
def test_log_missing(caplog):
    log_missing([], [])
    assert caplog.text == ""
    log_missing([1], [1])
    assert caplog.text == ""
    log_missing([1], [1, 2])
    assert caplog.text == ""
    # No logging should have occured up until this point
    log_missing([1, 2], [1])  # 2 missing

    assert caplog.messages.pop() == "Missing: 2"
    assert "2" in caplog.text


def t(hour: int, minutes: int = 0) -> pd.Timestamp:
    """Simplify creation of datetimes"""
    return pd.Timestamp(2000, 1, 1, hour, minutes)


class Dummy:
    """Dummy class to mimic "datapoints.to_pandas()"""

    def __init__(self, df) -> None:
        self.to_pandas = lambda: df


def test_retrieve_range_2(cognite_client_mock):
    # NOTE: quite complicated test

    # --- BEGIN SETUP
    time_series = [
        TimeSeries(external_id="a", is_step=True),
        TimeSeries(external_id="b", is_step=False),
        TimeSeries(external_id="c", is_step=True),
    ]
    cognite_client_mock.time_series.retrieve_multiple.return_value = time_series

    #      00:05       00:40
    t_l = [t(0, 5), t(0, 40)]
    a_l = [2, np.NaN]
    b_l = [np.NaN, 40]
    c_l = [np.NaN, 42]
    latest = Dummy(pd.DataFrame({"a": a_l, "b": b_l, "c": c_l}, index=t_l))
    cognite_client_mock.time_series.data.retrieve_latest.return_value = latest

    #      01:00   02:00   *02:50    *03:10     04:00   05:00
    t_r = [t(1), t(2), t(2, 50), t(3, 10), t(4), t(5)]
    a_r = [np.NaN, 5, np.NaN, 6, np.NaN, 7]
    b_r = [np.NaN, 30, 40, 30, np.NaN, 40]
    c_r = [np.NaN] * 6
    range_ = Dummy(pd.DataFrame({"a": a_r, "b": b_r, "c": c_r}, index=t_r))
    cognite_client_mock.time_series.data.retrieve.return_value = range_
    # -- END SETUP

    df = _retrieve_range(
        client=cognite_client_mock,
        external_ids=["dummy"],
        start=int(t(1).timestamp() * 1000),
        end=int(t(5).timestamp() * 1000),
    )

    # Column names are correct
    assert list(df.columns) == ["a", "b", "c"]
    # Granularity should be "1h" -> 01:00 02:00 03:00 04:00 05:00
    assert list(df.index) == [t(1), t(2), t(3), t(4), t(5)]
    # Existing datapoints on whole hours should not have changed
    assert df["a"][t(2)] == a_r[1]  # "a" @ 02:00
    assert df["a"][t(5)] == a_r[5]  # "a" @ 05:00
    assert df["b"][t(2)] == b_r[1]  # "b" @ 02:00
    assert df["b"][t(5)] == b_r[5]  # "b" @ 05:00
    # "a" should be step interpolated (is_step=True)
    assert df["a"][t(1)] == a_l[0]
    assert df["a"][t(3)] == a_r[1]
    assert df["a"][t(4)] == a_r[3]
    # "b" should be interpolated (is_step=False)
    assert b_l[1] >= df["b"][t(1)] >= b_r[1]
    assert b_r[2] >= df["b"][t(3)] >= b_r[3]
    assert b_r[3] <= df["b"][t(4)] <= b_r[5]
    # "c" did not have any datapoints in range, should be ffilled from latest
    assert df["c"][t(1)] == 42
    assert df["c"][t(2)] == 42
    assert df["c"][t(3)] == 42
    assert df["c"][t(4)] == 42
    assert df["c"][t(5)] == 42


def test_retrieve_range_2_no_datapoints_in_range(cognite_client_mock):
    # NOTE: quite complicated test

    # --- BEGIN SETUP
    time_series = [TimeSeries(external_id="a", is_step=True)]
    cognite_client_mock.time_series.retrieve_multiple.return_value = time_series

    #      00:05       00:40
    t_l = [t(0, 5), t(0, 40)]
    a_l = [42, np.NaN]
    latest = Dummy(pd.DataFrame({"a": a_l}, index=t_l))
    cognite_client_mock.time_series.data.retrieve_latest.return_value = latest

    range_ = Dummy(pd.DataFrame())  # Empty DataFrame since no data in range
    cognite_client_mock.time_series.data.retrieve.return_value = range_
    # -- END SETUP

    df = _retrieve_range(
        client=cognite_client_mock,
        external_ids=["dummy"],
        start=int(t(1).timestamp() * 1000),
        end=int(t(5).timestamp() * 1000),
    )

    # Column names are correct
    assert list(df.columns) == ["a"]
    # Granularity should be "1h" -> 01:00 02:00 03:00 04:00 05:00
    assert list(df.index) == [t(1)]
    # Should be ffilled from latest
    assert df["a"][t(1)] == 42


class TestGroupFilesByMetadata:
    @staticmethod
    def mock_file_metadata(index, file_group) -> FileMetadata:
        md = {"shop:file_group": file_group} if file_group is not None else {}
        return FileMetadata(id=index, data_set_id=42, external_id=str(index), metadata=md)

    def test_no_group(self):
        """If no group metadata available return list as is"""
        files = [self.mock_file_metadata(i, None) for i in range(5)]
        actual = group_files_by_metadata(files)
        assert actual == {"default": files}

    def test_group(self):
        """If group metadata available group files by group metadata"""
        expected = {
            "1": [
                self.mock_file_metadata(1, "1"),
                self.mock_file_metadata(2, "1"),
            ],
            "2": [
                self.mock_file_metadata(4, "2"),
            ],
            "3": [
                self.mock_file_metadata(0, "3"),
                self.mock_file_metadata(3, "3"),
            ],
        }
        files = list(chain(*expected.values()))

        actual = group_files_by_metadata(files)

        assert actual == expected

    def test_no_group_and_group(self):
        """If grouped files available ignore non-grouped files"""
        expected = {
            "3": [
                self.mock_file_metadata(0, "3"),
            ],
            "1": [
                self.mock_file_metadata(1, "1"),
                self.mock_file_metadata(2, "1"),
            ],
        }
        files = list(chain(*expected.values()))
        files.append(self.mock_file_metadata(3, None))
        files.insert(0, self.mock_file_metadata(4, None))

        actual = group_files_by_metadata(files)

        assert actual == expected
