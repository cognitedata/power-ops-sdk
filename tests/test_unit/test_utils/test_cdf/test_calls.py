from unittest.mock import MagicMock

import pytest

from cognite.powerops.utils.cdf.datasets_calls import get_dataset_id


class DummyDataRecord:
    def __init__(self, last_updated_time):
        self.last_updated_time = last_updated_time


class DummyConfig:
    def __init__(self, read, write, monitor, last_updated_time):
        self.read_data_set = read
        self.write_data_set = write
        self.monitor_data_set = monitor
        self.data_record = DummyDataRecord(last_updated_time)


class DummyDataset:
    def __init__(self, id):
        self.id = id


@pytest.fixture
def mock_client():
    client = MagicMock()
    # Mock the nested call: client.v1.day_ahead_configuration.data_set_configuration.list(limit=-1)
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = [
        DummyConfig("read_ext", "write_ext", "monitor_ext", 100)
    ]
    client.cdf.data_sets.retrieve.return_value = DummyDataset(42)
    return client


def test_get_dataset_id_read(mock_client):
    result = get_dataset_id(mock_client, "READ")
    assert result == 42
    mock_client.v1.day_ahead_configuration.data_set_configuration.list.assert_called_with(limit=-1)
    mock_client.cdf.data_sets.retrieve.assert_called_with(external_id="read_ext")


def test_get_dataset_id_write(mock_client):
    result = get_dataset_id(mock_client, "WRITE")
    assert result == 42
    mock_client.cdf.data_sets.retrieve.assert_called_with(external_id="write_ext")


def test_get_dataset_id_monitor(mock_client):
    result = get_dataset_id(mock_client, "MONITOR")
    assert result == 42
    mock_client.cdf.data_sets.retrieve.assert_called_with(external_id="monitor_ext")


def test_get_dataset_id_no_configs():
    client = MagicMock()
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = []
    with pytest.raises(ValueError, match="No dataset configuration found."):
        get_dataset_id(client, "READ")


def test_get_dataset_id_no_external_id():
    client = MagicMock()
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = [DummyConfig("", "", "", 100)]
    with pytest.raises(ValueError, match="No external ID found for the dataset."):
        get_dataset_id(client, "READ")
