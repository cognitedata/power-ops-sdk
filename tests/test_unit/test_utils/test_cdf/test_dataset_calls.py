from unittest.mock import MagicMock

import pytest

from cognite.powerops.utils.cdf.datasets_calls import (
    get_dataset_external_id,
    get_dataset_id,
)


class DummyDataRecord:
    def __init__(self, last_updated_time):
        self.last_updated_time = last_updated_time


class DummyConfig:
    def __init__(self, read, write, monitor, process, last_updated_time):
        self.read_data_set = read
        self.write_data_set = write
        self.monitor_data_set = monitor
        self.process_data_set = process
        self.data_record = DummyDataRecord(last_updated_time)


class DummyDataset:
    def __init__(self, id, external_id):
        self.id = id
        self.external_id = external_id


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = [
        DummyConfig("read_ext", "write_ext", "monitor_ext", "process_ext", 100)
    ]
    client.cdf.data_sets.retrieve.side_effect = lambda external_id: (
        DummyDataset(42, external_id) if external_id else None
    )
    return client


@pytest.fixture
def mock_client_multiple_configs():
    client = MagicMock()
    # Two configs, the second one is newer (last_updated_time=200)
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = [
        DummyConfig("old_read", "old_write", "old_monitor", "old_process", 100),
        DummyConfig("new_read", "new_write", "new_monitor", "new_process", 200),
    ]
    client.cdf.data_sets.retrieve.side_effect = lambda external_id: (
        DummyDataset(99, external_id) if external_id else None
    )
    return client


def test_get_dataset_id_read(mock_client):
    assert get_dataset_id(mock_client, "READ") == 42


def test_get_dataset_id_write(mock_client):
    assert get_dataset_id(mock_client, "WRITE") == 42


def test_get_dataset_id_monitor(mock_client):
    assert get_dataset_id(mock_client, "MONITOR") == 42


def test_get_dataset_id_process(mock_client):
    assert get_dataset_id(mock_client, "PROCESS") == 42


def test_get_dataset_external_id_read(mock_client):
    assert get_dataset_external_id(mock_client, "READ") == "read_ext"


def test_get_dataset_external_id_write(mock_client):
    assert get_dataset_external_id(mock_client, "WRITE") == "write_ext"


def test_get_dataset_external_id_monitor(mock_client):
    assert get_dataset_external_id(mock_client, "MONITOR") == "monitor_ext"


def test_get_dataset_external_id_process(mock_client):
    assert get_dataset_external_id(mock_client, "PROCESS") == "process_ext"


def test_no_dataset_config():
    client = MagicMock()
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = []
    with pytest.raises(ValueError, match="No dataset configuration found."):
        get_dataset_id(client, "READ")


def test_no_external_id():
    client = MagicMock()
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = [
        DummyConfig(None, None, None, None, 100)
    ]
    with pytest.raises(ValueError, match="No external_id found for data_set_type: READ"):
        get_dataset_id(client, "READ")


def test_dataset_not_found():
    client = MagicMock()
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = [
        DummyConfig("read_ext", "write_ext", "monitor_ext", "process_ext", 100)
    ]
    client.cdf.data_sets.retrieve.return_value = None
    with pytest.raises(ValueError, match="Dataset with external_id 'read_ext' not found."):
        get_dataset_id(client, "READ")


def test_invalid_type(mock_client):
    with pytest.raises(ValueError, match="Unknown data_set_type: INVALID"):
        get_dataset_id(mock_client, "INVALID")


def test_multiple_configs_latest_used(mock_client_multiple_configs):
    # Should use the config with last_updated_time=200
    assert get_dataset_external_id(mock_client_multiple_configs, "READ") == "new_read"
    assert get_dataset_external_id(mock_client_multiple_configs, "WRITE") == "new_write"
    assert get_dataset_external_id(mock_client_multiple_configs, "MONITOR") == "new_monitor"
    assert get_dataset_external_id(mock_client_multiple_configs, "PROCESS") == "new_process"
