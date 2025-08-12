from unittest.mock import MagicMock

import pytest

from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.utils.cdf.datasets_calls import get_latest_dataset


@pytest.fixture
def get_monitor_dataset_by_external_id(client: PowerOpsClient):
    return client.cdf.data_sets.retrieve(external_id="monitor_dataset")


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
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = [
        DummyConfig("old_read", "old_write", "old_monitor", "old_process", 100),
        DummyConfig("new_read", "new_write", "new_monitor", "new_process", 200),
    ]
    client.cdf.data_sets.retrieve.side_effect = lambda external_id: (
        DummyDataset(99, external_id) if external_id else None
    )
    return client


def test_get_latest_dataset_read(mock_client):
    ds = get_latest_dataset(mock_client, "READ")
    assert ds.id == 42
    assert ds.external_id == "read_ext"


def test_get_latest_dataset_write(mock_client):
    ds = get_latest_dataset(mock_client, "WRITE")
    assert ds.id == 42
    assert ds.external_id == "write_ext"


def test_get_latest_dataset_monitor(mock_client):
    ds = get_latest_dataset(mock_client, "MONITOR")
    assert ds.id == 42
    assert ds.external_id == "monitor_ext"


def test_get_latest_dataset_process(mock_client):
    ds = get_latest_dataset(mock_client, "PROCESS")
    assert ds.id == 42
    assert ds.external_id == "process_ext"


def test_no_dataset_config():
    client = MagicMock()
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = []
    with pytest.raises(ValueError, match="No dataset configuration found."):
        get_latest_dataset(client, "READ")


def test_no_external_id():
    client = MagicMock()
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = [
        DummyConfig(None, None, None, None, 100)
    ]
    with pytest.raises(ValueError, match="No external_id found for data_set_type: READ"):
        get_latest_dataset(client, "READ")


def test_dataset_not_found():
    client = MagicMock()
    client.v1.day_ahead_configuration.data_set_configuration.list.return_value = [
        DummyConfig("read_ext", "write_ext", "monitor_ext", "process_ext", 100)
    ]
    client.cdf.data_sets.retrieve.return_value = None
    with pytest.raises(ValueError, match="Dataset with external_id 'read_ext' not found."):
        get_latest_dataset(client, "READ")


def test_invalid_type(mock_client):
    with pytest.raises(ValueError, match="Unknown data_set_type: INVALID"):
        get_latest_dataset(mock_client, "INVALID")


def test_multiple_configs_latest_used(mock_client_multiple_configs):
    # Should use the config with last_updated_time=200
    ds = get_latest_dataset(mock_client_multiple_configs, "READ")
    assert ds.external_id == "new_read"
    ds = get_latest_dataset(mock_client_multiple_configs, "WRITE")
    assert ds.external_id == "new_write"
    ds = get_latest_dataset(mock_client_multiple_configs, "MONITOR")
    assert ds.external_id == "new_monitor"
    ds = get_latest_dataset(mock_client_multiple_configs, "PROCESS")
    assert ds.external_id == "new_process"
