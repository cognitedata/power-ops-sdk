from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes import DataSet

from cognite.powerops.client._generated.data_classes import (
    DataRecord,
    DataSetConfiguration,
)
from cognite.powerops.utils.retrieve import get_data_set_from_config


@pytest.fixture
def mock_client() -> MagicMock:
    client = MagicMock()
    client.powermodel.day_ahead_configuration.data_set_configuration.list.return_value = [
        DataSetConfiguration(
            external_id="config1",
            name="Config 1",
            read_data_set="read_ext",
            write_data_set="write_ext",
            monitor_data_set="monitor_ext",
            process_data_set="process_ext",
            data_record=DataRecord(last_updated_time=1234567890, created_time=1234560000, version=1),
        )
    ]
    client.cdf.data_sets.retrieve.side_effect = lambda external_id: (
        DataSet(id=42, external_id=external_id) if external_id else None
    )
    return client


@pytest.fixture
def mock_client_multiple_configs():
    client = MagicMock()
    client.powermodel.day_ahead_configuration.data_set_configuration.list.return_value = [
        DataSetConfiguration(
            external_id="old_config",
            name="Old Config",
            read_data_set="old_read",
            write_data_set="old_write",
            monitor_data_set="old_monitor",
            process_data_set="old_process",
            data_record=DataRecord(last_updated_time=200, created_time=1234560000, version=1),
        ),
        DataSetConfiguration(
            external_id="new_config",
            name="New Config",
            read_data_set="new_read",
            write_data_set="new_write",
            monitor_data_set="new_monitor",
            process_data_set="new_process",
            data_record=DataRecord(last_updated_time=1234567890, created_time=1234560000, version=1),
        ),
    ]
    client.cdf.data_sets.retrieve.side_effect = lambda external_id: (
        DataSet(id=99, external_id=external_id) if external_id else None
    )
    return client


def test_mock_client_side_effect(mock_client: MagicMock):
    ds = mock_client.cdf.data_sets.retrieve(external_id="monitor_ext")

    assert ds is not None
    assert ds.id == 42
    assert ds.external_id == "monitor_ext"


class TestGetLatestDataset:
    def test_get_data_set_from_config_read(self, mock_client: MagicMock):
        ds = get_data_set_from_config(mock_client, "READ")
        assert ds.id == 42
        assert ds.external_id == "read_ext"

    def test_get_data_set_from_config_write(self, mock_client: MagicMock):
        ds = get_data_set_from_config(mock_client, "WRITE")
        assert ds.id == 42
        assert ds.external_id == "write_ext"

    def test_get_data_set_from_config_monitor(self, mock_client: MagicMock):
        ds = get_data_set_from_config(mock_client, "MONITOR")
        assert ds.id == 42
        assert ds.external_id == "monitor_ext"

    def test_get_data_set_from_config_process(self, mock_client: MagicMock):
        ds = get_data_set_from_config(mock_client, "PROCESS")
        assert ds.id == 42
        assert ds.external_id == "process_ext"

    def test_no_dataset_config(self):
        client = MagicMock()
        client.powermodel.day_ahead_configuration.data_set_configuration.list.return_value = []
        with pytest.raises(ValueError, match="No dataset configuration found."):
            get_data_set_from_config(client, "READ")

    def test_no_external_id(self):
        client = MagicMock()
        client.powermodel.day_ahead_configuration.data_set_configuration.list.return_value = [
            DataSetConfiguration(
                external_id="no_extid",
                name="No External Id",
                read_data_set="",
                write_data_set="",
                monitor_data_set="",
                process_data_set="",
                data_record=DataRecord(last_updated_time=100, created_time=50, version=1),
            )
        ]
        with pytest.raises(ValueError, match="No external_id found for data_set_type: READ"):
            get_data_set_from_config(client, "READ")

    def test_dataset_not_found(self):
        client = MagicMock()
        client.powermodel.day_ahead_configuration.data_set_configuration.list.return_value = [
            DataSetConfiguration(
                external_id="config1",
                name="Config 1",
                read_data_set="read_ext",
                write_data_set="write_ext",
                monitor_data_set="monitor_ext",
                process_data_set="process_ext",
                data_record=DataRecord(last_updated_time=100, created_time=50, version=1),
            )
        ]
        client.cdf.data_sets.retrieve.return_value = None
        with pytest.raises(ValueError, match="Dataset with external_id 'read_ext' not found."):
            get_data_set_from_config(client, "READ")

    def test_invalid_type(self, mock_client: MagicMock):
        with pytest.raises(ValueError, match="Unknown data_set_type: INVALID"):
            get_data_set_from_config(mock_client, "INVALID")

    def test_multiple_configs_latest_used(self, mock_client_multiple_configs):
        # Should use the config with last_updated_time=1234567890
        ds = get_data_set_from_config(mock_client_multiple_configs, "READ")
        assert ds.external_id == "new_read"
        ds = get_data_set_from_config(mock_client_multiple_configs, "WRITE")
        assert ds.external_id == "new_write"
        ds = get_data_set_from_config(mock_client_multiple_configs, "MONITOR")
        assert ds.external_id == "new_monitor"
        ds = get_data_set_from_config(mock_client_multiple_configs, "PROCESS")
        assert ds.external_id == "new_process"
