import pytest
from cognite.client.data_classes import DataSet

from cognite.powerops.client._generated.v1.data_classes import DataSetConfiguration
from cognite.powerops.client._generated.v1.data_classes._data_set_configuration import DataSetConfigurationWrite
from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.utils.retrieve import get_data_set_from_config
from tests.test_integration.conftest import random_external_id


@pytest.fixture
def new_data_set_configuration(power_ops_client: PowerOpsClient):
    external_id = random_external_id("pytest_data_set_config")
    config = DataSetConfigurationWrite(
        external_id=external_id,
        name="Test Data Set Configuration",
        read_data_set="powerops:process",
        write_data_set="powerops:process",
        monitor_data_set="new_powerops:monitor",
        process_data_set="powerops:process",
    )
    power_ops_client.v1.upsert(config)
    yield config
    power_ops_client.v1.delete(external_id=config.external_id)
    assert power_ops_client.v1.day_ahead_configuration.data_set_configuration.retrieve(external_id=external_id) is None


class TestGetLatestDataset:
    def test_all_data_set_types(self, power_ops_client: PowerOpsClient):
        # Relies on at least the DataSetConfiguration having been created by resync/toolkit
        # See resync/shared/data_set_configuration.py for definition
        ds_read = get_data_set_from_config(power_ops_client, "READ")
        ds_write = get_data_set_from_config(power_ops_client, "WRITE")
        ds_monitor = get_data_set_from_config(power_ops_client, "MONITOR")
        ds_process = get_data_set_from_config(power_ops_client, "PROCESS")

        # Given multiple tests can run in parallel we can't assume only the resync DataSetConfiguration exists
        # Only check that the datasets are not None and are of the correct type
        assert ds_read is not None
        assert ds_write is not None
        assert ds_monitor is not None
        assert ds_process is not None

        assert isinstance(ds_read, DataSet)
        assert isinstance(ds_write, DataSet)
        assert isinstance(ds_monitor, DataSet)
        assert isinstance(ds_process, DataSet)

    def test_latest_dataset_configuration(
        self, power_ops_client: PowerOpsClient, new_data_set_configuration: DataSetConfiguration
    ):
        assert (
            get_data_set_from_config(power_ops_client, "READ").external_id == new_data_set_configuration.read_data_set
        )
        assert (
            get_data_set_from_config(power_ops_client, "WRITE").external_id == new_data_set_configuration.write_data_set
        )
        assert (
            get_data_set_from_config(power_ops_client, "MONITOR").external_id
            == new_data_set_configuration.monitor_data_set
        )
        assert (
            get_data_set_from_config(power_ops_client, "PROCESS").external_id
            == new_data_set_configuration.process_data_set
        )
