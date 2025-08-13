import pytest

from cognite.powerops.client._generated.v1.data_classes import DataSetConfiguration
from cognite.powerops.client._generated.v1.data_classes._core.base import DataRecordWrite
from cognite.powerops.client._generated.v1.data_classes._data_set_configuration import DataSetConfigurationWrite
from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.utils.cdf.datasets_calls import get_latest_dataset


@pytest.fixture
def temp_datasetconfiguration(power_ops_client: PowerOpsClient):
    config = DataSetConfigurationWrite(
        external_id="integration_temp_config",
        name="Integration Temp Config",
        read_data_set="powerops:process",
        write_data_set="powerops:process",
        monitor_data_set="new_powerops:monitor",
        process_data_set="powerops:process",
        data_record=DataRecordWrite(version=1),
    )
    power_ops_client.v1.upsert(config)
    yield config
    power_ops_client.v1.delete(external_id=config.external_id)


def test_get_current_dataset_configuration_all_types(power_ops_client: PowerOpsClient):
    ds_read = get_latest_dataset(power_ops_client, "READ")
    ds_write = get_latest_dataset(power_ops_client, "WRITE")
    ds_monitor = get_latest_dataset(power_ops_client, "MONITOR")
    ds_process = get_latest_dataset(power_ops_client, "PROCESS")

    assert ds_read is not None
    assert ds_write is not None
    assert ds_monitor is not None
    assert ds_process is not None

    assert ds_read.external_id == "powerops:process"
    assert ds_write.external_id == "powerops:process"
    assert ds_monitor.external_id == "powerops:monitor"
    assert ds_process.external_id == "powerops:process"


def test_create_and_get_latest_dataset_configuration(
    power_ops_client: PowerOpsClient, temp_datasetconfiguration: DataSetConfiguration
):
    print(f"Created dataset configuration: {temp_datasetconfiguration}")
    print(f"Latest READ dataset: {get_latest_dataset(power_ops_client, 'READ')}")
    assert get_latest_dataset(power_ops_client, "READ").external_id == "powerops:process"
    assert get_latest_dataset(power_ops_client, "WRITE").external_id == "powerops:process"
    assert get_latest_dataset(power_ops_client, "MONITOR").external_id == "new_powerops:monitor"
    assert get_latest_dataset(power_ops_client, "PROCESS").external_id == "powerops:process"
