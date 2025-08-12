from typing import Literal

from cognite.client.data_classes import DataSet

from cognite.powerops.client.powerops_client import PowerOpsClient

DataSetType = Literal["READ", "WRITE", "MONITOR", "PROCESS"]


def get_latest_dataset(client: PowerOpsClient, data_set_type: DataSetType = "READ") -> DataSet | None:
    dataset_configs = client.v1.day_ahead_configuration.data_set_configuration.list(limit=-1)
    if not dataset_configs:
        raise ValueError("No dataset configuration found.")

    # Pick the config with the latest last_updated_time
    dataset_config = max(
        dataset_configs,
        key=lambda cfg: getattr(cfg.data_record, "last_updated_time", 0),
    )

    if data_set_type == "READ":
        external_id = dataset_config.read_data_set
    elif data_set_type == "WRITE":
        external_id = dataset_config.write_data_set
    elif data_set_type == "MONITOR":
        external_id = dataset_config.monitor_data_set
    elif data_set_type == "PROCESS":
        external_id = dataset_config.process_data_set
    else:
        raise ValueError(f"Unknown data_set_type: {data_set_type}")

    if not external_id:
        raise ValueError(f"No external_id found for data_set_type: {data_set_type}")

    dataset = client.cdf.data_sets.retrieve(external_id=external_id)
    if dataset is None:
        raise ValueError(f"Dataset with external_id '{external_id}' not found.")
    return dataset
