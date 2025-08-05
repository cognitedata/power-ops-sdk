from typing import Literal

from cognite.powerops.client.powerops_client import PowerOpsClient

DataSetType = Literal["READ", "WRITE", "MONITOR"]


def get_dataset_id(client: PowerOpsClient, data_set_type: DataSetType = "READ") -> int:
    # Fetch config object
    dataset_configs = client.v1.day_ahead_configuration.data_set_configuration.list(limit=-1)

    if not dataset_configs:
        raise ValueError("No dataset configuration found.")

    dataset_config = dataset_configs[0]
    for current_config in dataset_configs:
        if current_config.data_record.last_updated_time > dataset_config.data_record.last_updated_time:
            dataset_config = current_config

    # Assuming we want the first one
    dataset_config = dataset_configs[0]
    external_id: str | None = ""
    if data_set_type == "READ":
        # This is the external id and then we need to find what the id is.
        external_id = dataset_config.read_data_set
    elif data_set_type == "WRITE":
        external_id = dataset_config.write_data_set
    elif data_set_type == "MONITOR":
        external_id = dataset_config.monitor_data_set
    else:
        raise ValueError(f"Unknown data_set_type: {data_set_type}")
    if not external_id:  # Monitor is set as Optional on our container
        raise ValueError(f"No external_id found for data_set_type: {data_set_type}")
    # fetch the actual id and return it . if client is powerops client then .cdf returns cognite client.
    # cognite client to retrieve the id.
    dataset = client.cdf.data_sets.retrieve(external_id=external_id)
    if dataset is None:
        raise ValueError(f"Dataset with external_id '{external_id}' not found.")
    return dataset.id
