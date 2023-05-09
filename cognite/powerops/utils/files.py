from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
from cognite.client import CogniteClient

from cognite.powerops.data_classes.shop_file_config import ShopFileConfig
from cognite.powerops.utils.serializer import load_yaml


def upload_shop_config_file(
    client: CogniteClient,
    config: ShopFileConfig,
    data_set_id: int,
    overwrite: bool = True,
) -> None:
    if config.path is None:
        raise ValueError("The Path must be set to upload file")
    file_content = Path(config.path).read_bytes()
    if config.md5_hash is None:
        config.set_md5_hash(file_content)
    try:
        file = client.files.upload_bytes(
            content=file_content,
            external_id=config.external_id,
            name=config.external_id,
            metadata=config.metadata,
            source="PowerOps bootstrap",
            mime_type="text/plain",
            data_set_id=data_set_id,
            overwrite=overwrite,
        )
        print(f"Uploaded file with externalId {file.external_id}")
    except Exception as e:
        print(e)


def shop_attribute_value_is_time_series(shop_attribute_value) -> bool:
    return isinstance(shop_attribute_value, dict) and isinstance(list(shop_attribute_value)[0], datetime)


# ! Assumes yaml with "model", "time" and "connections"!!
# --> TODO: "time" is just dropped?
# TODO: extract loading of yaml
def get_model_without_timeseries(
    yaml_raw_path: str,
    non_time_series_attributes_to_remove: Optional[list[str]] = None,
    encoding=None,
) -> dict:
    if non_time_series_attributes_to_remove is None:
        non_time_series_attributes_to_remove = []

    model_incl_time_and_connections = load_yaml(Path(yaml_raw_path), encoding=encoding, clean_data=True)

    model = model_incl_time_and_connections["model"]

    for object_type, objects in model.items():
        for object_attributes in objects.values():
            for attribute_name, attribute_data in list(
                object_attributes.items()
            ):  # Needs list() since we pop while iterating over the dict
                if f"{object_type}.{attribute_name}" in non_time_series_attributes_to_remove:
                    object_attributes.pop(attribute_name)

                elif shop_attribute_value_is_time_series(attribute_data):
                    object_attributes.pop(attribute_name)

    model_dict = {"model": model}

    # TODO: remove this when standardizing input SHOP file strucutre (model vs model + connections)
    if "connections" in model_incl_time_and_connections:
        model_dict["connections"] = model_incl_time_and_connections["connections"]

    return model_dict


def process_yaml_file(yaml_raw_path: str, yaml_processed_path: str) -> None:
    """Process raw YAML file and store as new file"""
    # Remove timeseries
    model_without_timeseries = get_model_without_timeseries(
        yaml_raw_path=yaml_raw_path,
        non_time_series_attributes_to_remove=["reservoir.start_vol", "generator.initial_state"],
        encoding="utf-8",
    )

    # Save cleaned model
    with open(yaml_processed_path, "w", encoding="utf-8") as stream:
        yaml.safe_dump(data=model_without_timeseries, stream=stream, allow_unicode=True, sort_keys=False)
