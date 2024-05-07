# Mypy does not understand the pydantic classes that allows both alias and name to be used in population
# https://github.com/pydantic/pydantic/issues/3923
# mypy: disable-error-code="call-arg"
from __future__ import annotations

import logging
import random
import re
from pathlib import Path
from typing import Optional, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadataWrite
from cognite.client.exceptions import CogniteAPIError

import cognite.powerops.client._generated.v1.data_classes as v1_data_classes
from cognite.powerops.resync.v2.data_classes import (
    FileUploadConfiguration,
)

logger = logging.getLogger(__name__)


def get_type_prefix(domain_model_type: Union[type | str]) -> str:
    """Get the type prefix for a domain model type.

    Given the domain model type, this function returns the type prefix as a snake case string. Also strips the "Write"
    suffix from the type name to get the root type name.

    Args:
        domain_model_type: The domain model type to get the prefix for.

    Returns:
        The type prefix as a snake case string.
    """

    if isinstance(domain_model_type, str):
        type_name = domain_model_type
    else:
        type_name = domain_model_type.__name__

    # Convert the type name to snake case
    external_id_prefix = re.sub(r"(?<!^)(?=[A-Z])", "_", type_name.replace("Write", "")).lower()
    return external_id_prefix


def ext_id_factory(domain_model_type: type, data: dict) -> str:
    """Generate an external id based on type and data input.

    Given a domain model type and a dictionary of data, this function generates an external id based on the type
    and data input. The external id is generated by combining the type prefix with a cleaned snake case version of
    the name field in the data. Certain types have special handling for external id generation, and if not the name
    field is required and will raise an error if missing.

    Args:
        domain_model_type: The domain model type to generate an external id for.
        data: The data to generate the external id from.

    Returns:
        The generated external id string in snake case format.

    Raises:
        ValueError: If the name field is missing in the data input.
    """
    # TODO: get better external ids
    type_prefix = get_type_prefix(domain_model_type)

    if "external_id" in data:
        return data["external_id"]

    # TODO proper naming
    if domain_model_type is v1_data_classes.GeneratorEfficiencyCurveWrite:
        return f"{type_prefix}_foo_{random.random()}"
    elif domain_model_type is v1_data_classes.TurbineEfficiencyCurveWrite:
        return f"{type_prefix}_foo_{random.random()}"

    try:
        name = data["name"]
    except KeyError as exc:
        raise ValueError(f"Missing required `name` field for data type {type_prefix}, {data}") from exc

    cleaned_name = name.lower().replace(" ", "_").replace("-", "_")

    return f"{type_prefix}_{cleaned_name}"


def get_external_id_from_field(
    key: str, value: str, all_domain_type_properties_types: dict[str, str], all_write_classes: dict[str, type]
) -> Optional[str]:
    """Get the external id for a field based on the input value being a reference.

    Given a key and value pair, this function checks if the value is a reference to another object and generates an
    external id based on the reference. The reference can be a direct external id, a reference to another object by
    name, or a reference to another object by name and type. If the value is a reference, the function generates an
    external id based on the reference and returns it. If the value is not a reference, the function returns None.

    Args:
        key: The key of the field in the data input.
        value: The value of the field in the data input.
        all_domain_type_properties_types: A dictionary mapping domain model type properties to their types.
        all_write_classes: A dictionary mapping type prefixes to their domain model types.

    Returns:
        The referenced external id if the value is a reference, otherwise None.

    Raises:
        ValueError: Invalid external id reference.
    """

    if value.startswith("[external_id]"):
        return value.replace("[external_id]", "")
    elif value.startswith("[name]"):
        name_reference = value.replace("[name]", "")
        expected_value_type = all_domain_type_properties_types[key]
        reference_type = get_property_type_from_annotation_string(expected_value_type)
        return ext_id_factory(reference_type, {"name": name_reference})
    elif value.startswith("[name|type:"):
        pattern = r"\[name\|type:(.*?)\](.*)"
        match = re.match(pattern, value)
        if match:
            type_prefix = get_type_prefix(match.group(1))
            reference_type = all_write_classes[type_prefix]
            name_reference = match.group(2).strip()
        else:
            raise ValueError(f"Invalid external id reference {value}")
        return ext_id_factory(reference_type, {"name": name_reference})

    return None


def get_property_type_from_annotation_string(annotation: str) -> type:
    """Get the domain model type from a string.

    Provided a string, this function returns the domain model write type corresponding to the string. The string is
    expected to be a type prefix, and the function returns the domain model type corresponding to the type prefix. If
    the type prefix is not supported, the function raises a KeyError.

    Args:
        type_string: The type prefix string to get the domain model type for.

    Returns:
        The domain model write type corresponding to the type prefix.

    Raises:
        ValueError: Type is not supported, add import to type
        ValueError: Invalid property for type reference
    """

    regex_pattern = r"\b(\w*Write)\b"
    matches = re.findall(regex_pattern, annotation)

    if matches:
        try:
            type_string = matches[0]
            if not type_string.endswith("Write"):
                type_string += "Write"
            return v1_data_classes.__dict__[type_string]
        except KeyError as exc:
            raise ValueError(f"Type {matches[0]} is not supported, add import to type") from exc
    else:
        raise ValueError(f"Invalid property for type reference {annotation}")


def check_input_keys(data: dict, all_domain_type_properties: list[str]) -> None:
    """Check that all keys in the input data are valid domain model properties.

    Given a dictionary of data, this function checks that all keys in the data are valid domain model properties. If a
    key is not a valid domain model property, the function raises a ValueError.

    Args:
        data: The input data to check.
        all_domain_type_properties: A list of all valid domain model properties.

    Raises:
        ValueError: Key not in domain model properties.
    """

    for key in data.keys():
        if key not in all_domain_type_properties:
            raise ValueError(f"Key {key} not in domain model properties")


def parse_external_ids(
    data: dict, all_domain_type_properties_types: dict[str, str], all_write_classes: dict[str, type]
) -> tuple[dict, list[str]]:
    """Parse external id references from the input data.

    Given a dictionary of data, this function parses external id references from the data. The function iterates over
    all key-value pairs in the data and checks if the value is a reference to another object. If the value is a
    reference, the function generates an external id based on the reference and replaces the value with the generated
    external id. The function returns the updated data and a list of all external ids referenced in the data.

    Args:
        data: The input data to parse external id references from.
        all_domain_type_properties_types: A dictionary mapping domain model type properties to their types.
        all_write_classes: A dictionary mapping type prefixes to their domain model types.

    Returns:
        data: The updated data with external id references replaced.
        reference_external_ids: A list of all external ids referenced in the data.
    """

    reference_external_ids = []

    for key, value in data.items():
        if isinstance(value, str):
            external_id_reference = get_external_id_from_field(
                key, value, all_domain_type_properties_types, all_write_classes
            )
            if external_id_reference:
                data[key] = external_id_reference
                reference_external_ids.append(external_id_reference)
        elif isinstance(value, list):
            sub_data = []
            for sub_value in value:
                if isinstance(sub_value, str):
                    external_id_reference = get_external_id_from_field(
                        key, sub_value, all_domain_type_properties_types, all_write_classes
                    )
                    if external_id_reference:
                        sub_data.append(external_id_reference)
                        reference_external_ids.append(external_id_reference)
                    else:
                        sub_data.append(sub_value)
                else:
                    # TODO: can this be a mix of external ids and other data?
                    sub_data.append(sub_value)
            data[key] = sub_data

    return data, reference_external_ids


def upload_files_to_cdf(
    cdf_client: CogniteClient, file_configuration: FileUploadConfiguration, directory_path: Path, data_set_id: int
) -> list[str]:
    """Upload files to CDF.

    Given a file upload configuration, this function uploads the files to CDF. The function iterates over all files in
    the file configuration and uploads them to CDF. The function also checks if the file already exists in CDF and
    updates the file if it does. The function returns a list of all external ids for the uploaded files.

    Args:
        cdf_client: The Cognite client to use for file upload.
        file_configuration: The file upload configuration to use for file upload.
        directory_path: The directory path to the files to upload.
        data_set_id: The data set id to use for the files.

    Returns:
        file_external_ids: A list of all external ids for the uploaded files.

    Raises:
        CogniteAPIError: If the file already exists in CDF.
        ValueError: External id is required for file upload.
    """
    full_path = file_configuration.folder_path

    file_list = [file.name for file in full_path.iterdir() if file.is_file()]

    file_external_ids = []
    for file_name in file_list:
        file_metadata = file_configuration.file_metadata.get(
            file_name,
            FileMetadataWrite(
                name=file_name,
                external_id=ext_id_factory(FileMetadataWrite, {"name": file_name}),
            ),
        )

        file_metadata.data_set_id = file_metadata.data_set_id or data_set_id
        try:
            if file_metadata.external_id:
                cdf_client.files.upload(
                    path=str(full_path / file_name),
                    overwrite=file_configuration.overwrite,
                    **file_metadata.dump(camel_case=False),
                )
                if file_configuration.overwrite:
                    cdf_client.files.update(file_metadata)
                file_external_ids.append(file_metadata.external_id)
            else:
                raise ValueError("External id is required for file upload")
        except CogniteAPIError as exc:
            if exc.code == 409:
                logger.warning(f"File with external_id {file_metadata.external_id} already exists in CDF")
            else:
                raise exc

    return file_external_ids


def check_all_linked_sources_exist(
    data_model_objects: list,
    file_external_ids: list[str],
    time_series_external_ids: list[str],
    other_external_ids: list[str],
) -> None:
    """Check that all linked sources exist or are referenced in the data model objects being upserted.

    TODO: Implement this function.

    Provided a list of external ids for files, time series, and other objects, this function checks that all linked
    sources exist or are referenced in the data model objects being upserted. The function iterates over all data model
    objects and checks that all linked sources exist or are referenced in the object. If a linked source does not exist,
    the function raises an error.

    Args:
        data_model_objects: A list of all data model objects being upserted.
        file_external_ids: A list of all external ids for files.
        time_series_external_ids: A list of all external ids for time series.
        other_external_ids: A list of all other external ids.

    Raises:
        ValueError: External id not found in linked sources.
    """
    print(data_model_objects)
    print(file_external_ids)
    print(time_series_external_ids)
    print(other_external_ids)
