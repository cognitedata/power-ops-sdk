# Mypy does not understand the pydantic classes that allows both alias and name to be used in population
# https://github.com/pydantic/pydantic/issues/3923
# mypy: disable-error-code="call-arg"
from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Optional

from cognite.client import CogniteClient
from pydantic import ValidationError

import cognite.powerops.client._generated.v1.data_classes as v1_data_classes
from cognite.powerops.resync.data_classes import (
    DataModelConfiguration,
    FileUploadConfiguration,
    PropertyConfiguration,
)
from cognite.powerops.resync.utils import (
    check_input_keys,
    ext_id_factory,
    parse_external_ids,
)
from cognite.powerops.utils.serialization import load_yaml

logger = logging.getLogger(__name__)

__all__ = ["ResyncImporter"]


class ResyncImporter:
    """Importer class for resync to populate the data model from yaml configuration files.

    Attributes:
        data_model_configuration_file: Path to the data model configuration file.
        working_directory: Path to the working directory.
        overwrite_data: A boolean to determine if data should be overwritten.
        data_set_id: An integer of the data set id.
        file_configuration: A FileUploadConfiguration object detailing the configuration of file uploads.
        data_model_configuration: A dictionary of types to DataModelConfiguration detailing all configuration
                                  properties of the data model type.
        data_model_classes: A dictionary of types to DomainModelWrite classes detailing all data model classes to be
                            used for the resync configuration.
    """

    working_directory: Path
    overwrite_data: bool
    folders_to_process: list[Path]
    data_model_classes: dict[str, type]
    data_model_configuration: dict[str, DataModelConfiguration]
    file_configuration: Optional[FileUploadConfiguration]

    def __init__(
        self,
        working_directory: Path,
        overwrite_data: Optional[bool],
        data_model_classes: dict[str, type],
        file_configuration: Optional[FileUploadConfiguration],
        data_model_configuration: Optional[dict[str, DataModelConfiguration]] = None,
    ) -> None:
        """Initializes the ResyncImporter"""

        self.working_directory = working_directory
        self.folders_to_process = [p for p in self.working_directory.iterdir() if p.is_dir()]
        if Path(self.working_directory / "files") in self.folders_to_process:
            self.folders_to_process.remove(self.working_directory / "files")

        self.overwrite_data = overwrite_data or False
        self.file_configuration = file_configuration
        self.data_model_configuration = data_model_configuration or {}
        self.data_model_classes = data_model_classes

        v1_data_classes.DomainModelWrite.external_id_factory = ext_id_factory

    @classmethod
    def from_yaml(
        cls,
        configuration_path: Path,
        data_model_classes: dict[str, type],
        cdf_client: CogniteClient,
    ) -> ResyncImporter:
        """Creates a ResyncImporter object from a resync configuration file.

        Args:
            configuration_path: Path to the resync configuration file.
            data_model_classes: A dictionary of all data model classes to be used for the resync configuration.

        Returns:
            A ResyncImporter object.
        """
        configuration = load_yaml(configuration_path, expected_return_type="dict")

        working_directory = Path(configuration.get("working_directory", Path.cwd()))

        if "file_configuration" in configuration:
            file_configuration_dict = configuration.get("file_configuration", {})

            data_set_id = file_configuration_dict.pop("data_set")

            file_configuration = FileUploadConfiguration(
                working_directory=working_directory,
                data_set_id=data_set_id,
                cdf_client=cdf_client,
                **file_configuration_dict if file_configuration_dict else {},
            )

        return cls(
            working_directory=working_directory,
            overwrite_data=configuration.get("overwrite_data"),
            file_configuration=file_configuration,
            data_model_classes=data_model_classes,
        )

    def to_data_model(self) -> tuple[list[v1_data_classes.DomainModelWrite], list[str]]:
        """Converts configuration files to data model objects based on provided resync configuration.

        Retrieves raw data from files and using the resync configuration parses the data into
        data model objects.

        Returns:
            data_model_objects: A list of all data model objects created from the configuration files.
            external_ids: A list of all external ids created and referenced from the data model objects.
        """

        data_model_objects: dict[str, v1_data_classes.DomainModelWrite] = {}
        external_ids: list[str] = []

        for folder in self.folders_to_process:
            all_data_model_files = list((folder).glob("*.yaml"))

            data_model_configuration_file = folder / "data_model_configuration.yaml"
            if data_model_configuration_file in all_data_model_files:
                all_data_model_files.remove(data_model_configuration_file)
                self.data_model_configuration = DataModelConfiguration.from_yaml(
                    self.data_model_classes, data_model_configuration_file
                )
            else:
                self.data_model_configuration = DataModelConfiguration.from_yaml(self.data_model_classes)
                logger.info(f"No data model configuration file found in {folder}, using default configuration")

            for data_model_file in all_data_model_files:
                # TODO: allow using prefix of file name to match type and add extra context to rest of file name?
                if data_model_file.stem not in self.data_model_configuration:
                    logger.warning(f"Skipping {data_model_file} as it is not in the supported types")
                    continue

                type_configuration = self.data_model_configuration[data_model_file.stem]

                raw_data_model_objects = load_yaml(data_model_file, "list")

                logger.info(f"Processing {data_model_file} as type {type_configuration.name}")

                for unprocessed_object in raw_data_model_objects:
                    results = self._dict_to_type_object(unprocessed_object, type_configuration)
                    data_model_objects.update(results[0])
                    external_ids.extend(results[1])

        return list(data_model_objects.values()), external_ids

    def _dict_to_type_object(
        self,
        raw_data: dict,
        type_configuration: DataModelConfiguration,
    ) -> tuple[dict[str, v1_data_classes.DomainModelWrite], list[str]]:
        """Converts a dictionary object into the specified data type.

        Provided the data input and type configuration the dictionary will be updated with properties to be overridden
        from other sources and then converted into the specified data type.

        Args:
            data: A dictionary of data to be converted into the specified data type.
            type_configuration: A DataModelConfiguration object detailing the configuration of the data type.

        Returns:
            data_model_objects: A dictionary of all data model objects created from the input data.
            reference_external_ids: A list of all external ids created and referenced from the data model objects.

        Raises:
            ValueError: If any required fields are missing or invalid for the specified data type.
        """

        logger.info(f"Processing {raw_data} as type {type_configuration.type_.__name__}")

        # check if any fields from input data don't match the domain model
        check_input_keys(raw_data, list(type_configuration.properties.keys()))

        # split data into immutable, property configuration and reference data
        immutable_data, property_configuration_data, reference_data = self._split_data_by_parsing_method(raw_data)

        # populate properties from configurations
        parsed_properties, generated_data_model_objects = self._populate_properties_from_configurations(
            property_configuration_data, type_configuration.property_configurations, raw_data
        )

        # populate all external id references
        parsed_reference_data, reference_external_ids = parse_external_ids(
            reference_data, type_configuration.properties, self.data_model_classes
        )

        # join all dictionaries together in priority order
        parsed_data = self._join_data_by_parsing_method(immutable_data, parsed_properties, parsed_reference_data)

        # TODO: handle these special cases in a better way
        if type_configuration.type_ is v1_data_classes.PlantInformationWrite:
            parsed_data["generators"] = (
                self._get_all_connections(parsed_data, "plant", "generator")
                if "generators" not in parsed_data
                else parsed_data["generators"]
            )
            parsed_data["connection_losses"] = (
                self._get_connection_losses(parsed_data)
                if "connection_losses" not in parsed_data
                else parsed_data["connection_losses"]
            )

        # TODO: extract all sub objects into the data_model_objects list
        try:
            fdm_object = type_configuration.type_(**parsed_data)
            generated_data_model_objects[fdm_object.external_id] = fdm_object
        except ValidationError as exc:
            raise ValueError(
                f"Missing/invalid field for {type_configuration.type_.__name__} in data {parsed_data}"
            ) from exc

        return generated_data_model_objects, reference_external_ids

    @staticmethod
    def _split_data_by_parsing_method(
        raw_data: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        """Splits an unparsed dictionary into a dictionary for each type of parsing should be applied.

        Provided the data input the dictionary will be split into immutable data, property configuration data and
        reference data. Where immutable data is the data that will not be overridden, property configuration data
        will be overridden from other sources based on the property configuration provided and reference data will
        be used to fetch the external ids.

        Args:
            raw_data: A dictionary of data to be split into the different parsing methods based on the values.

        Returns:
            immutable_data: A dictionary of data that will not be overridden by any other settings.
            property_configuration_data: A dictionary of data that will be overridden from other sources.
            reference_data: A dictionary of data that will be used to fetch the external ids.
        """

        immutable_data: dict[str, Any] = {}
        property_configuration_data: dict[str, Any] = {}
        reference_data: dict[str, Any] = {}

        for key, value in raw_data.items():
            if isinstance(value, str) and value.startswith("[SOURCE"):
                property_configuration_data[key] = value
            elif isinstance(value, str) and (value.startswith("[external_id") or value.startswith("[name")):
                reference_data[key] = value
            elif isinstance(value, list):
                immutable_subdata = []
                reference_subdata = []
                for item in value:
                    if isinstance(item, str) and (item.startswith("[external_id") or item.startswith("[name")):
                        reference_subdata.append(item)
                    else:
                        immutable_subdata.append(item)
                if reference_subdata:
                    reference_data[key] = reference_subdata
                if immutable_subdata:
                    immutable_data[key] = immutable_subdata
            else:
                immutable_data[key] = value

        return immutable_data, property_configuration_data, reference_data

    @staticmethod
    def _join_data_by_parsing_method(
        immutable_data: dict[str, Any], property_configuration_data: dict[str, Any], reference_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Joins dictionaries together in a prioritized order based on the parsing method.

        Provided the data input the dictionaries will be joined together in a prioritized order based on the parsing
        method. The immutable data will override any other data, followed by the reference data and lastly the
        property configuration data so that any general property configurations will be overridden by the specific
        instance data.

        Args:
            immutable_data: A dictionary of data that will not be overridden by any other settings.
            property_configuration_data: A dictionary of data that will be overridden from other sources.
            reference_data: A dictionary of data that will be overridden or joined only with the immutable data.

        Returns:
            joined_data: A dictionary of all data joined together in a prioritized order based on the parsing method.
        """

        joined_data = property_configuration_data.copy()
        joined_data.update(reference_data)

        for key, value in immutable_data.items():
            if key in joined_data and isinstance(joined_data[key], list):
                joined_data[key].extend(value)
            else:
                joined_data[key] = value

        return joined_data

    def _populate_properties_from_configurations(
        self, property_configuration_data: dict, property_configurations: list[PropertyConfiguration], raw_data: dict
    ) -> tuple[dict[str, Any], dict[str, v1_data_classes.DomainModelWrite]]:
        """Populates properties from configurations based on the provided data.

        Provided the data input and property configurations the data will be parsed based on the property configurations
        and the data will be updated with the parsed properties and any generated data model objects. The property
        configurations will be prioritized based on the union of the default property configurations and the override
        property. The generated data model objects will be added to the generated_data_model_objects dictionary.

        Args:
            property_configuration_data: A dictionary of data that will be overridden from other sources.
            property_configurations: A list of PropertyConfiguration objects detailing the configuration of
                                     each property.
            raw_data: A dictionary of data to be referenced based on the property configurations.

        Returns:
            parsed_properties: A dictionary of all parsed properties based on the property configurations.
            generated_data_model_objects: A dictionary of all generated data model objects from any subtype properties.
        """

        override_property_configurations = PropertyConfiguration.from_data(property_configuration_data)
        all_properties_to_extract = PropertyConfiguration.union_lists(
            property_configurations, override_property_configurations
        )

        parsed_properties = {}
        generated_data_model_objects = {}
        for property_configuration in all_properties_to_extract:
            if property_configuration.is_subtype:
                if property_configuration.is_list:
                    # TODO: simplify list logic
                    subtype_list = self._get_subtype_list(property_configuration, raw_data)

                    parsed_properties[property_configuration.property] = [
                        subtype.external_id for subtype in subtype_list
                    ]
                    for subtype in subtype_list:
                        if subtype.external_id in generated_data_model_objects:
                            logger.warning(
                                f"Subtype {subtype.external_id} already exists in generated data model objects"
                            )
                        generated_data_model_objects[subtype.external_id] = subtype
                else:
                    subtype = self._get_subtype_from_dictionary(property_configuration, raw_data)

                    parsed_properties[property_configuration.property] = subtype.external_id
                    generated_data_model_objects[subtype.external_id] = subtype
            else:
                parsed_properties[property_configuration.property] = self._get_property_value_from_source(
                    property_configuration, raw_data
                )

        return parsed_properties, generated_data_model_objects

    # TODO: check if this is the correct way to handle connections, update to not use from_type/to_type?
    # https://github.com/cognitedata/power-ops-sdk/pull/347#discussion_r1594731007
    def _get_all_connections(self, data: dict, power_asset_type_a: str, power_asset_type_b: str) -> list[str]:
        """Creates a list of all connections based on the provided data and asset types.

        Provided the data input and asset types the connections will be filtered based on the asset types and the
        connections will be returned as a list of external ids.

        Args:
            data: A dictionary of data to be used to filter the connections.
            power_asset_type_a: A string of the first power asset type to filter the connections.
            power_asset_type_b: A string of the second power asset type to filter the connections.

        Returns:
            connections: A list of all connections external ids based on the provided data and asset types.
        """

        all_connections = []
        source_files = set()
        for dm_config in self.data_model_configuration.values():
            for property_config in dm_config.property_configurations:
                if property_config.source_file:
                    source_files.add(property_config.source_file)

        for source_file in source_files:
            all_connections += self._get_property_value_from_source(
                PropertyConfiguration(
                    property="connections",
                    source_file=source_file,
                    extraction_path="connections",
                    cast_type=None,
                ),
                data,
            )
        connections = []

        name = data["name"]

        for connection in all_connections:
            if (
                connection["from"] == name
                and connection.get("from_type") == power_asset_type_a
                and connection.get("to_type") == power_asset_type_b
            ):
                connections.append(connection["to"])
            elif (
                connection["to"] == name
                and connection.get("to_type") == power_asset_type_a
                and connection.get("from_type") == power_asset_type_b
            ):
                connections.append(connection["from"])

        # TODO: use external_id logic?
        return [f"{power_asset_type_b}_{asset}".lower() for asset in set(connections)]

    def _get_connection_losses(self, data: dict) -> float:
        """Gets the connection losses based on the provided data.

        TODO: Implement connection losses calculation based on the provided data.

        Provided the data input the connection losses will be calculated based on the provided data.

        Args:
            data: A dictionary of data to be used to filter the connections.

        Returns:
            connection_losses: A float of the connection losses based on the provided data.
        """

        return 0.0

    def _get_property_value_from_source(
        self,
        property_configuration: PropertyConfiguration,
        instance_data: Optional[dict] = None,
        source_data: Optional[dict] = None,
    ) -> Any:
        """Gets the property value based on the provided data and property configuration.

        TODO: Implement type casting and list handling, simplify logic.

        Provided the data input and property configuration the property value will be extracted based on the
        property configuration extraction_path and source_file. The property value will be cast to the specified type
        if provided.

        Args:
            property_configuration: A PropertyConfiguration object detailing the configuration of the property.
            instance_data: A dictionary of instance data to be used for any reference data based on the
                            property configuration.
            source_data: A dictionary of source data to be used to extract the property value. If not provided the
                         source data will be loaded from the source_file in the property configuration.

        Returns:
            property_value: The property value extracted based on the provided data and property configuration.

        Raises:
            ValueError: No source data provided for property configuration
            ValueError: Source data is not a dictionary
            ValueError: Source data is not indexable
        """

        if source_data is None:
            source_data = {}
        if instance_data is None:
            instance_data = {}
        if not source_data:
            if property_configuration.source_file:
                path = self.working_directory / Path(property_configuration.source_file)
                source_data = load_yaml(path, "dict")
            else:
                if property_configuration.default_value:
                    return property_configuration.default_value
                raise ValueError("No source data or default value provided for property configuration")

        try:
            for key in property_configuration.extraction_path or []:
                match = re.match(r"\[(\w+)\]", key)
                if key == "value":
                    # TODO: handle value and key to not use next(iter)??
                    if isinstance(source_data, dict):
                        source_data = next(iter(source_data.values()))
                    else:
                        raise ValueError("Source data is not a dictionary")
                elif match:
                    if isinstance(source_data, (Sequence | Mapping)):
                        if match.group(1).isdigit():
                            source_data = source_data[int(match.group(1))]
                        else:
                            source_data = source_data[instance_data[match.group(1)]]
                    else:
                        raise ValueError("Source data is not indexable")
                elif isinstance(source_data, (Sequence | Mapping)):
                    source_data = source_data[key]
                else:
                    raise ValueError("Source data is not indexable")
        except KeyError:
            logger.warning(f"Key {key} not found in {property_configuration.source_file}, using default value")
            return property_configuration.default_value

        if property_configuration.is_list and property_configuration.cast_type:
            # TODO: handle for list of subtypes??
            return source_data
        elif property_configuration.is_list:
            return source_data

        if property_configuration.cast_type and source_data:
            return self.cast_into_type(source_data, property_configuration.cast_type)
        else:
            return source_data

    def _get_subtype_from_dictionary(
        self, property_configuration: PropertyConfiguration, instance_data: Optional[dict] = None
    ) -> Any:
        """Gets the subtype property value based on the provided data and property configuration.

        TODO: Implement subtype handling, simplify logic.

        Provided the data input and property configuration the property value will be extracted based on the
        property configuration extraction_path and source_file. The property value will be cast to the specified
        type if provided.

        Args:
            property_configuration: A PropertyConfiguration object detailing the configuration of the property.
            instance_data: A dictionary of instance data to be used for any reference data based on the property
                            configuration.

        Returns:
            property_value: The property value extracted based on the provided data and property configuration.

        Raises:
            ValueError: Type is not supported, add import to type
            ValueError: Property configuration requires a cast_type
        """

        if instance_data is None:
            instance_data = {}
        if property_configuration.cast_type not in self.data_model_configuration:
            raise ValueError(f"Type {property_configuration.cast_type} is not supported, add import to type")
        subtype_data_model_configuration = self.data_model_configuration[property_configuration.cast_type]

        subtype_data = {}
        for subtype_property_configuration in subtype_data_model_configuration.property_configurations:
            if not subtype_property_configuration.is_subtype:
                subtype_data[subtype_property_configuration.property] = self._get_property_value_from_source(
                    subtype_property_configuration, instance_data
                )
            else:
                # TODO: handle this scenario???
                logger.error("can't process")

        if property_configuration.cast_type:
            return self.cast_into_type(subtype_data, property_configuration.cast_type)
        else:
            raise ValueError(f"Property configuration {property_configuration} requires a cast_type")

    def _get_subtype_list(
        self, property_configuration: PropertyConfiguration, instance_data: Optional[dict] = None
    ) -> list[Any]:
        """Gets a list of subtype property values based on the provided data and property configuration.

        TODO: Implement subtype handling, simplify logic.

        Provided the data input and property configuration the property value will be extracted based on the property
        configuration extraction_path and source_file. The property value will be cast to the specified
        type if provided.

        Args:
            property_configuration: A PropertyConfiguration object detailing the configuration of the property.
            instance_data: A dictionary of instance data to be used for any reference data based on the property
                            configuration.

        Returns:
            parsed_list_data: A list of property values extracted based on the provided data and property configuration.

        Raises:
            ValueError: Type is not supported, add import to type
            ValueError: Property configuration requires a cast_type
        """

        if instance_data is None:
            instance_data = {}
        list_data = self._get_property_value_from_source(property_configuration, instance_data)
        if not list_data:
            return []
        if property_configuration.cast_type not in self.data_model_configuration:
            raise ValueError(f"Type {property_configuration.cast_type} is not supported, add import to type")
        subtype_data_model_configuration = self.data_model_configuration[property_configuration.cast_type]

        parsed_list_data = []
        for source_data in list_data:
            subtype_data = {}
            for subtype_property_configuration in subtype_data_model_configuration.property_configurations:
                if not subtype_property_configuration.is_subtype:
                    subtype_data[subtype_property_configuration.property] = self._get_property_value_from_source(
                        subtype_property_configuration, instance_data, source_data
                    )  # TODO: get value from source
                else:
                    # TODO: handle this scenario???
                    pass
            if property_configuration.cast_type:
                parsed_list_data.append(self.cast_into_type(subtype_data, property_configuration.cast_type))
            else:
                raise ValueError(f"Property configuration {property_configuration} requires a cast_type")

        return parsed_list_data

    def cast_into_type(self, data: dict[str, Any], cast_type: str) -> type:
        """Casts a dictionary into a type.

        Provided the data input and type the dictionary will be cast into the specified type.

        Args:
            data: A dictionary of data to be cast into the specified type.
            type_: A DomainModelWrite object detailing the type to cast the data into.

        Returns:
            fdm_object: A DomainModelWrite object cast from the data input.
        """
        primitive_type_mapping = {
            "int": int,
            "float": float,
            "str": str,
        }

        if cast_type in primitive_type_mapping:
            return primitive_type_mapping[cast_type](data)

        if cast_type not in self.data_model_classes:
            raise ValueError(f"Type {cast_type} is not supported")
        type_ = self.data_model_classes[cast_type]

        return type_(**data)
