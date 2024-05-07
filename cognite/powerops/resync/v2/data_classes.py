# Mypy does not understand the pydantic classes that allows both alias and name to be used in population
# https://github.com/pydantic/pydantic/issues/3923
# mypy: disable-error-code="call-arg"
from __future__ import annotations

import inspect
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

from cognite.client.data_classes import FileMetadataWrite

import cognite.powerops.client._generated.v1.data_classes as v1_data_classes
from cognite.powerops.utils.serialization import load_yaml

logger = logging.getLogger(__name__)


# TODO: clean up remove duplicate
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


# TODO: clean up remove?
def get_data_model_write_classes() -> dict[str, type]:
    """Get all domain model write classes.

    This function returns a dictionary with the type prefix as the key and the domain model write class as the value
    for all domain model write classes.

    Returns:
        A dictionary with the type prefix as the key and the domain model write class as the value.
    """

    parent_class = v1_data_classes.DomainModelWrite
    expected_types_mapping = {}
    for name, obj in inspect.getmembers(v1_data_classes):
        if name.endswith("Write") and inspect.isclass(obj) and issubclass(obj, parent_class) and obj != parent_class:
            expected_types_mapping[get_type_prefix(obj)] = obj

    return expected_types_mapping


def get_properties_for_type(domain_model_type: type) -> dict[str, str]:
    """Get all properties for a domain model write class.

    Given a domain model write class, this function returns a dictionary with the property name as the key and the type
    as the value.

    Args:
        domain_model_type: The domain model write class to get the properties for.

    Returns:
        A dictionary with the property name as the key and the type as the value.
    """

    annotations = {"external_id": "str"}

    for domain_class in domain_model_type.__mro__:
        if domain_class.__name__.endswith("Write"):
            annotations.update(domain_class.__annotations__)

    return annotations


@dataclass
class FileUploadConfiguration:
    """Data class for file upload configuration.

    Attributes:
        folder_path: The folder path to the files to upload.
        overwrite: Whether to overwrite existing files with the same name or external_id.
        file_metadata: A dictionary of file names to FileMetadataWrite objects.
    """

    folder_path: Path
    overwrite: bool
    file_metadata: dict[str, FileMetadataWrite]

    def __init__(
        self,
        working_directory: Optional[Path] = None,
        folder_path: Optional[str] = None,
        overwrite: bool = True,
        file_metadata: Optional[list[dict[str, Any]]] = None,
    ) -> None:
        """Initializes the FileUploadConfiguration with the provided values and processes some of the values."""

        if folder_path:
            self.folder_path = Path(folder_path)
        elif working_directory:
            self.folder_path = working_directory / "files"
        else:
            raise ValueError("Either folder_path or working_directory must be provided")
        self.overwrite = overwrite

        self.file_metadata = {}
        if file_metadata:
            for metadata in file_metadata:
                if "name" not in metadata:
                    raise ValueError("File metadata must contain a name field")
                if "directory" in metadata:
                    metadata["directory"] = self._convert_string_to_unix_path(metadata["directory"])

                file_metadata_instance = FileMetadataWrite(**metadata)
                if file_metadata_instance.name:
                    self.file_metadata[file_metadata_instance.name] = file_metadata_instance
                else:
                    raise ValueError("File metadata must contain a name field")

    @staticmethod
    def _convert_string_to_unix_path(path_str: str) -> str:
        path = Path(path_str).as_posix()

        return path if path.startswith("/") else f"/{path}"


@dataclass
class PropertyConfiguration:
    """Data class for property configuration.

    Attributes:
        property: The property name.
        source_file: The source file to extract the property from.
        extraction_path: The extraction path in the source file.
        default_value: The default value if the property is not found.
        is_subtype: Whether the property is a subtype.
        is_list: Whether the property is a list.
        cast_type: The type to cast the property to.
        cast_type_str: The string representation of the cast type.
    """

    property: str
    source_file: Optional[Path]
    extraction_path: Optional[list[str]]
    default_value: Optional[Any]
    is_subtype: bool
    is_list: bool
    cast_type: Optional[type]
    cast_type_str: Optional[str]

    def __init__(
        self,
        property: str,
        source_file: Optional[Path] = None,
        extraction_path: Optional[str] = None,
        default_value: Optional[Any] = None,
        is_subtype: bool = False,
        is_list: bool = False,
        cast_type: Optional[str] = None,
    ) -> None:
        """Initializes the PropertyConfiguration with the provided values and processes some of the values."""

        self.property = property

        type_mapping = self.generate_string_type_mapping()

        if cast_type:
            if cast_type in type_mapping:
                self.cast_type = type_mapping[cast_type]
            else:
                raise ValueError(f"Invalid type string: {cast_type}")
        else:
            self.cast_type = None

        self.cast_type_str = cast_type

        self.source_file = source_file
        self.extraction_path = extraction_path.split(".") if extraction_path else None
        self.default_value = default_value
        self.is_subtype = is_subtype
        self.is_list = is_list

    @staticmethod
    def generate_string_type_mapping() -> dict[str, type]:
        """Generate a type mapping from string to type.

        This function generates a dictionary mapping string type names to type objects. The mapping includes the basic
        types int, float, and str, as well as all domain model write classes.

        Returns:
            A dictionary mapping string type names to type objects.
        """
        type_mapping = {
            "int": int,
            "float": float,
            "str": str,
        }

        type_mapping.update(get_data_model_write_classes())

        return type_mapping

    @classmethod
    def from_dictionary(cls, property_configurations: dict[str, Any]) -> list[PropertyConfiguration]:
        """Generate a list of PropertyConfiguration objects from a dictionary.

        Given a dictionary of property configurations, this function generates a list of PropertyConfiguration objects.

        Args:
            property_configurations: A dictionary of property configurations.

        Returns:
            A list of PropertyConfiguration objects.
        """

        parsed_property_configurations = []
        for property, configuration in property_configurations.items():
            parsed_property_configurations.append(cls(property=property, **configuration))
        return parsed_property_configurations

    @classmethod
    def from_string(
        cls, property: str, value: str, regex_pattern: str = r"^\[SOURCE:(.+.yaml)\](.+)?"
    ) -> PropertyConfiguration:
        """Generate a PropertyConfiguration object from a string matching a regex pattern.

        Given a property name and a string value, this function generates a PropertyConfiguration object if the string
        matches the regex pattern.

        Args:
            property: The property name.
            value: The string value to parse.
            regex_pattern: The regex pattern to match the string against.

        Returns:
            A PropertyConfiguration object.

        Raises:
            ValueError: If the string does not match the regex pattern.
        """

        match = re.match(regex_pattern, value)

        if match:
            source_file_path = Path(match.group(1))
            extraction_path = match.group(2)

            # TODO: implement default value
            default_value = None
            is_list = False
            cast_type = "str"
            is_subtype = False

            return cls(property, source_file_path, extraction_path, default_value, is_subtype, is_list, cast_type)
        else:
            raise ValueError(f"String {value} does not match pattern {regex_pattern}")

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> list[PropertyConfiguration]:
        """Generate a list of PropertyConfiguration objects from a dictionary using a regex pattern.

        Given a dictionary of property configurations, this function generates a list of PropertyConfiguration objects
        for all properties that match the regex pattern.

        Args:
            data: A dictionary of property configurations.

        Returns:
            A list of PropertyConfiguration objects.
        """

        extraction_prefix = "[SOURCE"
        regex_pattern = r"\[SOURCE:(.+.yaml)\](.+)"

        fields_to_extract = []
        for property, value in data.items():
            if isinstance(value, str) and value.startswith(extraction_prefix):
                fields_to_extract.append(cls.from_string(property, value, regex_pattern))

        return fields_to_extract

    @staticmethod
    def union_lists(
        default_property_configurations: list[PropertyConfiguration],
        override_property_configurations: list[PropertyConfiguration],
    ) -> list[PropertyConfiguration]:
        """Combine two lists of PropertyConfiguration objects.

        Given two lists of PropertyConfiguration objects, this function combines the lists and removes duplicates. The
        default_property_configurations list is used as the base list, and the override_property_configurations list is
        used to override any configurations in the base list.

        Args:
            default_property_configurations: The base list of PropertyConfiguration objects.
            override_property_configurations: The list of PropertyConfiguration objects to override the base list.

        Returns:
            A list of PropertyConfiguration objects with no duplicates.
        """

        configurations_dict = {config.property: config for config in default_property_configurations}
        configurations_dict.update({config.property: config for config in override_property_configurations})

        return list(configurations_dict.values())


@dataclass
class DataModelConfiguration:
    """Data class for data model configuration.

    Attributes:
        name: The name of the data model type.
        type_: The type of the data model.
        properties: A dictionary of properties for the data model type.
        property_configurations: A list of PropertyConfiguration objects detailing the configuration of the properties.
    """

    name: str
    type_: type
    properties: dict[str, str]
    property_configurations: list[PropertyConfiguration]

    def __init__(
        self,
        name: str,
        type_: type,
        properties: dict[str, str],
        property_configurations: Optional[list[PropertyConfiguration]] = None,
    ) -> None:
        """Initializes the DataModelConfiguration with the provided values."""
        if property_configurations is None:
            property_configurations = []
        self.name = name
        self.type_ = type_
        self.properties = properties
        self.property_configurations = property_configurations

    @classmethod
    def from_yaml(cls, configuration_path: Path) -> dict[str, DataModelConfiguration]:
        """Generate a dictionary of DataModelConfiguration objects from a YAML file.

        Given a path to a YAML file containing data model configurations, this function generates a dictionary of
        DataModelConfiguration objects.

        Args:
            configuration_path: The path to the YAML file containing data model configurations.

        Returns:
            A dictionary of DataModelConfiguration objects.

        Raises:
            ValueError: If a property configuration is not a valid property for the domain model type.
        """
        raw_data_model_configuration = load_yaml(configuration_path, expected_return_type="dict")

        all_write_classes = get_data_model_write_classes()

        all_data_model_configurations = {}
        for name, type_ in all_write_classes.items():
            properties = get_properties_for_type(type_)
            property_configurations = PropertyConfiguration.from_dictionary(raw_data_model_configuration.get(name, {}))

            # check if all property configurations are valid properties
            for property_configuration in property_configurations:
                if property_configuration.property not in properties:
                    # TODO: make logging??
                    raise ValueError(
                        f"Property {property_configuration.property} not in domain model properties for {name}"
                    )

            all_data_model_configurations[name] = cls(
                type_=type_, name=name, properties=properties, property_configurations=property_configurations
            )

        return all_data_model_configurations


@dataclass
class ResyncConfiguration:
    """Data class for resync configuration.

    Attributes:
        data_model_configuration_file: The path to the data model configuration file.
        working_directory: The working directory for the resync importer.
        logging_level: The logging level for the resync importer.
        overwrite_data: Whether to overwrite existing data in CDF.
        data_set_id: The data set id to use for the resync importer.
        all_write_classes: A dictionary of all domain model write classes.
        file_configuration: A FileUploadConfiguration object detailing the file upload configuration.
    """

    data_model_configuration_file: Path
    working_directory: Path
    logging_level: str
    overwrite_data: bool
    data_set_id: int
    all_write_classes: dict[str, type]
    file_configuration: Optional[FileUploadConfiguration]

    def __init__(
        self,
        data_model_configuration_file: str,
        working_directory: str,
        data_set_id: int,
        logging_level: str = "INFO",
        overwrite_data: bool = True,
        file_configuration: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initializes the ResyncConfiguration with the provided values and converts some of them."""

        self.data_model_configuration_file = Path(data_model_configuration_file)
        self.working_directory = Path(working_directory)
        self.file_configuration = (
            FileUploadConfiguration(working_directory=self.working_directory, **file_configuration)
            if file_configuration
            else None
        )
        self.all_write_classes = get_data_model_write_classes()

        self.logging_level = logging_level
        self.overwrite_data = overwrite_data
        self.data_set_id = data_set_id

    @classmethod
    def from_yaml(cls, configuration_path: Path) -> ResyncConfiguration:
        """Generate a ResyncConfiguration object from a YAML file.

        Given a path to a YAML file containing resync configurations, this function generates a ResyncConfiguration
        object.

        Args:
            configuration_path: The path to the YAML file containing resync configurations.

        Returns:
            A ResyncConfiguration object.
        """

        configuration = load_yaml(configuration_path, expected_return_type="dict")

        return cls(**configuration)
