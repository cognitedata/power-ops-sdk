# Mypy does not understand the pydantic classes that allows both alias and name to be used in population
# https://github.com/pydantic/pydantic/issues/3923
# mypy: disable-error-code="call-arg"
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from cognite.powerops.utils.serialization import load_yaml

logger = logging.getLogger("resync")


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
        cast_type: The string representation of the cast type.
    """

    property: str
    source_file: Optional[Path]
    extraction_path: Optional[list[str]]
    default_value: Optional[Any]
    is_subtype: bool
    is_list: bool
    cast_type: Optional[str]

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
        self.source_file = source_file
        self.extraction_path = extraction_path.split(".") if extraction_path else None
        self.default_value = default_value
        self.is_subtype = is_subtype
        self.is_list = is_list
        self.cast_type = cast_type

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
        properties: Optional[dict[str, str]] = None,
        property_configurations: Optional[list[PropertyConfiguration]] = None,
    ) -> None:
        """Initializes the DataModelConfiguration with the provided values."""
        if property_configurations is None:
            property_configurations = []
        if property_configurations is None:
            property_configurations = []
        self.name = name
        self.type_ = type_

        if properties:
            self.properties = properties
        else:
            self.properties = self._get_properties_for_type(self.type_)

        self.property_configurations = property_configurations

    @classmethod
    def from_yaml(
        cls, all_write_classes: dict[str, type], configuration_path: Optional[Path] = None
    ) -> dict[str, DataModelConfiguration]:
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
        if configuration_path:
            raw_data_model_configuration = load_yaml(configuration_path, expected_return_type="dict")
        else:
            raw_data_model_configuration = {}

        all_data_model_configurations = {}
        for name, type_ in all_write_classes.items():
            data = raw_data_model_configuration.get(name, {})
            properties = cls._get_properties_for_type(type_)
            property_configurations = PropertyConfiguration.from_dictionary(data)

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

    @staticmethod
    def _get_properties_for_type(domain_model_type: type) -> dict[str, str]:
        """Get all properties for a domain model write class.

        Given a domain model write class, this function returns a dictionary with the property name as the key and the
        type as the value.

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
