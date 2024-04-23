# Mypy does not understand the pydantic classes that allows both alias and name to be used in population
# https://github.com/pydantic/pydantic/issues/3923
# mypy: disable-error-code="call-arg"
from __future__ import annotations

import re
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Optional, get_args

from pydantic import ValidationError

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelWrite,
    GeneratorEfficiencyCurveFields,
    GeneratorEfficiencyCurveWrite,
    GeneratorFields,
    GeneratorWrite,
    PlantInformationFields,
    PlantInformationWrite,
)
from cognite.powerops.utils.serialization import load_yaml

__all__ = ["ConfigImporter"]


def get_type_prefix(domain_model_type: type) -> str:
    type_name = domain_model_type.__name__.replace("Write", "")
    external_id_prefix = re.sub(r"(?<!^)(?=[A-Z])", "_", type_name).lower()
    return external_id_prefix


def ext_id_factory(domain_model_type: type, data: dict) -> str:

    type_prefix = get_type_prefix(domain_model_type)

    if "external_id" in data:
        return data["external_id"]

    # TODO proper naming
    if domain_model_type is GeneratorEfficiencyCurveWrite:
        return f"{type_prefix}_foo"

    try:
        name = data["name"]
    except KeyError as exc:
        raise ValueError(f"Missing required `name` field for data type {type_prefix}, {data}") from exc

    cleaned_name = name.lower().replace(" ", "_").replace("-", "_")

    return f"{type_prefix}_{cleaned_name}"


@dataclass
class GeneratorConfiguration:
    source_file: str = "files/model.yaml"
    production_min: str = "model.generator.[name].p_min"
    penstock_number: str = "model.generator.[name].penstock"
    start_cost: str = "model.generator.[name].startcost.value"


@dataclass
class PlantConfiguration:
    source_file: str = "files/model.yaml"
    head_loss_factor: str = "model.plant.[name].main_loss"
    outlet_level: str = "model.plant.[name].outlet_level"
    production_max: str = "model.plant.[name].p_max"
    production_min: str = "model.plant.[name].p_min"


@dataclass
class ResyncConfiguration:

    def __init__(
        self,
        data: dict,
    ) -> None:
        self.generator = GeneratorConfiguration(**data["generator"]) if "generator" in data else None
        self.plant_information = (
            PlantConfiguration(**data["plant_information"]) if "plant_information" in data else None
        )
        self.file_path = Path(data["file_path"]) if "file_path" in data else Path("files")


class ConfigImporter:

    def __init__(
        self,
        all_type_mappings: dict[type, list[dict[str, Any]]],
        directory: Path,
        configuration: ResyncConfiguration,
    ) -> None:
        self.all_type_mappings = all_type_mappings
        self.directory = directory
        self.configuration = configuration

        DomainModelWrite.external_id_factory = ext_id_factory

    @classmethod
    def from_directory(
        cls, directory: Path, expected_types: list[Any], configuration_file: Path = Path("resync_configuration.yaml")
    ) -> ConfigImporter:

        all_types: dict[type, list[dict[str, Any]]] = {}
        for data_model_type in expected_types:
            type_prefix = get_type_prefix(data_model_type)
            data_model_type_file = directory / f"{type_prefix}.yaml"
            data_model_type_list: list[dict[str, Any]] = (
                load_yaml(data_model_type_file, expected_return_type="list") if data_model_type_file else []
            )

            all_types[data_model_type] = data_model_type_list

        configuration = ResyncConfiguration(load_yaml(directory / configuration_file, expected_return_type="dict"))

        return cls(all_types, directory, configuration)

    def config_to_fdm(self) -> list:
        fdm_objects: dict[str, Any] = {}

        for data_model_type, unprocessed_objects in self.all_type_mappings.items():
            for obj in unprocessed_objects:
                self._dict_to_type_object(fdm_objects, data_model_type, obj)

        return list(fdm_objects.values())

    def _check_strings_match_pattern(self, key_value_pairs):

        regex_pattern = r"\[SOURCE:(.+.yaml)\](.+)"

        captured_file_paths = {}
        captured_dictionary_paths = {}

        print(key_value_pairs)

        for key, value in key_value_pairs.items():
            match = re.match(regex_pattern, value)

            if match:
                captured_file_paths[key] = match.group(1)
                captured_dictionary_paths[key] = match.group(2)
            else:
                raise ValueError(f"String {value} does not match pattern {regex_pattern}")

        file_dictionaries = {}
        for file_path in set(captured_file_paths.values()):
            print(file_path)
            file_dictionaries[file_path] = load_yaml(self.directory / Path(file_path), "dict")
            print(next(iter(file_dictionaries[file_path])))

        for source_key, dictionary_path in captured_dictionary_paths.items():
            file_path = captured_file_paths[source_key]
            dictionary = file_dictionaries[file_path]
            result = self._get_value_from_dictionary(dictionary, dictionary_path)

            key_value_pairs[source_key] = result

        return key_value_pairs

    def _get_value_from_dictionary(
        self, source_data: dict, dictionary_path: str, instance_data: Optional[dict] = None
    ) -> Any:
        if instance_data is None:
            instance_data = {}
        print(dictionary_path)
        keys = dictionary_path.split(".")

        try:
            for key in keys:
                match = re.match(r"\[(\w+)\]", key)
                if key == "value":
                    source_data = next(iter(source_data.values()))
                elif match:
                    if match.group(1).isdigit():
                        print(source_data)
                        print(int(match.group(1)))
                        source_data = source_data[int(match.group(1))]
                    else:
                        source_data = source_data[instance_data[match.group(1)]]
                else:
                    source_data = source_data[key]
        except KeyError:
            return None

        print(source_data)

        return source_data

    def _extract_model_fields(self, data: dict) -> dict:
        fields_to_extract = {
            key: value for key, value in data.items() if isinstance(value, str) and value.startswith("[SOURCE")
        }

        print(fields_to_extract)

        data = self._check_strings_match_pattern(fields_to_extract) if fields_to_extract else data

        return data

    def _dict_to_type_object(self, fdm_objects: dict, domain_model_type: type, data: dict):

        extracted_data = self._extract_model_fields(data)

        type_prefix = get_type_prefix(domain_model_type)
        print(type_prefix)

        # TODO: make this more generic
        type_configuration = getattr(self.configuration, type_prefix)
        print(type_configuration)

        type_fields = [field.name for field in fields(type(type_configuration))]
        default_file = load_yaml(self.directory / type_configuration.source_file, "dict")

        if domain_model_type is GeneratorEfficiencyCurveWrite:
            domain_model_type_fields = get_args(GeneratorEfficiencyCurveFields)
        elif domain_model_type is PlantInformationWrite:
            domain_model_type_fields = get_args(PlantInformationFields)
            data["generator_efficiency_curve"] = GeneratorEfficiencyCurveWrite(
                external_id=f"{type_prefix}_foo",
                power=self._get_value_from_dictionary(default_file, "model.generator.[name].gen_eff_curve.x", data),
                efficiency=self._get_value_from_dictionary(
                    default_file, "model.generator.[name].gen_eff_curve.y", data
                ),
            )
        elif domain_model_type is GeneratorWrite:
            domain_model_type_fields = get_args(GeneratorFields)
        else:
            domain_model_type_fields = ()

        print(domain_model_type_fields)

        for field_name in domain_model_type_fields:
            print(field_name)
            if field_name in extracted_data:
                data[field_name] = extracted_data[field_name]
            elif field_name in type_fields:
                data[field_name] = self._get_value_from_dictionary(
                    default_file, getattr(type_configuration, field_name), data
                )

        try:
            fdm_object = domain_model_type(**data)
        except ValidationError as exc:
            raise ValueError(f"Missing/invalid field for {domain_model_type.__name__} in data {data}") from exc

        if fdm_object.external_id in fdm_objects:
            raise ValueError(f"{domain_model_type.__name__} with external id {fdm_object.external_id} already exists")

        fdm_objects[fdm_object.external_id] = fdm_object
