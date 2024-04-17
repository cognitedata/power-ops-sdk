# Mypy does not understand the pydantic classes that allows both alias and name to be used in population
# https://github.com/pydantic/pydantic/issues/3923
# mypy: disable-error-code="call-arg"
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelWrite,
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

    try:
        name = data["name"]
    except KeyError as exc:
        raise ValueError(f"Missing required `name` field for data type {type_prefix}, {data}") from exc

    cleaned_name = name.lower().replace(" ", "_").replace("-", "_")

    return f"{type_prefix}_{cleaned_name}"


class ConfigImporter:

    def __init__(
        self,
        all_type_mappings: dict[type, list[dict[str, Any]]],
    ) -> None:
        self.all_type_mappings = all_type_mappings

        DomainModelWrite.external_id_factory = ext_id_factory

    @classmethod
    def from_directory(cls, directory: Path, expected_types: list[Any]) -> ConfigImporter:

        all_types: dict[type, list[dict[str, Any]]] = {}
        for data_model_type in expected_types:
            type_prefix = get_type_prefix(data_model_type)
            data_model_type_file = directory / f"{type_prefix}.yaml"
            data_model_type_list: list[dict[str, Any]] = (
                load_yaml(data_model_type_file, expected_return_type="list") if data_model_type_file else []
            )

            all_types[data_model_type] = data_model_type_list

        return cls(all_types)

    def config_to_fdm(self) -> list:
        fdm_objects: dict[str, Any] = {}

        for data_model_type, unprocessed_objects in self.all_type_mappings.items():
            for obj in unprocessed_objects:
                self._dict_to_type_object(fdm_objects, data_model_type, obj)

        return list(fdm_objects.values())

    def _dict_to_type_object(self, fdm_objects: dict, domain_model_type: type, data: dict):

        try:
            fdm_object = domain_model_type(**data)
        except ValidationError as exc:
            raise ValueError(f"Missing/invalid field for {domain_model_type.__name__} in data {data}") from exc

        if fdm_object.external_id in fdm_objects:
            raise ValueError(f"{domain_model_type.__name__} with external id {fdm_object.external_id} already exists")

        fdm_objects[fdm_object.external_id] = fdm_object
