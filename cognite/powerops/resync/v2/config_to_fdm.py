# Mypy does not understand the pydantic classes that allows both alias and name to be used in population
# https://github.com/pydantic/pydantic/issues/3923
# mypy: disable-error-code="call-arg"
from __future__ import annotations

import re
from math import floor, log10
from pathlib import Path
from typing import Any, Optional
from pydantic import ValidationError

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelWrite,
    PriceAreaWrite,
    PriceScenarioWrite,
)
from cognite.powerops.utils.serialization import load_yaml

__all__ = ["ConfigImporter"]


def ext_id_factory(domain_model_type: type, data: dict) -> str:

    type_name = domain_model_type.__name__.replace("Write", "")

    # TODO: check if it's one of the expected data types
    try:
        name = data["name"]
    except KeyError as exc:
        raise ValueError(
            f"Missing required `name` field for data type {type_name}, {data}"
        ) from exc

    external_id_prefix = re.sub(r'(?<!^)(?=[A-Z])', '_', type_name).lower()

    return f"{external_id_prefix}_{name}"


class ConfigImporter:

    def __init__(self,
        # price_areas: list
    ) -> None:
        self.price_scenarios = [{"name": "foo", "timeseries": "bar"}, {"name": "bar", "timeseries": "bar"}]
        self.price_area = [{"name": "bar", "timezone": "europe"}]

        DomainModelWrite.external_id_factory = ext_id_factory

        pass


    # TODO: use file name to
    # @classmethod
    # def from_directory(cls, directory: Path) -> ConfigImporter:
    #     config_files = list(directory.glob(f"**/**.yaml"))

    #     return cls(config_files)


    def config_to_fdm(self) -> list:
        fdm_objects: dict[str, Any] = {}

        for price_scenario in self.price_scenarios:
            self._dict_to_type_object(fdm_objects, PriceScenarioWrite, price_scenario)

        for price_area in self.price_area:
            self._dict_to_type_object(fdm_objects, PriceAreaWrite, price_area)

        return list(fdm_objects.values())


    def _dict_to_type_object(self, fdm_objects: dict, domain_model_type: type, data: dict):

        try:
            fdm_object = domain_model_type(**data)
        except ValidationError as exc:
            raise ValueError(f"Missing required field for {domain_model_type.__name__} in data {data}")

        if fdm_object.external_id in fdm_objects:
            raise ValueError(f"{domain_model_type.__name__} with external id {fdm_object.external_id} already exists")

        fdm_objects[fdm_object.external_id] = fdm_object
