from __future__ import annotations

from typing import Generic, TypeVar

from cognite.dm_clients.domain_modeling import DomainModel, DomainModelAPI

from cognite.powerops.client.dm.client import PowerOpsDmClient
from cognite.powerops.client.dm.schema import Case, CommandsConfig, Mapping, Scenario, Transformation

DomainModelT = TypeVar("DomainModelT", bound=DomainModel)


class DMAPIBase(Generic[DomainModelT]):
    model_class: DomainModelT
    dm_attr: str

    def __init__(self, dm_client: PowerOpsDmClient):
        self.dm_client = dm_client

    @property
    def model_api(self) -> DomainModelAPI[DomainModelT]:
        return getattr(self.dm_client, self.dm_attr)

    # CRUD

    def retrieve(self, external_id: str) -> DomainModelT:
        items = self.model_api.retrieve([external_id])
        if len(items) < 1:
            raise ValueError(f"{self.model_class.__name__} with externalId {external_id}" f" not found.")
        if len(items) > 1:
            # this should never happen
            raise ValueError(f"Multiple {self.model_class.__name__} items with externalId" f" {external_id} found.")
        return items[0]

    def update(self, transformation: DomainModelT) -> DomainModelT:
        self.model_api.apply([transformation])
        return transformation

    def delete(self, transformation: DomainModelT) -> None:
        self.model_api.delete([transformation])


class CaseAPI(DMAPIBase[Case]):
    model_class = Case
    dm_attr = "case"


class CommandsAPI(DMAPIBase[CommandsConfig]):
    model_class = CommandsConfig
    dm_attr = "commands"


class MappingAPI(DMAPIBase[Mapping]):
    model_class = Mapping
    dm_attr = "mapping"


class ScenarioAPI(DMAPIBase[Scenario]):
    model_class = Scenario
    dm_attr = "scenario"


class TransformationAPI(DMAPIBase[Transformation]):
    model_class = Transformation
    dm_attr = "transformation"
