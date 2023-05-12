from typing import Generic, TypeVar

from dm_clients.domain_modeling import DomainModel, DomainModelAPI

from cognite.powerops.client.dm.client import PowerOpsDmClient

DomainModelT = TypeVar("DomainModelT", bound=DomainModel)


class DMClientBase(Generic[DomainModelT]):
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
