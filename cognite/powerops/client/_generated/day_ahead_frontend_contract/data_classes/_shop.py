from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = ["SHOP", "SHOPApply", "SHOPList", "SHOPApplyList", "SHOPFields", "SHOPTextFields"]


SHOPTextFields = Literal["name", "shop_cases", "price_scenarios"]
SHOPFields = Literal["name", "shop_cases", "price_scenarios"]

_SHOP_PROPERTIES_BY_FIELD = {
    "name": "name",
    "shop_cases": "shopCases",
    "price_scenarios": "priceScenarios",
}


class SHOP(DomainModel):
    """This represent a read version of shop.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop.
        name: The name field.
        shop_cases: The shop case field.
        price_scenarios: The price scenario field.
        created_time: The created time of the shop node.
        last_updated_time: The last updated time of the shop node.
        deleted_time: If present, the deleted time of the shop node.
        version: The version of the shop node.
    """

    space: str = "dayAheadFrontendContractModel"
    name: Optional[str] = None
    shop_cases: Optional[list[str]] = Field(None, alias="shopCases")
    price_scenarios: Optional[str] = Field(None, alias="priceScenarios")

    def as_apply(self) -> SHOPApply:
        """Convert this read version of shop to a write version."""
        return SHOPApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            shop_cases=self.shop_cases,
            price_scenarios=self.price_scenarios,
        )


class SHOPApply(DomainModelApply):
    """This represent a write version of shop.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop.
        name: The name field.
        shop_cases: The shop case field.
        price_scenarios: The price scenario field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "dayAheadFrontendContractModel"
    name: str
    shop_cases: Optional[list[str]] = Field(None, alias="shopCases")
    price_scenarios: Optional[str] = Field(None, alias="priceScenarios")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.shop_cases is not None:
            properties["shopCases"] = self.shop_cases
        if self.price_scenarios is not None:
            properties["priceScenarios"] = self.price_scenarios
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("dayAheadFrontendContractModel", "SHOP", "1"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class SHOPList(TypeList[SHOP]):
    """List of shops in read version."""

    _NODE = SHOP

    def as_apply(self) -> SHOPApplyList:
        """Convert this read version of shop to a write version."""
        return SHOPApplyList([node.as_apply() for node in self.data])


class SHOPApplyList(TypeApplyList[SHOPApply]):
    """List of shops in write version."""

    _NODE = SHOPApply
