from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "ProductionPricePair",
    "ProductionPricePairApply",
    "ProductionPricePairList",
    "ProductionPricePairApplyList",
    "ProductionPricePairFields",
    "ProductionPricePairTextFields",
]


ProductionPricePairTextFields = Literal["production", "price"]
ProductionPricePairFields = Literal["production", "price"]

_PRODUCTIONPRICEPAIR_PROPERTIES_BY_FIELD = {
    "production": "production",
    "price": "price",
}


class ProductionPricePair(DomainModel):
    """This represent a read version of production price pair.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the production price pair.
        production: The production field.
        price: The price field.
        created_time: The created time of the production price pair node.
        last_updated_time: The last updated time of the production price pair node.
        deleted_time: If present, the deleted time of the production price pair node.
        version: The version of the production price pair node.
    """

    space: str = "dayAheadFrontendContractModel"
    production: Optional[str] = None
    price: Optional[str] = None

    def as_apply(self) -> ProductionPricePairApply:
        """Convert this read version of production price pair to a write version."""
        return ProductionPricePairApply(
            space=self.space,
            external_id=self.external_id,
            production=self.production,
            price=self.price,
        )


class ProductionPricePairApply(DomainModelApply):
    """This represent a write version of production price pair.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the production price pair.
        production: The production field.
        price: The price field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "dayAheadFrontendContractModel"
    production: Optional[str] = None
    price: Optional[str] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.production is not None:
            properties["production"] = self.production
        if self.price is not None:
            properties["price"] = self.price
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("dayAheadFrontendContractModel", "ProductionPricePair", "1"),
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


class ProductionPricePairList(TypeList[ProductionPricePair]):
    """List of production price pairs in read version."""

    _NODE = ProductionPricePair

    def as_apply(self) -> ProductionPricePairApplyList:
        """Convert this read version of production price pair to a write version."""
        return ProductionPricePairApplyList([node.as_apply() for node in self.data])


class ProductionPricePairApplyList(TypeApplyList[ProductionPricePairApply]):
    """List of production price pairs in write version."""

    _NODE = ProductionPricePairApply
