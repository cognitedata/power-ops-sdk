from __future__ import annotations

from typing import Literal, Optional, Union  # noqa: F401

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)


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
    """This represents the reading version of production price pair.

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

    space: str = DEFAULT_INSTANCE_SPACE
    production: Union[TimeSeries, str, None] = None
    price: Union[TimeSeries, str, None] = None

    def as_apply(self) -> ProductionPricePairApply:
        """Convert this read version of production price pair to the writing version."""
        return ProductionPricePairApply(
            space=self.space,
            external_id=self.external_id,
            production=self.production,
            price=self.price,
        )


class ProductionPricePairApply(DomainModelApply):
    """This represents the writing version of production price pair.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the production price pair.
        production: The production field.
        price: The price field.
        existing_version: Fail the ingestion request if the production price pair version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    production: Union[TimeSeries, str, None] = None
    price: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-day-ahead-bid", "ProductionPricePair", "1"
        )

        properties = {}
        if self.production is not None:
            properties["production"] = (
                self.production if isinstance(self.production, str) else self.production.external_id
            )
        if self.price is not None:
            properties["price"] = self.price if isinstance(self.price, str) else self.price.external_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.production, CogniteTimeSeries):
            resources.time_series.append(self.production)

        if isinstance(self.price, CogniteTimeSeries):
            resources.time_series.append(self.price)

        return resources


class ProductionPricePairList(DomainModelList[ProductionPricePair]):
    """List of production price pairs in the read version."""

    _INSTANCE = ProductionPricePair

    def as_apply(self) -> ProductionPricePairApplyList:
        """Convert these read versions of production price pair to the writing versions."""
        return ProductionPricePairApplyList([node.as_apply() for node in self.data])


class ProductionPricePairApplyList(DomainModelApplyList[ProductionPricePairApply]):
    """List of production price pairs in the writing version."""

    _INSTANCE = ProductionPricePairApply


def _create_production_price_pair_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
