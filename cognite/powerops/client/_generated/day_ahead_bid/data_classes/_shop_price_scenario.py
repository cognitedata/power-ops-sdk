from __future__ import annotations

from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)


__all__ = [
    "SHOPPriceScenario",
    "SHOPPriceScenarioApply",
    "SHOPPriceScenarioList",
    "SHOPPriceScenarioApplyList",
    "SHOPPriceScenarioFields",
    "SHOPPriceScenarioTextFields",
]


SHOPPriceScenarioTextFields = Literal["name", "price"]
SHOPPriceScenarioFields = Literal["name", "price"]

_SHOPPRICESCENARIO_PROPERTIES_BY_FIELD = {
    "name": "name",
    "price": "price",
}


class SHOPPriceScenario(DomainModel):
    """This represents the reading version of shop price scenario.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop price scenario.
        data_record: The data record of the shop price scenario node.
        name: Name for the BidMethod
        price: The price field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    price: Union[TimeSeries, str, None] = None

    def as_apply(self) -> SHOPPriceScenarioApply:
        """Convert this read version of shop price scenario to the writing version."""
        return SHOPPriceScenarioApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            price=self.price,
        )


class SHOPPriceScenarioApply(DomainModelApply):
    """This represents the writing version of shop price scenario.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop price scenario.
        data_record: The data record of the shop price scenario node.
        name: Name for the BidMethod
        price: The price field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    price: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            SHOPPriceScenario, dm.ViewId("power-ops-day-ahead-bid", "SHOPPriceScenario", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.price is not None or write_none:
            if isinstance(self.price, str) or self.price is None:
                properties["price"] = self.price
            else:
                properties["price"] = self.price.external_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.price, CogniteTimeSeries):
            resources.time_series.append(self.price)

        return resources


class SHOPPriceScenarioList(DomainModelList[SHOPPriceScenario]):
    """List of shop price scenarios in the read version."""

    _INSTANCE = SHOPPriceScenario

    def as_apply(self) -> SHOPPriceScenarioApplyList:
        """Convert these read versions of shop price scenario to the writing versions."""
        return SHOPPriceScenarioApplyList([node.as_apply() for node in self.data])


class SHOPPriceScenarioApplyList(DomainModelApplyList[SHOPPriceScenarioApply]):
    """List of shop price scenarios in the writing version."""

    _INSTANCE = SHOPPriceScenarioApply


def _create_shop_price_scenario_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
