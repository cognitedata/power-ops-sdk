from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

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

if TYPE_CHECKING:
    from ._shop_price_scenario import SHOPPriceScenario, SHOPPriceScenarioApply


__all__ = [
    "SHOPPriceScenarioResult",
    "SHOPPriceScenarioResultApply",
    "SHOPPriceScenarioResultList",
    "SHOPPriceScenarioResultApplyList",
    "SHOPPriceScenarioResultFields",
    "SHOPPriceScenarioResultTextFields",
]


SHOPPriceScenarioResultTextFields = Literal["price", "production"]
SHOPPriceScenarioResultFields = Literal["price", "production"]

_SHOPPRICESCENARIORESULT_PROPERTIES_BY_FIELD = {
    "price": "price",
    "production": "production",
}


class SHOPPriceScenarioResult(DomainModel):
    """This represents the reading version of shop price scenario result.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop price scenario result.
        data_record: The data record of the shop price scenario result node.
        price: The price field.
        production: The production field.
        price_scenario: The price scenario field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "SHOPPriceScenarioResult"
    )
    price: Union[TimeSeries, str, None] = None
    production: Union[TimeSeries, str, None] = None
    price_scenario: Union[SHOPPriceScenario, str, dm.NodeId, None] = Field(None, repr=False, alias="priceScenario")

    def as_apply(self) -> SHOPPriceScenarioResultApply:
        """Convert this read version of shop price scenario result to the writing version."""
        return SHOPPriceScenarioResultApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            price=self.price,
            production=self.production,
            price_scenario=self.price_scenario.as_apply()
            if isinstance(self.price_scenario, DomainModel)
            else self.price_scenario,
        )


class SHOPPriceScenarioResultApply(DomainModelApply):
    """This represents the writing version of shop price scenario result.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop price scenario result.
        data_record: The data record of the shop price scenario result node.
        price: The price field.
        production: The production field.
        price_scenario: The price scenario field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "SHOPPriceScenarioResult"
    )
    price: Union[TimeSeries, str, None] = None
    production: Union[TimeSeries, str, None] = None
    price_scenario: Union[SHOPPriceScenarioApply, str, dm.NodeId, None] = Field(None, repr=False, alias="priceScenario")

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
            SHOPPriceScenarioResult, dm.ViewId("power-ops-day-ahead-bid", "SHOPPriceScenarioResult", "1")
        )

        properties: dict[str, Any] = {}

        if self.price is not None or write_none:
            if isinstance(self.price, str) or self.price is None:
                properties["price"] = self.price
            else:
                properties["price"] = self.price.external_id

        if self.production is not None or write_none:
            if isinstance(self.production, str) or self.production is None:
                properties["production"] = self.production
            else:
                properties["production"] = self.production.external_id

        if self.price_scenario is not None:
            properties["priceScenario"] = {
                "space": self.space if isinstance(self.price_scenario, str) else self.price_scenario.space,
                "externalId": self.price_scenario
                if isinstance(self.price_scenario, str)
                else self.price_scenario.external_id,
            }

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

        if isinstance(self.price_scenario, DomainModelApply):
            other_resources = self.price_scenario._to_instances_apply(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.price, CogniteTimeSeries):
            resources.time_series.append(self.price)

        if isinstance(self.production, CogniteTimeSeries):
            resources.time_series.append(self.production)

        return resources


class SHOPPriceScenarioResultList(DomainModelList[SHOPPriceScenarioResult]):
    """List of shop price scenario results in the read version."""

    _INSTANCE = SHOPPriceScenarioResult

    def as_apply(self) -> SHOPPriceScenarioResultApplyList:
        """Convert these read versions of shop price scenario result to the writing versions."""
        return SHOPPriceScenarioResultApplyList([node.as_apply() for node in self.data])


class SHOPPriceScenarioResultApplyList(DomainModelApplyList[SHOPPriceScenarioResultApply]):
    """List of shop price scenario results in the writing version."""

    _INSTANCE = SHOPPriceScenarioResultApply


def _create_shop_price_scenario_result_filter(
    view_id: dm.ViewId,
    price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if price_scenario and isinstance(price_scenario, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("priceScenario"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": price_scenario},
            )
        )
    if price_scenario and isinstance(price_scenario, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("priceScenario"),
                value={"space": price_scenario[0], "externalId": price_scenario[1]},
            )
        )
    if price_scenario and isinstance(price_scenario, list) and isinstance(price_scenario[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("priceScenario"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in price_scenario],
            )
        )
    if price_scenario and isinstance(price_scenario, list) and isinstance(price_scenario[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("priceScenario"),
                values=[{"space": item[0], "externalId": item[1]} for item in price_scenario],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
