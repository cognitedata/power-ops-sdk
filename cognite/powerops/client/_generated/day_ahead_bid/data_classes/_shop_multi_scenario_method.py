from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
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
)
from ._bid_method import BidMethod, BidMethodApply

if TYPE_CHECKING:
    from ._shop_price_scenario import SHOPPriceScenario, SHOPPriceScenarioApply


__all__ = [
    "SHOPMultiScenarioMethod",
    "SHOPMultiScenarioMethodApply",
    "SHOPMultiScenarioMethodList",
    "SHOPMultiScenarioMethodApplyList",
    "SHOPMultiScenarioMethodFields",
    "SHOPMultiScenarioMethodTextFields",
]


SHOPMultiScenarioMethodTextFields = Literal["name", "shop_cases"]
SHOPMultiScenarioMethodFields = Literal["name", "shop_cases"]

_SHOPMULTISCENARIOMETHOD_PROPERTIES_BY_FIELD = {
    "name": "name",
    "shop_cases": "shopCases",
}


class SHOPMultiScenarioMethod(BidMethod):
    """This represents the reading version of shop multi scenario method.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop multi scenario method.
        data_record: The data record of the shop multi scenario method node.
        name: Name for the BidMethod
        shop_cases: The shop case field.
        price_scenarios: An array of scenarios for this bid method.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "DayAheadSHOPMultiScenarioMethod"
    )
    shop_cases: Optional[list[str]] = Field(None, alias="shopCases")
    price_scenarios: Union[list[SHOPPriceScenario], list[str], None] = Field(
        default=None, repr=False, alias="priceScenarios"
    )

    def as_apply(self) -> SHOPMultiScenarioMethodApply:
        """Convert this read version of shop multi scenario method to the writing version."""
        return SHOPMultiScenarioMethodApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            shop_cases=self.shop_cases,
            price_scenarios=[
                price_scenario.as_apply() if isinstance(price_scenario, DomainModel) else price_scenario
                for price_scenario in self.price_scenarios or []
            ],
        )


class SHOPMultiScenarioMethodApply(BidMethodApply):
    """This represents the writing version of shop multi scenario method.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop multi scenario method.
        data_record: The data record of the shop multi scenario method node.
        name: Name for the BidMethod
        shop_cases: The shop case field.
        price_scenarios: An array of scenarios for this bid method.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "DayAheadSHOPMultiScenarioMethod"
    )
    shop_cases: Optional[list[str]] = Field(None, alias="shopCases")
    price_scenarios: Union[list[SHOPPriceScenarioApply], list[str], None] = Field(
        default=None, repr=False, alias="priceScenarios"
    )

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
            SHOPMultiScenarioMethod, dm.ViewId("power-ops-day-ahead-bid", "SHOPMultiScenarioMethod", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.shop_cases is not None or write_none:
            properties["shopCases"] = self.shop_cases

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

        edge_type = dm.DirectRelationReference("power-ops-types", "PriceScenario")
        for price_scenario in self.price_scenarios or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=price_scenario,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
            )
            resources.extend(other_resources)

        return resources


class SHOPMultiScenarioMethodList(DomainModelList[SHOPMultiScenarioMethod]):
    """List of shop multi scenario methods in the read version."""

    _INSTANCE = SHOPMultiScenarioMethod

    def as_apply(self) -> SHOPMultiScenarioMethodApplyList:
        """Convert these read versions of shop multi scenario method to the writing versions."""
        return SHOPMultiScenarioMethodApplyList([node.as_apply() for node in self.data])


class SHOPMultiScenarioMethodApplyList(DomainModelApplyList[SHOPMultiScenarioMethodApply]):
    """List of shop multi scenario methods in the writing version."""

    _INSTANCE = SHOPMultiScenarioMethodApply


def _create_shop_multi_scenario_method_filter(
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
