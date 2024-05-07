from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)
from ._bid_method import BidMethod, BidMethodWrite

if TYPE_CHECKING:
    from ._shop_price_scenario import SHOPPriceScenario, SHOPPriceScenarioGraphQL, SHOPPriceScenarioWrite


__all__ = [
    "SHOPMultiScenarioMethod",
    "SHOPMultiScenarioMethodWrite",
    "SHOPMultiScenarioMethodApply",
    "SHOPMultiScenarioMethodList",
    "SHOPMultiScenarioMethodWriteList",
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


class SHOPMultiScenarioMethodGraphQL(GraphQLCore):
    """This represents the reading version of shop multi scenario method, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop multi scenario method.
        data_record: The data record of the shop multi scenario method node.
        name: Name for the BidMethod
        shop_cases: The shop case field.
        price_scenarios: An array of scenarios for this bid method.
    """

    view_id = dm.ViewId("power-ops-day-ahead-bid", "SHOPMultiScenarioMethod", "1")
    name: Optional[str] = None
    shop_cases: Optional[list[str]] = Field(None, alias="shopCases")
    price_scenarios: Optional[list[SHOPPriceScenarioGraphQL]] = Field(default=None, repr=False, alias="priceScenarios")

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    @field_validator("price_scenarios", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> SHOPMultiScenarioMethod:
        """Convert this GraphQL format of shop multi scenario method to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return SHOPMultiScenarioMethod(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            shop_cases=self.shop_cases,
            price_scenarios=[
                price_scenario.as_read() if isinstance(price_scenario, GraphQLCore) else price_scenario
                for price_scenario in self.price_scenarios or []
            ],
        )

    def as_write(self) -> SHOPMultiScenarioMethodWrite:
        """Convert this GraphQL format of shop multi scenario method to the writing format."""
        return SHOPMultiScenarioMethodWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            shop_cases=self.shop_cases,
            price_scenarios=[
                price_scenario.as_write() if isinstance(price_scenario, DomainModel) else price_scenario
                for price_scenario in self.price_scenarios or []
            ],
        )


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
    price_scenarios: Union[list[SHOPPriceScenario], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="priceScenarios"
    )

    def as_write(self) -> SHOPMultiScenarioMethodWrite:
        """Convert this read version of shop multi scenario method to the writing version."""
        return SHOPMultiScenarioMethodWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            shop_cases=self.shop_cases,
            price_scenarios=[
                price_scenario.as_write() if isinstance(price_scenario, DomainModel) else price_scenario
                for price_scenario in self.price_scenarios or []
            ],
        )

    def as_apply(self) -> SHOPMultiScenarioMethodWrite:
        """Convert this read version of shop multi scenario method to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPMultiScenarioMethodWrite(BidMethodWrite):
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
    price_scenarios: Union[list[SHOPPriceScenarioWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="priceScenarios"
    )

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
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
                existing_version=None if allow_version_increase else self.data_record.existing_version,
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
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=price_scenario,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class SHOPMultiScenarioMethodApply(SHOPMultiScenarioMethodWrite):
    def __new__(cls, *args, **kwargs) -> SHOPMultiScenarioMethodApply:
        warnings.warn(
            "SHOPMultiScenarioMethodApply is deprecated and will be removed in v1.0. Use SHOPMultiScenarioMethodWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "SHOPMultiScenarioMethod.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class SHOPMultiScenarioMethodList(DomainModelList[SHOPMultiScenarioMethod]):
    """List of shop multi scenario methods in the read version."""

    _INSTANCE = SHOPMultiScenarioMethod

    def as_write(self) -> SHOPMultiScenarioMethodWriteList:
        """Convert these read versions of shop multi scenario method to the writing versions."""
        return SHOPMultiScenarioMethodWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> SHOPMultiScenarioMethodWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPMultiScenarioMethodWriteList(DomainModelWriteList[SHOPMultiScenarioMethodWrite]):
    """List of shop multi scenario methods in the writing version."""

    _INSTANCE = SHOPMultiScenarioMethodWrite


class SHOPMultiScenarioMethodApplyList(SHOPMultiScenarioMethodWriteList): ...


def _create_shop_multi_scenario_method_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
