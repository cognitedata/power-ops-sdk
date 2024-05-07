from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
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
    TimeSeries,
)

if TYPE_CHECKING:
    from ._shop_price_scenario import SHOPPriceScenario, SHOPPriceScenarioGraphQL, SHOPPriceScenarioWrite


__all__ = [
    "SHOPPriceScenarioResult",
    "SHOPPriceScenarioResultWrite",
    "SHOPPriceScenarioResultApply",
    "SHOPPriceScenarioResultList",
    "SHOPPriceScenarioResultWriteList",
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


class SHOPPriceScenarioResultGraphQL(GraphQLCore):
    """This represents the reading version of shop price scenario result, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop price scenario result.
        data_record: The data record of the shop price scenario result node.
        price: The price field.
        production: The production field.
        price_scenario: The price scenario field.
    """

    view_id = dm.ViewId("power-ops-day-ahead-bid", "SHOPPriceScenarioResult", "1")
    price: Union[TimeSeries, dict, None] = None
    production: Union[TimeSeries, dict, None] = None
    price_scenario: Optional[SHOPPriceScenarioGraphQL] = Field(None, repr=False, alias="priceScenario")

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

    @field_validator("price_scenario", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> SHOPPriceScenarioResult:
        """Convert this GraphQL format of shop price scenario result to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return SHOPPriceScenarioResult(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            price=self.price,
            production=self.production,
            price_scenario=(
                self.price_scenario.as_read() if isinstance(self.price_scenario, GraphQLCore) else self.price_scenario
            ),
        )

    def as_write(self) -> SHOPPriceScenarioResultWrite:
        """Convert this GraphQL format of shop price scenario result to the writing format."""
        return SHOPPriceScenarioResultWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            price=self.price,
            production=self.production,
            price_scenario=(
                self.price_scenario.as_write() if isinstance(self.price_scenario, DomainModel) else self.price_scenario
            ),
        )


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

    def as_write(self) -> SHOPPriceScenarioResultWrite:
        """Convert this read version of shop price scenario result to the writing version."""
        return SHOPPriceScenarioResultWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            price=self.price,
            production=self.production,
            price_scenario=(
                self.price_scenario.as_write() if isinstance(self.price_scenario, DomainModel) else self.price_scenario
            ),
        )

    def as_apply(self) -> SHOPPriceScenarioResultWrite:
        """Convert this read version of shop price scenario result to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPPriceScenarioResultWrite(DomainModelWrite):
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
    price_scenario: Union[SHOPPriceScenarioWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="priceScenario")

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
                "externalId": (
                    self.price_scenario if isinstance(self.price_scenario, str) else self.price_scenario.external_id
                ),
            }

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

        if isinstance(self.price_scenario, DomainModelWrite):
            other_resources = self.price_scenario._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.price, CogniteTimeSeries):
            resources.time_series.append(self.price)

        if isinstance(self.production, CogniteTimeSeries):
            resources.time_series.append(self.production)

        return resources


class SHOPPriceScenarioResultApply(SHOPPriceScenarioResultWrite):
    def __new__(cls, *args, **kwargs) -> SHOPPriceScenarioResultApply:
        warnings.warn(
            "SHOPPriceScenarioResultApply is deprecated and will be removed in v1.0. Use SHOPPriceScenarioResultWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "SHOPPriceScenarioResult.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class SHOPPriceScenarioResultList(DomainModelList[SHOPPriceScenarioResult]):
    """List of shop price scenario results in the read version."""

    _INSTANCE = SHOPPriceScenarioResult

    def as_write(self) -> SHOPPriceScenarioResultWriteList:
        """Convert these read versions of shop price scenario result to the writing versions."""
        return SHOPPriceScenarioResultWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> SHOPPriceScenarioResultWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPPriceScenarioResultWriteList(DomainModelWriteList[SHOPPriceScenarioResultWrite]):
    """List of shop price scenario results in the writing version."""

    _INSTANCE = SHOPPriceScenarioResultWrite


class SHOPPriceScenarioResultApplyList(SHOPPriceScenarioResultWriteList): ...


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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
