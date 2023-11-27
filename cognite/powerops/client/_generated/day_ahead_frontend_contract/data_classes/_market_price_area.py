from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._bid_method import BidMethodApply

__all__ = [
    "MarketPriceArea",
    "MarketPriceAreaApply",
    "MarketPriceAreaList",
    "MarketPriceAreaApplyList",
    "MarketPriceAreaFields",
    "MarketPriceAreaTextFields",
]


MarketPriceAreaTextFields = Literal["name", "price_area", "timezone", "main_scenario", "price_scenarios"]
MarketPriceAreaFields = Literal["name", "price_area", "timezone", "main_scenario", "price_scenarios"]

_MARKETPRICEAREA_PROPERTIES_BY_FIELD = {
    "name": "name",
    "price_area": "priceArea",
    "timezone": "timezone",
    "main_scenario": "mainScenario",
    "price_scenarios": "priceScenarios",
}


class MarketPriceArea(DomainModel):
    """This represent a read version of market price area.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market price area.
        name: The name field.
        price_area: The price area field.
        default_method: The default method field.
        timezone: The timezone field.
        main_scenario: The main scenario field.
        price_scenarios: The price scenario field.
        created_time: The created time of the market price area node.
        last_updated_time: The last updated time of the market price area node.
        deleted_time: If present, the deleted time of the market price area node.
        version: The version of the market price area node.
    """

    space: str = "dayAheadFrontendContractModel"
    name: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    default_method: Optional[str] = Field(None, alias="defaultMethod")
    timezone: Optional[str] = None
    main_scenario: Optional[str] = Field(None, alias="mainScenario")
    price_scenarios: Optional[list[str]] = Field(None, alias="priceScenarios")

    def as_apply(self) -> MarketPriceAreaApply:
        """Convert this read version of market price area to a write version."""
        return MarketPriceAreaApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            price_area=self.price_area,
            default_method=self.default_method,
            timezone=self.timezone,
            main_scenario=self.main_scenario,
            price_scenarios=self.price_scenarios,
        )


class MarketPriceAreaApply(DomainModelApply):
    """This represent a write version of market price area.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market price area.
        name: The name field.
        price_area: The price area field.
        default_method: The default method field.
        timezone: The timezone field.
        main_scenario: The main scenario field.
        price_scenarios: The price scenario field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "dayAheadFrontendContractModel"
    name: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    default_method: Union[BidMethodApply, str, None] = Field(None, repr=False, alias="defaultMethod")
    timezone: Optional[str] = None
    main_scenario: Optional[str] = Field(None, alias="mainScenario")
    price_scenarios: Optional[list[str]] = Field(None, alias="priceScenarios")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.price_area is not None:
            properties["priceArea"] = self.price_area
        if self.default_method is not None:
            properties["defaultMethod"] = {
                "space": self.space if isinstance(self.default_method, str) else self.default_method.space,
                "externalId": self.default_method
                if isinstance(self.default_method, str)
                else self.default_method.external_id,
            }
        if self.timezone is not None:
            properties["timezone"] = self.timezone
        if self.main_scenario is not None:
            properties["mainScenario"] = self.main_scenario
        if self.price_scenarios is not None:
            properties["priceScenarios"] = self.price_scenarios
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("dayAheadFrontendContractModel", "MarketPriceArea", "1"),
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

        if isinstance(self.default_method, DomainModelApply):
            instances = self.default_method._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class MarketPriceAreaList(TypeList[MarketPriceArea]):
    """List of market price areas in read version."""

    _NODE = MarketPriceArea

    def as_apply(self) -> MarketPriceAreaApplyList:
        """Convert this read version of market price area to a write version."""
        return MarketPriceAreaApplyList([node.as_apply() for node in self.data])


class MarketPriceAreaApplyList(TypeApplyList[MarketPriceAreaApply]):
    """List of market price areas in write version."""

    _NODE = MarketPriceAreaApply
