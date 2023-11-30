from __future__ import annotations

from typing import Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)


__all__ = ["SHOP", "SHOPApply", "SHOPList", "SHOPApplyList", "SHOPFields", "SHOPTextFields"]


SHOPTextFields = Literal["name", "shop_cases"]
SHOPFields = Literal["name", "shop_cases", "price_scenarios"]

_SHOP_PROPERTIES_BY_FIELD = {
    "name": "name",
    "shop_cases": "shopCases",
    "price_scenarios": "priceScenarios",
}


class SHOP(DomainModel):
    """This represents the reading version of shop.

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

    space: str = "poweropsDayAheadFrontendContractModel"
    name: Optional[str] = None
    shop_cases: Optional[list[str]] = Field(None, alias="shopCases")
    price_scenarios: Union[TimeSeries, str, None] = Field(None, alias="priceScenarios")

    def as_apply(self) -> SHOPApply:
        """Convert this read version of shop to the writing version."""
        return SHOPApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            shop_cases=self.shop_cases,
            price_scenarios=self.price_scenarios,
        )


class SHOPApply(DomainModelApply):
    """This represents the writing version of shop.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop.
        name: The name field.
        shop_cases: The shop case field.
        price_scenarios: The price scenario field.
        existing_version: Fail the ingestion request if the shop version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "poweropsDayAheadFrontendContractModel"
    name: str
    shop_cases: Optional[list[str]] = Field(None, alias="shopCases")
    price_scenarios: Union[TimeSeries, str, None] = Field(None, alias="priceScenarios")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "poweropsDayAheadFrontendContractModel", "SHOP", "1"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.shop_cases is not None:
            properties["shopCases"] = self.shop_cases
        if self.price_scenarios is not None:
            properties["priceScenarios"] = (
                self.price_scenarios if isinstance(self.price_scenarios, str) else self.price_scenarios.external_id
            )

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

        if isinstance(self.price_scenarios, CogniteTimeSeries):
            resources.time_series.append(self.price_scenarios)

        return resources


class SHOPList(DomainModelList[SHOP]):
    """List of shops in the read version."""

    _INSTANCE = SHOP

    def as_apply(self) -> SHOPApplyList:
        """Convert these read versions of shop to the writing versions."""
        return SHOPApplyList([node.as_apply() for node in self.data])


class SHOPApplyList(DomainModelApplyList[SHOPApply]):
    """List of shops in the writing version."""

    _INSTANCE = SHOPApply


def _create_shop_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
