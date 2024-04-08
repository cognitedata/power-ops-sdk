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
    from ._shop_result import SHOPResult, SHOPResultGraphQL, SHOPResultWrite


__all__ = [
    "PriceProduction",
    "PriceProductionWrite",
    "PriceProductionApply",
    "PriceProductionList",
    "PriceProductionWriteList",
    "PriceProductionApplyList",
    "PriceProductionFields",
    "PriceProductionTextFields",
]


PriceProductionTextFields = Literal["price", "production"]
PriceProductionFields = Literal["price", "production"]

_PRICEPRODUCTION_PROPERTIES_BY_FIELD = {
    "price": "price",
    "production": "production",
}


class PriceProductionGraphQL(GraphQLCore):
    """This represents the reading version of price production, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price production.
        data_record: The data record of the price production node.
        price: The price field.
        production: The production field.
        shop_result: The shop result field.
    """

    view_id = dm.ViewId("sp_powerops_models_temp", "PriceProduction", "1")
    price: Union[TimeSeries, str, None] = None
    production: Union[TimeSeries, str, None] = None
    shop_result: Optional[SHOPResultGraphQL] = Field(None, repr=False, alias="shopResult")

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

    @field_validator("shop_result", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> PriceProduction:
        """Convert this GraphQL format of price production to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PriceProduction(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            price=self.price,
            production=self.production,
            shop_result=self.shop_result.as_read() if isinstance(self.shop_result, GraphQLCore) else self.shop_result,
        )

    def as_write(self) -> PriceProductionWrite:
        """Convert this GraphQL format of price production to the writing format."""
        return PriceProductionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            price=self.price,
            production=self.production,
            shop_result=self.shop_result.as_write() if isinstance(self.shop_result, DomainModel) else self.shop_result,
        )


class PriceProduction(DomainModel):
    """This represents the reading version of price production.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price production.
        data_record: The data record of the price production node.
        price: The price field.
        production: The production field.
        shop_result: The shop result field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "PriceProduction"
    )
    price: Union[TimeSeries, str, None] = None
    production: Union[TimeSeries, str, None] = None
    shop_result: Union[SHOPResult, str, dm.NodeId, None] = Field(None, repr=False, alias="shopResult")

    def as_write(self) -> PriceProductionWrite:
        """Convert this read version of price production to the writing version."""
        return PriceProductionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            price=self.price,
            production=self.production,
            shop_result=self.shop_result.as_write() if isinstance(self.shop_result, DomainModel) else self.shop_result,
        )

    def as_apply(self) -> PriceProductionWrite:
        """Convert this read version of price production to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceProductionWrite(DomainModelWrite):
    """This represents the writing version of price production.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price production.
        data_record: The data record of the price production node.
        price: The price field.
        production: The production field.
        shop_result: The shop result field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "PriceProduction"
    )
    price: Union[TimeSeries, str, None] = None
    production: Union[TimeSeries, str, None] = None
    shop_result: Union[SHOPResultWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="shopResult")

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
            PriceProduction, dm.ViewId("sp_powerops_models_temp", "PriceProduction", "1")
        )

        properties: dict[str, Any] = {}

        if self.price is not None:
            if isinstance(self.price, str) or self.price is None:
                properties["price"] = self.price
            else:
                properties["price"] = self.price.external_id

        if self.production is not None:
            if isinstance(self.production, str) or self.production is None:
                properties["production"] = self.production
            else:
                properties["production"] = self.production.external_id

        if self.shop_result is not None:
            properties["shopResult"] = {
                "space": self.space if isinstance(self.shop_result, str) else self.shop_result.space,
                "externalId": self.shop_result if isinstance(self.shop_result, str) else self.shop_result.external_id,
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

        if isinstance(self.shop_result, DomainModelWrite):
            other_resources = self.shop_result._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.price, CogniteTimeSeries):
            resources.time_series.append(self.price)

        if isinstance(self.production, CogniteTimeSeries):
            resources.time_series.append(self.production)

        return resources


class PriceProductionApply(PriceProductionWrite):
    def __new__(cls, *args, **kwargs) -> PriceProductionApply:
        warnings.warn(
            "PriceProductionApply is deprecated and will be removed in v1.0. Use PriceProductionWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PriceProduction.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PriceProductionList(DomainModelList[PriceProduction]):
    """List of price productions in the read version."""

    _INSTANCE = PriceProduction

    def as_write(self) -> PriceProductionWriteList:
        """Convert these read versions of price production to the writing versions."""
        return PriceProductionWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PriceProductionWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceProductionWriteList(DomainModelWriteList[PriceProductionWrite]):
    """List of price productions in the writing version."""

    _INSTANCE = PriceProductionWrite


class PriceProductionApplyList(PriceProductionWriteList): ...


def _create_price_production_filter(
    view_id: dm.ViewId,
    shop_result: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if shop_result and isinstance(shop_result, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("shopResult"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": shop_result},
            )
        )
    if shop_result and isinstance(shop_result, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("shopResult"), value={"space": shop_result[0], "externalId": shop_result[1]}
            )
        )
    if shop_result and isinstance(shop_result, list) and isinstance(shop_result[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("shopResult"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in shop_result],
            )
        )
    if shop_result and isinstance(shop_result, list) and isinstance(shop_result[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("shopResult"),
                values=[{"space": item[0], "externalId": item[1]} for item in shop_result],
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
