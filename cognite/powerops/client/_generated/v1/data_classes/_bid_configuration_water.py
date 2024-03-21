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
from ._bid_configuration import BidConfiguration, BidConfigurationWrite

if TYPE_CHECKING:
    from ._bid_method_water_value import BidMethodWaterValue, BidMethodWaterValueGraphQL, BidMethodWaterValueWrite
    from ._market_configuration import MarketConfiguration, MarketConfigurationGraphQL, MarketConfigurationWrite
    from ._plant import Plant, PlantGraphQL, PlantWrite
    from ._price_area import PriceArea, PriceAreaGraphQL, PriceAreaWrite
    from ._watercourse import Watercourse, WatercourseGraphQL, WatercourseWrite


__all__ = [
    "BidConfigurationWater",
    "BidConfigurationWaterWrite",
    "BidConfigurationWaterApply",
    "BidConfigurationWaterList",
    "BidConfigurationWaterWriteList",
    "BidConfigurationWaterApplyList",
]


class BidConfigurationWaterGraphQL(GraphQLCore):
    """This represents the reading version of bid configuration water, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid configuration water.
        data_record: The data record of the bid configuration water node.
        market_configuration: The bid method related to the bid configuration
        method: The bid method related to the bid configuration
        price_area: The price area related to the bid configuration
        plants: The plants and related information needed to run the water value based method
        watercourses: The watercourses and related information needed to run the water value based method
    """

    view_id = dm.ViewId("sp_powerops_models", "BidConfigurationWater", "1")
    market_configuration: Optional[MarketConfigurationGraphQL] = Field(None, repr=False, alias="marketConfiguration")
    method: Optional[BidMethodWaterValueGraphQL] = Field(None, repr=False)
    price_area: Optional[PriceAreaGraphQL] = Field(None, repr=False, alias="priceArea")
    plants: Optional[list[PlantGraphQL]] = Field(default=None, repr=False)
    watercourses: Optional[list[WatercourseGraphQL]] = Field(default=None, repr=False)

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

    @field_validator("market_configuration", "method", "price_area", "plants", "watercourses", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> BidConfigurationWater:
        """Convert this GraphQL format of bid configuration water to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidConfigurationWater(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            market_configuration=(
                self.market_configuration.as_read()
                if isinstance(self.market_configuration, GraphQLCore)
                else self.market_configuration
            ),
            method=self.method.as_read() if isinstance(self.method, GraphQLCore) else self.method,
            price_area=self.price_area.as_read() if isinstance(self.price_area, GraphQLCore) else self.price_area,
            plants=[plant.as_read() if isinstance(plant, GraphQLCore) else plant for plant in self.plants or []],
            watercourses=[
                watercourse.as_read() if isinstance(watercourse, GraphQLCore) else watercourse
                for watercourse in self.watercourses or []
            ],
        )

    def as_write(self) -> BidConfigurationWaterWrite:
        """Convert this GraphQL format of bid configuration water to the writing format."""
        return BidConfigurationWaterWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            market_configuration=(
                self.market_configuration.as_write()
                if isinstance(self.market_configuration, DomainModel)
                else self.market_configuration
            ),
            method=self.method.as_write() if isinstance(self.method, DomainModel) else self.method,
            price_area=self.price_area.as_write() if isinstance(self.price_area, DomainModel) else self.price_area,
            plants=[plant.as_write() if isinstance(plant, DomainModel) else plant for plant in self.plants or []],
            watercourses=[
                watercourse.as_write() if isinstance(watercourse, DomainModel) else watercourse
                for watercourse in self.watercourses or []
            ],
        )


class BidConfigurationWater(BidConfiguration):
    """This represents the reading version of bid configuration water.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid configuration water.
        data_record: The data record of the bid configuration water node.
        market_configuration: The bid method related to the bid configuration
        method: The bid method related to the bid configuration
        price_area: The price area related to the bid configuration
        plants: The plants and related information needed to run the water value based method
        watercourses: The watercourses and related information needed to run the water value based method
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "BidConfigurationWater"
    )
    method: Union[BidMethodWaterValue, str, dm.NodeId, None] = Field(None, repr=False)
    price_area: Union[PriceArea, str, dm.NodeId, None] = Field(None, repr=False, alias="priceArea")
    plants: Union[list[Plant], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)
    watercourses: Union[list[Watercourse], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)

    def as_write(self) -> BidConfigurationWaterWrite:
        """Convert this read version of bid configuration water to the writing version."""
        return BidConfigurationWaterWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            market_configuration=(
                self.market_configuration.as_write()
                if isinstance(self.market_configuration, DomainModel)
                else self.market_configuration
            ),
            method=self.method.as_write() if isinstance(self.method, DomainModel) else self.method,
            price_area=self.price_area.as_write() if isinstance(self.price_area, DomainModel) else self.price_area,
            plants=[plant.as_write() if isinstance(plant, DomainModel) else plant for plant in self.plants or []],
            watercourses=[
                watercourse.as_write() if isinstance(watercourse, DomainModel) else watercourse
                for watercourse in self.watercourses or []
            ],
        )

    def as_apply(self) -> BidConfigurationWaterWrite:
        """Convert this read version of bid configuration water to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidConfigurationWaterWrite(BidConfigurationWrite):
    """This represents the writing version of bid configuration water.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid configuration water.
        data_record: The data record of the bid configuration water node.
        market_configuration: The bid method related to the bid configuration
        method: The bid method related to the bid configuration
        price_area: The price area related to the bid configuration
        plants: The plants and related information needed to run the water value based method
        watercourses: The watercourses and related information needed to run the water value based method
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "BidConfigurationWater"
    )
    method: Union[BidMethodWaterValueWrite, str, dm.NodeId, None] = Field(None, repr=False)
    price_area: Union[PriceAreaWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="priceArea")
    plants: Union[list[PlantWrite], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)
    watercourses: Union[list[WatercourseWrite], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)

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
            BidConfigurationWater, dm.ViewId("sp_powerops_models", "BidConfigurationWater", "1")
        )

        properties: dict[str, Any] = {}

        if self.market_configuration is not None:
            properties["marketConfiguration"] = {
                "space": self.space if isinstance(self.market_configuration, str) else self.market_configuration.space,
                "externalId": (
                    self.market_configuration
                    if isinstance(self.market_configuration, str)
                    else self.market_configuration.external_id
                ),
            }

        if self.method is not None:
            properties["method"] = {
                "space": self.space if isinstance(self.method, str) else self.method.space,
                "externalId": self.method if isinstance(self.method, str) else self.method.external_id,
            }

        if self.price_area is not None:
            properties["priceArea"] = {
                "space": self.space if isinstance(self.price_area, str) else self.price_area.space,
                "externalId": self.price_area if isinstance(self.price_area, str) else self.price_area.external_id,
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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.plants")
        for plant in self.plants or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=plant,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.watercourses")
        for watercourse in self.watercourses or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=watercourse,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.market_configuration, DomainModelWrite):
            other_resources = self.market_configuration._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.method, DomainModelWrite):
            other_resources = self.method._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.price_area, DomainModelWrite):
            other_resources = self.price_area._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class BidConfigurationWaterApply(BidConfigurationWaterWrite):
    def __new__(cls, *args, **kwargs) -> BidConfigurationWaterApply:
        warnings.warn(
            "BidConfigurationWaterApply is deprecated and will be removed in v1.0. Use BidConfigurationWaterWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidConfigurationWater.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidConfigurationWaterList(DomainModelList[BidConfigurationWater]):
    """List of bid configuration waters in the read version."""

    _INSTANCE = BidConfigurationWater

    def as_write(self) -> BidConfigurationWaterWriteList:
        """Convert these read versions of bid configuration water to the writing versions."""
        return BidConfigurationWaterWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidConfigurationWaterWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidConfigurationWaterWriteList(DomainModelWriteList[BidConfigurationWaterWrite]):
    """List of bid configuration waters in the writing version."""

    _INSTANCE = BidConfigurationWaterWrite


class BidConfigurationWaterApplyList(BidConfigurationWaterWriteList): ...


def _create_bid_configuration_water_filter(
    view_id: dm.ViewId,
    market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if market_configuration and isinstance(market_configuration, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketConfiguration"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": market_configuration},
            )
        )
    if market_configuration and isinstance(market_configuration, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketConfiguration"),
                value={"space": market_configuration[0], "externalId": market_configuration[1]},
            )
        )
    if market_configuration and isinstance(market_configuration, list) and isinstance(market_configuration[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketConfiguration"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in market_configuration],
            )
        )
    if market_configuration and isinstance(market_configuration, list) and isinstance(market_configuration[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketConfiguration"),
                values=[{"space": item[0], "externalId": item[1]} for item in market_configuration],
            )
        )
    if method and isinstance(method, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("method"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": method}
            )
        )
    if method and isinstance(method, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("method"), value={"space": method[0], "externalId": method[1]})
        )
    if method and isinstance(method, list) and isinstance(method[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("method"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in method],
            )
        )
    if method and isinstance(method, list) and isinstance(method[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("method"), values=[{"space": item[0], "externalId": item[1]} for item in method]
            )
        )
    if price_area and isinstance(price_area, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("priceArea"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": price_area}
            )
        )
    if price_area and isinstance(price_area, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("priceArea"), value={"space": price_area[0], "externalId": price_area[1]}
            )
        )
    if price_area and isinstance(price_area, list) and isinstance(price_area[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("priceArea"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in price_area],
            )
        )
    if price_area and isinstance(price_area, list) and isinstance(price_area[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("priceArea"),
                values=[{"space": item[0], "externalId": item[1]} for item in price_area],
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
