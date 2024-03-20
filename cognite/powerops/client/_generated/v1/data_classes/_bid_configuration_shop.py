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
    from ._bid_method_shop_multi_scenario import (
        BidMethodSHOPMultiScenario,
        BidMethodSHOPMultiScenarioGraphQL,
        BidMethodSHOPMultiScenarioWrite,
    )
    from ._market_configuration import MarketConfiguration, MarketConfigurationGraphQL, MarketConfigurationWrite
    from ._plant_shop import PlantShop, PlantShopGraphQL, PlantShopWrite
    from ._price_area import PriceArea, PriceAreaGraphQL, PriceAreaWrite
    from ._watercourse_shop import WatercourseShop, WatercourseShopGraphQL, WatercourseShopWrite


__all__ = [
    "BidConfigurationShop",
    "BidConfigurationShopWrite",
    "BidConfigurationShopApply",
    "BidConfigurationShopList",
    "BidConfigurationShopWriteList",
    "BidConfigurationShopApplyList",
    "BidConfigurationShopFields",
    "BidConfigurationShopTextFields",
]


BidConfigurationShopTextFields = Literal["name"]
BidConfigurationShopFields = Literal["name"]

_BIDCONFIGURATIONSHOP_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class BidConfigurationShopGraphQL(GraphQLCore):
    """This represents the reading version of bid configuration shop, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid configuration shop.
        data_record: The data record of the bid configuration shop node.
        market_configuration: The bid method related to the bid configuration
        name: The name of the bid configuration
        method: The bid method related to the bid configuration
        price_area: The price area related to the bid configuration
        plants_shop: The plants modelled in the shop runs
        watercourses_shop: The watercourses modelled in the shop runs
    """

    view_id = dm.ViewId("sp_powerops_models", "BidConfigurationShop", "1")
    market_configuration: Optional[MarketConfigurationGraphQL] = Field(None, repr=False, alias="marketConfiguration")
    name: Optional[str] = None
    method: Optional[BidMethodSHOPMultiScenarioGraphQL] = Field(None, repr=False)
    price_area: Optional[PriceAreaGraphQL] = Field(None, repr=False, alias="priceArea")
    plants_shop: Optional[list[PlantShopGraphQL]] = Field(default=None, repr=False, alias="plantsShop")
    watercourses_shop: Optional[list[WatercourseShopGraphQL]] = Field(
        default=None, repr=False, alias="watercoursesShop"
    )

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

    @field_validator("market_configuration", "method", "price_area", "plants_shop", "watercourses_shop", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> BidConfigurationShop:
        """Convert this GraphQL format of bid configuration shop to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidConfigurationShop(
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
            name=self.name,
            method=self.method.as_read() if isinstance(self.method, GraphQLCore) else self.method,
            price_area=self.price_area.as_read() if isinstance(self.price_area, GraphQLCore) else self.price_area,
            plants_shop=[
                plants_shop.as_read() if isinstance(plants_shop, GraphQLCore) else plants_shop
                for plants_shop in self.plants_shop or []
            ],
            watercourses_shop=[
                watercourses_shop.as_read() if isinstance(watercourses_shop, GraphQLCore) else watercourses_shop
                for watercourses_shop in self.watercourses_shop or []
            ],
        )

    def as_write(self) -> BidConfigurationShopWrite:
        """Convert this GraphQL format of bid configuration shop to the writing format."""
        return BidConfigurationShopWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            market_configuration=(
                self.market_configuration.as_write()
                if isinstance(self.market_configuration, DomainModel)
                else self.market_configuration
            ),
            name=self.name,
            method=self.method.as_write() if isinstance(self.method, DomainModel) else self.method,
            price_area=self.price_area.as_write() if isinstance(self.price_area, DomainModel) else self.price_area,
            plants_shop=[
                plants_shop.as_write() if isinstance(plants_shop, DomainModel) else plants_shop
                for plants_shop in self.plants_shop or []
            ],
            watercourses_shop=[
                watercourses_shop.as_write() if isinstance(watercourses_shop, DomainModel) else watercourses_shop
                for watercourses_shop in self.watercourses_shop or []
            ],
        )


class BidConfigurationShop(BidConfiguration):
    """This represents the reading version of bid configuration shop.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid configuration shop.
        data_record: The data record of the bid configuration shop node.
        market_configuration: The bid method related to the bid configuration
        name: The name of the bid configuration
        method: The bid method related to the bid configuration
        price_area: The price area related to the bid configuration
        plants_shop: The plants modelled in the shop runs
        watercourses_shop: The watercourses modelled in the shop runs
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "BidConfigurationShop"
    )
    name: Optional[str] = None
    method: Union[BidMethodSHOPMultiScenario, str, dm.NodeId, None] = Field(None, repr=False)
    price_area: Union[PriceArea, str, dm.NodeId, None] = Field(None, repr=False, alias="priceArea")
    plants_shop: Union[list[PlantShop], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="plantsShop"
    )
    watercourses_shop: Union[list[WatercourseShop], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="watercoursesShop"
    )

    def as_write(self) -> BidConfigurationShopWrite:
        """Convert this read version of bid configuration shop to the writing version."""
        return BidConfigurationShopWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            market_configuration=(
                self.market_configuration.as_write()
                if isinstance(self.market_configuration, DomainModel)
                else self.market_configuration
            ),
            name=self.name,
            method=self.method.as_write() if isinstance(self.method, DomainModel) else self.method,
            price_area=self.price_area.as_write() if isinstance(self.price_area, DomainModel) else self.price_area,
            plants_shop=[
                plants_shop.as_write() if isinstance(plants_shop, DomainModel) else plants_shop
                for plants_shop in self.plants_shop or []
            ],
            watercourses_shop=[
                watercourses_shop.as_write() if isinstance(watercourses_shop, DomainModel) else watercourses_shop
                for watercourses_shop in self.watercourses_shop or []
            ],
        )

    def as_apply(self) -> BidConfigurationShopWrite:
        """Convert this read version of bid configuration shop to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidConfigurationShopWrite(BidConfigurationWrite):
    """This represents the writing version of bid configuration shop.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid configuration shop.
        data_record: The data record of the bid configuration shop node.
        market_configuration: The bid method related to the bid configuration
        name: The name of the bid configuration
        method: The bid method related to the bid configuration
        price_area: The price area related to the bid configuration
        plants_shop: The plants modelled in the shop runs
        watercourses_shop: The watercourses modelled in the shop runs
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "BidConfigurationShop"
    )
    name: Optional[str] = None
    method: Union[BidMethodSHOPMultiScenarioWrite, str, dm.NodeId, None] = Field(None, repr=False)
    price_area: Union[PriceAreaWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="priceArea")
    plants_shop: Union[list[PlantShopWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="plantsShop"
    )
    watercourses_shop: Union[list[WatercourseShopWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="watercoursesShop"
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
            BidConfigurationShop, dm.ViewId("sp_powerops_models", "BidConfigurationShop", "1")
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

        if self.name is not None or write_none:
            properties["name"] = self.name

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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.plantsShop")
        for plants_shop in self.plants_shop or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=plants_shop,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.watercoursesShop")
        for watercourses_shop in self.watercourses_shop or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=watercourses_shop,
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


class BidConfigurationShopApply(BidConfigurationShopWrite):
    def __new__(cls, *args, **kwargs) -> BidConfigurationShopApply:
        warnings.warn(
            "BidConfigurationShopApply is deprecated and will be removed in v1.0. Use BidConfigurationShopWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidConfigurationShop.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidConfigurationShopList(DomainModelList[BidConfigurationShop]):
    """List of bid configuration shops in the read version."""

    _INSTANCE = BidConfigurationShop

    def as_write(self) -> BidConfigurationShopWriteList:
        """Convert these read versions of bid configuration shop to the writing versions."""
        return BidConfigurationShopWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidConfigurationShopWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidConfigurationShopWriteList(DomainModelWriteList[BidConfigurationShopWrite]):
    """List of bid configuration shops in the writing version."""

    _INSTANCE = BidConfigurationShopWrite


class BidConfigurationShopApplyList(BidConfigurationShopWriteList): ...


def _create_bid_configuration_shop_filter(
    view_id: dm.ViewId,
    market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
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
