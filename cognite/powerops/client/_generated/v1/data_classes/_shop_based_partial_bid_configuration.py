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
from ._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationWrite

if TYPE_CHECKING:
    from ._power_asset import PowerAsset, PowerAssetGraphQL, PowerAssetWrite
    from ._scenario_set import ScenarioSet, ScenarioSetGraphQL, ScenarioSetWrite


__all__ = [
    "ShopBasedPartialBidConfiguration",
    "ShopBasedPartialBidConfigurationWrite",
    "ShopBasedPartialBidConfigurationApply",
    "ShopBasedPartialBidConfigurationList",
    "ShopBasedPartialBidConfigurationWriteList",
    "ShopBasedPartialBidConfigurationApplyList",
    "ShopBasedPartialBidConfigurationFields",
    "ShopBasedPartialBidConfigurationTextFields",
]


ShopBasedPartialBidConfigurationTextFields = Literal["name", "method"]
ShopBasedPartialBidConfigurationFields = Literal["name", "method", "add_steps"]

_SHOPBASEDPARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD = {
    "name": "name",
    "method": "method",
    "add_steps": "addSteps",
}


class ShopBasedPartialBidConfigurationGraphQL(GraphQLCore):
    """This represents the reading version of shop based partial bid configuration, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop based partial bid configuration.
        data_record: The data record of the shop based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        add_steps: TODO definition
        power_asset: TODO description
        shop_scenarios: The shop scenario field.
    """

    view_id = dm.ViewId("sp_powerops_models_temp", "ShopBasedPartialBidConfiguration", "1")
    name: Optional[str] = None
    method: Optional[str] = None
    add_steps: Optional[bool] = Field(None, alias="addSteps")
    power_asset: Optional[PowerAssetGraphQL] = Field(None, repr=False, alias="powerAsset")
    shop_scenarios: Optional[ScenarioSetGraphQL] = Field(None, repr=False, alias="shopScenarios")

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

    @field_validator("power_asset", "shop_scenarios", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ShopBasedPartialBidConfiguration:
        """Convert this GraphQL format of shop based partial bid configuration to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopBasedPartialBidConfiguration(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            method=self.method,
            add_steps=self.add_steps,
            power_asset=self.power_asset.as_read() if isinstance(self.power_asset, GraphQLCore) else self.power_asset,
            shop_scenarios=(
                self.shop_scenarios.as_read() if isinstance(self.shop_scenarios, GraphQLCore) else self.shop_scenarios
            ),
        )

    def as_write(self) -> ShopBasedPartialBidConfigurationWrite:
        """Convert this GraphQL format of shop based partial bid configuration to the writing format."""
        return ShopBasedPartialBidConfigurationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            method=self.method,
            add_steps=self.add_steps,
            power_asset=self.power_asset.as_write() if isinstance(self.power_asset, DomainModel) else self.power_asset,
            shop_scenarios=(
                self.shop_scenarios.as_write() if isinstance(self.shop_scenarios, DomainModel) else self.shop_scenarios
            ),
        )


class ShopBasedPartialBidConfiguration(PartialBidConfiguration):
    """This represents the reading version of shop based partial bid configuration.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop based partial bid configuration.
        data_record: The data record of the shop based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        add_steps: TODO definition
        power_asset: TODO description
        shop_scenarios: The shop scenario field.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "ShopBasedPartialBidConfiguration"
    )
    power_asset: Union[PowerAsset, str, dm.NodeId, None] = Field(None, repr=False, alias="powerAsset")
    shop_scenarios: Union[ScenarioSet, str, dm.NodeId, None] = Field(None, repr=False, alias="shopScenarios")

    def as_write(self) -> ShopBasedPartialBidConfigurationWrite:
        """Convert this read version of shop based partial bid configuration to the writing version."""
        return ShopBasedPartialBidConfigurationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            method=self.method,
            add_steps=self.add_steps,
            power_asset=self.power_asset.as_write() if isinstance(self.power_asset, DomainModel) else self.power_asset,
            shop_scenarios=(
                self.shop_scenarios.as_write() if isinstance(self.shop_scenarios, DomainModel) else self.shop_scenarios
            ),
        )

    def as_apply(self) -> ShopBasedPartialBidConfigurationWrite:
        """Convert this read version of shop based partial bid configuration to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopBasedPartialBidConfigurationWrite(PartialBidConfigurationWrite):
    """This represents the writing version of shop based partial bid configuration.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop based partial bid configuration.
        data_record: The data record of the shop based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        add_steps: TODO definition
        power_asset: TODO description
        shop_scenarios: The shop scenario field.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "ShopBasedPartialBidConfiguration"
    )
    power_asset: Union[PowerAssetWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="powerAsset")
    shop_scenarios: Union[ScenarioSetWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="shopScenarios")

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
            ShopBasedPartialBidConfiguration,
            dm.ViewId("sp_powerops_models_temp", "ShopBasedPartialBidConfiguration", "1"),
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.method is not None or write_none:
            properties["method"] = self.method

        if self.add_steps is not None:
            properties["addSteps"] = self.add_steps

        if self.power_asset is not None:
            properties["powerAsset"] = {
                "space": self.space if isinstance(self.power_asset, str) else self.power_asset.space,
                "externalId": self.power_asset if isinstance(self.power_asset, str) else self.power_asset.external_id,
            }

        if self.shop_scenarios is not None:
            properties["shopScenarios"] = {
                "space": self.space if isinstance(self.shop_scenarios, str) else self.shop_scenarios.space,
                "externalId": (
                    self.shop_scenarios if isinstance(self.shop_scenarios, str) else self.shop_scenarios.external_id
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

        if isinstance(self.power_asset, DomainModelWrite):
            other_resources = self.power_asset._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.shop_scenarios, DomainModelWrite):
            other_resources = self.shop_scenarios._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class ShopBasedPartialBidConfigurationApply(ShopBasedPartialBidConfigurationWrite):
    def __new__(cls, *args, **kwargs) -> ShopBasedPartialBidConfigurationApply:
        warnings.warn(
            "ShopBasedPartialBidConfigurationApply is deprecated and will be removed in v1.0. Use ShopBasedPartialBidConfigurationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopBasedPartialBidConfiguration.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopBasedPartialBidConfigurationList(DomainModelList[ShopBasedPartialBidConfiguration]):
    """List of shop based partial bid configurations in the read version."""

    _INSTANCE = ShopBasedPartialBidConfiguration

    def as_write(self) -> ShopBasedPartialBidConfigurationWriteList:
        """Convert these read versions of shop based partial bid configuration to the writing versions."""
        return ShopBasedPartialBidConfigurationWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopBasedPartialBidConfigurationWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopBasedPartialBidConfigurationWriteList(DomainModelWriteList[ShopBasedPartialBidConfigurationWrite]):
    """List of shop based partial bid configurations in the writing version."""

    _INSTANCE = ShopBasedPartialBidConfigurationWrite


class ShopBasedPartialBidConfigurationApplyList(ShopBasedPartialBidConfigurationWriteList): ...


def _create_shop_based_partial_bid_configuration_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    add_steps: bool | None = None,
    power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    shop_scenarios: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if isinstance(add_steps, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("addSteps"), value=add_steps))
    if power_asset and isinstance(power_asset, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("powerAsset"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": power_asset},
            )
        )
    if power_asset and isinstance(power_asset, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("powerAsset"), value={"space": power_asset[0], "externalId": power_asset[1]}
            )
        )
    if power_asset and isinstance(power_asset, list) and isinstance(power_asset[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("powerAsset"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in power_asset],
            )
        )
    if power_asset and isinstance(power_asset, list) and isinstance(power_asset[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("powerAsset"),
                values=[{"space": item[0], "externalId": item[1]} for item in power_asset],
            )
        )
    if shop_scenarios and isinstance(shop_scenarios, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("shopScenarios"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": shop_scenarios},
            )
        )
    if shop_scenarios and isinstance(shop_scenarios, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("shopScenarios"),
                value={"space": shop_scenarios[0], "externalId": shop_scenarios[1]},
            )
        )
    if shop_scenarios and isinstance(shop_scenarios, list) and isinstance(shop_scenarios[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("shopScenarios"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in shop_scenarios],
            )
        )
    if shop_scenarios and isinstance(shop_scenarios, list) and isinstance(shop_scenarios[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("shopScenarios"),
                values=[{"space": item[0], "externalId": item[1]} for item in shop_scenarios],
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
