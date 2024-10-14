from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)
from ._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationWrite

if TYPE_CHECKING:
    from ._plant_water_value_based import PlantWaterValueBased, PlantWaterValueBasedGraphQL, PlantWaterValueBasedWrite


__all__ = [
    "WaterValueBasedPartialBidConfiguration",
    "WaterValueBasedPartialBidConfigurationWrite",
    "WaterValueBasedPartialBidConfigurationApply",
    "WaterValueBasedPartialBidConfigurationList",
    "WaterValueBasedPartialBidConfigurationWriteList",
    "WaterValueBasedPartialBidConfigurationApplyList",
    "WaterValueBasedPartialBidConfigurationFields",
    "WaterValueBasedPartialBidConfigurationTextFields",
    "WaterValueBasedPartialBidConfigurationGraphQL",
]


WaterValueBasedPartialBidConfigurationTextFields = Literal["name", "method"]
WaterValueBasedPartialBidConfigurationFields = Literal["name", "method", "add_steps"]

_WATERVALUEBASEDPARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD = {
    "name": "name",
    "method": "method",
    "add_steps": "addSteps",
}

class WaterValueBasedPartialBidConfigurationGraphQL(GraphQLCore):
    """This represents the reading version of water value based partial bid configuration, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid configuration.
        data_record: The data record of the water value based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        power_asset: TODO description (has to be a Plant)
        add_steps: TODO definition
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "WaterValueBasedPartialBidConfiguration", "1")
    name: Optional[str] = None
    method: Optional[str] = None
    power_asset: Optional[PlantWaterValueBasedGraphQL] = Field(default=None, repr=False, alias="powerAsset")
    add_steps: Optional[bool] = Field(None, alias="addSteps")

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
    @field_validator("power_asset", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> WaterValueBasedPartialBidConfiguration:
        """Convert this GraphQL format of water value based partial bid configuration to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return WaterValueBasedPartialBidConfiguration(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            method=self.method,
            power_asset=self.power_asset.as_read() if isinstance(self.power_asset, GraphQLCore) else self.power_asset,
            add_steps=self.add_steps,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> WaterValueBasedPartialBidConfigurationWrite:
        """Convert this GraphQL format of water value based partial bid configuration to the writing format."""
        return WaterValueBasedPartialBidConfigurationWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            method=self.method,
            power_asset=self.power_asset.as_write() if isinstance(self.power_asset, GraphQLCore) else self.power_asset,
            add_steps=self.add_steps,
        )


class WaterValueBasedPartialBidConfiguration(PartialBidConfiguration):
    """This represents the reading version of water value based partial bid configuration.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid configuration.
        data_record: The data record of the water value based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        power_asset: TODO description (has to be a Plant)
        add_steps: TODO definition
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "WaterValueBasedPartialBidConfiguration", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "WaterValueBasedPartialBidConfiguration")

    def as_write(self) -> WaterValueBasedPartialBidConfigurationWrite:
        """Convert this read version of water value based partial bid configuration to the writing version."""
        return WaterValueBasedPartialBidConfigurationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            method=self.method,
            power_asset=self.power_asset.as_write() if isinstance(self.power_asset, DomainModel) else self.power_asset,
            add_steps=self.add_steps,
        )

    def as_apply(self) -> WaterValueBasedPartialBidConfigurationWrite:
        """Convert this read version of water value based partial bid configuration to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WaterValueBasedPartialBidConfigurationWrite(PartialBidConfigurationWrite):
    """This represents the writing version of water value based partial bid configuration.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid configuration.
        data_record: The data record of the water value based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        power_asset: TODO description (has to be a Plant)
        add_steps: TODO definition
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "WaterValueBasedPartialBidConfiguration", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "WaterValueBasedPartialBidConfiguration")

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.method is not None or write_none:
            properties["method"] = self.method

        if self.power_asset is not None:
            properties["powerAsset"] = {
                "space":  self.space if isinstance(self.power_asset, str) else self.power_asset.space,
                "externalId": self.power_asset if isinstance(self.power_asset, str) else self.power_asset.external_id,
            }

        if self.add_steps is not None:
            properties["addSteps"] = self.add_steps


        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())



        if isinstance(self.power_asset, DomainModelWrite):
            other_resources = self.power_asset._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class WaterValueBasedPartialBidConfigurationApply(WaterValueBasedPartialBidConfigurationWrite):
    def __new__(cls, *args, **kwargs) -> WaterValueBasedPartialBidConfigurationApply:
        warnings.warn(
            "WaterValueBasedPartialBidConfigurationApply is deprecated and will be removed in v1.0. Use WaterValueBasedPartialBidConfigurationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "WaterValueBasedPartialBidConfiguration.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class WaterValueBasedPartialBidConfigurationList(DomainModelList[WaterValueBasedPartialBidConfiguration]):
    """List of water value based partial bid configurations in the read version."""

    _INSTANCE = WaterValueBasedPartialBidConfiguration

    def as_write(self) -> WaterValueBasedPartialBidConfigurationWriteList:
        """Convert these read versions of water value based partial bid configuration to the writing versions."""
        return WaterValueBasedPartialBidConfigurationWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> WaterValueBasedPartialBidConfigurationWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WaterValueBasedPartialBidConfigurationWriteList(DomainModelWriteList[WaterValueBasedPartialBidConfigurationWrite]):
    """List of water value based partial bid configurations in the writing version."""

    _INSTANCE = WaterValueBasedPartialBidConfigurationWrite

class WaterValueBasedPartialBidConfigurationApplyList(WaterValueBasedPartialBidConfigurationWriteList): ...



def _create_water_value_based_partial_bid_configuration_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    add_steps: bool | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
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
    if power_asset and isinstance(power_asset, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("powerAsset"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": power_asset}))
    if power_asset and isinstance(power_asset, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("powerAsset"), value={"space": power_asset[0], "externalId": power_asset[1]}))
    if power_asset and isinstance(power_asset, list) and isinstance(power_asset[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("powerAsset"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in power_asset]))
    if power_asset and isinstance(power_asset, list) and isinstance(power_asset[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("powerAsset"), values=[{"space": item[0], "externalId": item[1]} for item in power_asset]))
    if isinstance(add_steps, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("addSteps"), value=add_steps))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
