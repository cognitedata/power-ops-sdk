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

if TYPE_CHECKING:
    from ._benchmarking_production_obligation_day_ahead import BenchmarkingProductionObligationDayAhead, BenchmarkingProductionObligationDayAheadGraphQL, BenchmarkingProductionObligationDayAheadWrite
    from ._power_asset import PowerAsset, PowerAssetGraphQL, PowerAssetWrite
    from ._shop_commands import ShopCommands, ShopCommandsGraphQL, ShopCommandsWrite
    from ._shop_model import ShopModel, ShopModelGraphQL, ShopModelWrite


__all__ = [
    "ShopModelWithAssets",
    "ShopModelWithAssetsWrite",
    "ShopModelWithAssetsApply",
    "ShopModelWithAssetsList",
    "ShopModelWithAssetsWriteList",
    "ShopModelWithAssetsApplyList",


    "ShopModelWithAssetsGraphQL",
]


class ShopModelWithAssetsGraphQL(GraphQLCore):
    """This represents the reading version of shop model with asset, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop model with asset.
        data_record: The data record of the shop model with asset node.
        shop_model: The shop model that includes one water course for one shop run
        shop_commands: Commands for the shop file
        power_assets: A list of power assets covered by the Shop model. For a given bid document, we will select the partial bids for these assets, and calculate production obligation for these partial bids (summed up)
        production_obligations: It is possible to specify time series for production obligation - one benchmarking run will be set up for each of these time series. The intended use of this, is to specify the production obligation resulting from the submitted bid, or any other bid document not modelled within PowerOps
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopModelWithAssets", "1")
    shop_model: Optional[ShopModelGraphQL] = Field(default=None, repr=False, alias="shopModel")
    shop_commands: Optional[ShopCommandsGraphQL] = Field(default=None, repr=False, alias="shopCommands")
    power_assets: Optional[list[PowerAssetGraphQL]] = Field(default=None, repr=False, alias="powerAssets")
    production_obligations: Optional[list[BenchmarkingProductionObligationDayAheadGraphQL]] = Field(default=None, repr=False, alias="productionObligations")

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
    @field_validator("shop_model", "shop_commands", "power_assets", "production_obligations", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopModelWithAssets:
        """Convert this GraphQL format of shop model with asset to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopModelWithAssets(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            shop_model=self.shop_model.as_read() if isinstance(self.shop_model, GraphQLCore) else self.shop_model,
            shop_commands=self.shop_commands.as_read() if isinstance(self.shop_commands, GraphQLCore) else self.shop_commands,
            power_assets=[power_asset.as_read() for power_asset in self.power_assets or []],
            production_obligations=[production_obligation.as_read() for production_obligation in self.production_obligations or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopModelWithAssetsWrite:
        """Convert this GraphQL format of shop model with asset to the writing format."""
        return ShopModelWithAssetsWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            shop_model=self.shop_model.as_write() if isinstance(self.shop_model, GraphQLCore) else self.shop_model,
            shop_commands=self.shop_commands.as_write() if isinstance(self.shop_commands, GraphQLCore) else self.shop_commands,
            power_assets=[power_asset.as_write() for power_asset in self.power_assets or []],
            production_obligations=[production_obligation.as_write() for production_obligation in self.production_obligations or []],
        )


class ShopModelWithAssets(DomainModel):
    """This represents the reading version of shop model with asset.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop model with asset.
        data_record: The data record of the shop model with asset node.
        shop_model: The shop model that includes one water course for one shop run
        shop_commands: Commands for the shop file
        power_assets: A list of power assets covered by the Shop model. For a given bid document, we will select the partial bids for these assets, and calculate production obligation for these partial bids (summed up)
        production_obligations: It is possible to specify time series for production obligation - one benchmarking run will be set up for each of these time series. The intended use of this, is to specify the production obligation resulting from the submitted bid, or any other bid document not modelled within PowerOps
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopModelWithAssets", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopModelWithAssets")
    shop_model: Union[ShopModel, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopModel")
    shop_commands: Union[ShopCommands, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopCommands")
    power_assets: Optional[list[Union[PowerAsset, str, dm.NodeId]]] = Field(default=None, repr=False, alias="powerAssets")
    production_obligations: Optional[list[Union[BenchmarkingProductionObligationDayAhead, str, dm.NodeId]]] = Field(default=None, repr=False, alias="productionObligations")

    def as_write(self) -> ShopModelWithAssetsWrite:
        """Convert this read version of shop model with asset to the writing version."""
        return ShopModelWithAssetsWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            shop_model=self.shop_model.as_write() if isinstance(self.shop_model, DomainModel) else self.shop_model,
            shop_commands=self.shop_commands.as_write() if isinstance(self.shop_commands, DomainModel) else self.shop_commands,
            power_assets=[power_asset.as_write() if isinstance(power_asset, DomainModel) else power_asset for power_asset in self.power_assets or []],
            production_obligations=[production_obligation.as_write() if isinstance(production_obligation, DomainModel) else production_obligation for production_obligation in self.production_obligations or []],
        )

    def as_apply(self) -> ShopModelWithAssetsWrite:
        """Convert this read version of shop model with asset to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopModelWithAssetsWrite(DomainModelWrite):
    """This represents the writing version of shop model with asset.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop model with asset.
        data_record: The data record of the shop model with asset node.
        shop_model: The shop model that includes one water course for one shop run
        shop_commands: Commands for the shop file
        power_assets: A list of power assets covered by the Shop model. For a given bid document, we will select the partial bids for these assets, and calculate production obligation for these partial bids (summed up)
        production_obligations: It is possible to specify time series for production obligation - one benchmarking run will be set up for each of these time series. The intended use of this, is to specify the production obligation resulting from the submitted bid, or any other bid document not modelled within PowerOps
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopModelWithAssets", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopModelWithAssets")
    shop_model: Union[ShopModelWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopModel")
    shop_commands: Union[ShopCommandsWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopCommands")
    power_assets: Optional[list[Union[PowerAssetWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="powerAssets")
    production_obligations: Optional[list[Union[BenchmarkingProductionObligationDayAheadWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="productionObligations")

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

        if self.shop_model is not None:
            properties["shopModel"] = {
                "space":  self.space if isinstance(self.shop_model, str) else self.shop_model.space,
                "externalId": self.shop_model if isinstance(self.shop_model, str) else self.shop_model.external_id,
            }

        if self.shop_commands is not None:
            properties["shopCommands"] = {
                "space":  self.space if isinstance(self.shop_commands, str) else self.shop_commands.space,
                "externalId": self.shop_commands if isinstance(self.shop_commands, str) else self.shop_commands.external_id,
            }


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



        edge_type = dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets")
        for power_asset in self.power_assets or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=power_asset,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets")
        for production_obligation in self.production_obligations or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=production_obligation,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.shop_model, DomainModelWrite):
            other_resources = self.shop_model._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.shop_commands, DomainModelWrite):
            other_resources = self.shop_commands._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class ShopModelWithAssetsApply(ShopModelWithAssetsWrite):
    def __new__(cls, *args, **kwargs) -> ShopModelWithAssetsApply:
        warnings.warn(
            "ShopModelWithAssetsApply is deprecated and will be removed in v1.0. Use ShopModelWithAssetsWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopModelWithAssets.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopModelWithAssetsList(DomainModelList[ShopModelWithAssets]):
    """List of shop model with assets in the read version."""

    _INSTANCE = ShopModelWithAssets

    def as_write(self) -> ShopModelWithAssetsWriteList:
        """Convert these read versions of shop model with asset to the writing versions."""
        return ShopModelWithAssetsWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopModelWithAssetsWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopModelWithAssetsWriteList(DomainModelWriteList[ShopModelWithAssetsWrite]):
    """List of shop model with assets in the writing version."""

    _INSTANCE = ShopModelWithAssetsWrite

class ShopModelWithAssetsApplyList(ShopModelWithAssetsWriteList): ...



def _create_shop_model_with_asset_filter(
    view_id: dm.ViewId,
    shop_model: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    shop_commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if shop_model and isinstance(shop_model, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopModel"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": shop_model}))
    if shop_model and isinstance(shop_model, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopModel"), value={"space": shop_model[0], "externalId": shop_model[1]}))
    if shop_model and isinstance(shop_model, list) and isinstance(shop_model[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("shopModel"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in shop_model]))
    if shop_model and isinstance(shop_model, list) and isinstance(shop_model[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("shopModel"), values=[{"space": item[0], "externalId": item[1]} for item in shop_model]))
    if shop_commands and isinstance(shop_commands, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopCommands"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": shop_commands}))
    if shop_commands and isinstance(shop_commands, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopCommands"), value={"space": shop_commands[0], "externalId": shop_commands[1]}))
    if shop_commands and isinstance(shop_commands, list) and isinstance(shop_commands[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("shopCommands"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in shop_commands]))
    if shop_commands and isinstance(shop_commands, list) and isinstance(shop_commands[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("shopCommands"), values=[{"space": item[0], "externalId": item[1]} for item in shop_commands]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
