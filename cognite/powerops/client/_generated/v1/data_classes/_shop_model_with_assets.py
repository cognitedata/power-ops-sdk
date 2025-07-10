from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from cognite.powerops.client._generated.v1.config import global_config
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    T_DomainModelList,
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    DirectRelationFilter,
)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._benchmarking_production_obligation_day_ahead import BenchmarkingProductionObligationDayAhead, BenchmarkingProductionObligationDayAheadList, BenchmarkingProductionObligationDayAheadGraphQL, BenchmarkingProductionObligationDayAheadWrite, BenchmarkingProductionObligationDayAheadWriteList
    from cognite.powerops.client._generated.v1.data_classes._power_asset import PowerAsset, PowerAssetList, PowerAssetGraphQL, PowerAssetWrite, PowerAssetWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_commands import ShopCommands, ShopCommandsList, ShopCommandsGraphQL, ShopCommandsWrite, ShopCommandsWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_model import ShopModel, ShopModelList, ShopModelGraphQL, ShopModelWrite, ShopModelWriteList


__all__ = [
    "ShopModelWithAssets",
    "ShopModelWithAssetsWrite",
    "ShopModelWithAssetsList",
    "ShopModelWithAssetsWriteList",
    "ShopModelWithAssetsGraphQL",
]


ShopModelWithAssetsTextFields = Literal["external_id", ]
ShopModelWithAssetsFields = Literal["external_id", ]

_SHOPMODELWITHASSETS_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


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
        power_assets: A list of power assets covered by the Shop model. For a given bid document, we will select the
            partial bids for these assets, and calculate production obligation for these partial bids (summed
            up)
        production_obligations: It is possible to specify time series for production obligation - one benchmarking run
            will be set up for each of these time series. The intended use of this, is to specify
            the production obligation resulting from the submitted bid, or any other bid document
            not modelled within PowerOps
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

    def as_read(self) -> ShopModelWithAssets:
        """Convert this GraphQL format of shop model with asset to the reading format."""
        return ShopModelWithAssets.model_validate(as_read_args(self))

    def as_write(self) -> ShopModelWithAssetsWrite:
        """Convert this GraphQL format of shop model with asset to the writing format."""
        return ShopModelWithAssetsWrite.model_validate(as_write_args(self))


class ShopModelWithAssets(DomainModel):
    """This represents the reading version of shop model with asset.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop model with asset.
        data_record: The data record of the shop model with asset node.
        shop_model: The shop model that includes one water course for one shop run
        shop_commands: Commands for the shop file
        power_assets: A list of power assets covered by the Shop model. For a given bid document, we will select the
            partial bids for these assets, and calculate production obligation for these partial bids (summed
            up)
        production_obligations: It is possible to specify time series for production obligation - one benchmarking run
            will be set up for each of these time series. The intended use of this, is to specify
            the production obligation resulting from the submitted bid, or any other bid document
            not modelled within PowerOps
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopModelWithAssets", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopModelWithAssets")
    shop_model: Union[ShopModel, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopModel")
    shop_commands: Union[ShopCommands, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopCommands")
    power_assets: Optional[list[Union[PowerAsset, str, dm.NodeId]]] = Field(default=None, repr=False, alias="powerAssets")
    production_obligations: Optional[list[Union[BenchmarkingProductionObligationDayAhead, str, dm.NodeId]]] = Field(default=None, repr=False, alias="productionObligations")
    @field_validator("shop_model", "shop_commands", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("power_assets", "production_obligations", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ShopModelWithAssetsWrite:
        """Convert this read version of shop model with asset to the writing version."""
        return ShopModelWithAssetsWrite.model_validate(as_write_args(self))



class ShopModelWithAssetsWrite(DomainModelWrite):
    """This represents the writing version of shop model with asset.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop model with asset.
        data_record: The data record of the shop model with asset node.
        shop_model: The shop model that includes one water course for one shop run
        shop_commands: Commands for the shop file
        power_assets: A list of power assets covered by the Shop model. For a given bid document, we will select the
            partial bids for these assets, and calculate production obligation for these partial bids (summed
            up)
        production_obligations: It is possible to specify time series for production obligation - one benchmarking run
            will be set up for each of these time series. The intended use of this, is to specify
            the production obligation resulting from the submitted bid, or any other bid document
            not modelled within PowerOps
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("shop_commands", "shop_model",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("power_assets", dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets")), ("production_obligations", dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("shop_commands", "shop_model",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopModelWithAssets", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopModelWithAssets")
    shop_model: Union[ShopModelWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopModel")
    shop_commands: Union[ShopCommandsWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopCommands")
    power_assets: Optional[list[Union[PowerAssetWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="powerAssets")
    production_obligations: Optional[list[Union[BenchmarkingProductionObligationDayAheadWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="productionObligations")

    @field_validator("shop_model", "shop_commands", "power_assets", "production_obligations", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ShopModelWithAssetsList(DomainModelList[ShopModelWithAssets]):
    """List of shop model with assets in the read version."""

    _INSTANCE = ShopModelWithAssets
    def as_write(self) -> ShopModelWithAssetsWriteList:
        """Convert these read versions of shop model with asset to the writing versions."""
        return ShopModelWithAssetsWriteList([node.as_write() for node in self.data])


    @property
    def shop_model(self) -> ShopModelList:
        from ._shop_model import ShopModel, ShopModelList
        return ShopModelList([item.shop_model for item in self.data if isinstance(item.shop_model, ShopModel)])
    @property
    def shop_commands(self) -> ShopCommandsList:
        from ._shop_commands import ShopCommands, ShopCommandsList
        return ShopCommandsList([item.shop_commands for item in self.data if isinstance(item.shop_commands, ShopCommands)])
    @property
    def power_assets(self) -> PowerAssetList:
        from ._power_asset import PowerAsset, PowerAssetList
        return PowerAssetList([item for items in self.data for item in items.power_assets or [] if isinstance(item, PowerAsset)])

    @property
    def production_obligations(self) -> BenchmarkingProductionObligationDayAheadList:
        from ._benchmarking_production_obligation_day_ahead import BenchmarkingProductionObligationDayAhead, BenchmarkingProductionObligationDayAheadList
        return BenchmarkingProductionObligationDayAheadList([item for items in self.data for item in items.production_obligations or [] if isinstance(item, BenchmarkingProductionObligationDayAhead)])


class ShopModelWithAssetsWriteList(DomainModelWriteList[ShopModelWithAssetsWrite]):
    """List of shop model with assets in the writing version."""

    _INSTANCE = ShopModelWithAssetsWrite
    @property
    def shop_model(self) -> ShopModelWriteList:
        from ._shop_model import ShopModelWrite, ShopModelWriteList
        return ShopModelWriteList([item.shop_model for item in self.data if isinstance(item.shop_model, ShopModelWrite)])
    @property
    def shop_commands(self) -> ShopCommandsWriteList:
        from ._shop_commands import ShopCommandsWrite, ShopCommandsWriteList
        return ShopCommandsWriteList([item.shop_commands for item in self.data if isinstance(item.shop_commands, ShopCommandsWrite)])
    @property
    def power_assets(self) -> PowerAssetWriteList:
        from ._power_asset import PowerAssetWrite, PowerAssetWriteList
        return PowerAssetWriteList([item for items in self.data for item in items.power_assets or [] if isinstance(item, PowerAssetWrite)])

    @property
    def production_obligations(self) -> BenchmarkingProductionObligationDayAheadWriteList:
        from ._benchmarking_production_obligation_day_ahead import BenchmarkingProductionObligationDayAheadWrite, BenchmarkingProductionObligationDayAheadWriteList
        return BenchmarkingProductionObligationDayAheadWriteList([item for items in self.data for item in items.production_obligations or [] if isinstance(item, BenchmarkingProductionObligationDayAheadWrite)])



def _create_shop_model_with_asset_filter(
    view_id: dm.ViewId,
    shop_model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    shop_commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(shop_model, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(shop_model):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopModel"), value=as_instance_dict_id(shop_model)))
    if shop_model and isinstance(shop_model, Sequence) and not isinstance(shop_model, str) and not is_tuple_id(shop_model):
        filters.append(dm.filters.In(view_id.as_property_ref("shopModel"), values=[as_instance_dict_id(item) for item in shop_model]))
    if isinstance(shop_commands, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(shop_commands):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopCommands"), value=as_instance_dict_id(shop_commands)))
    if shop_commands and isinstance(shop_commands, Sequence) and not isinstance(shop_commands, str) and not is_tuple_id(shop_commands):
        filters.append(dm.filters.In(view_id.as_property_ref("shopCommands"), values=[as_instance_dict_id(item) for item in shop_commands]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopModelWithAssetsQuery(NodeQueryCore[T_DomainModelList, ShopModelWithAssetsList]):
    _view_id = ShopModelWithAssets._view_id
    _result_cls = ShopModelWithAssets
    _result_list_cls_end = ShopModelWithAssetsList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
    ):
        from ._benchmarking_production_obligation_day_ahead import _BenchmarkingProductionObligationDayAheadQuery
        from ._power_asset import _PowerAssetQuery
        from ._shop_commands import _ShopCommandsQuery
        from ._shop_model import _ShopModelQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _ShopModelQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.shop_model = _ShopModelQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("shopModel"),
                    direction="outwards",
                ),
                connection_name="shop_model",
                connection_property=ViewPropertyId(self._view_id, "shopModel"),
            )

        if _ShopCommandsQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.shop_commands = _ShopCommandsQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("shopCommands"),
                    direction="outwards",
                ),
                connection_name="shop_commands",
                connection_property=ViewPropertyId(self._view_id, "shopCommands"),
            )

        if _PowerAssetQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.power_assets = _PowerAssetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="power_assets",
                connection_property=ViewPropertyId(self._view_id, "powerAssets"),
            )

        if _BenchmarkingProductionObligationDayAheadQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.production_obligations = _BenchmarkingProductionObligationDayAheadQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="production_obligations",
                connection_property=ViewPropertyId(self._view_id, "productionObligations"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.shop_model_filter = DirectRelationFilter(self, self._view_id.as_property_ref("shopModel"))
        self.shop_commands_filter = DirectRelationFilter(self, self._view_id.as_property_ref("shopCommands"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.shop_model_filter,
            self.shop_commands_filter,
        ])

    def list_shop_model_with_asset(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopModelWithAssetsList:
        return self._list(limit=limit)


class ShopModelWithAssetsQuery(_ShopModelWithAssetsQuery[ShopModelWithAssetsList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopModelWithAssetsList)
