from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._benchmarking_production_obligation_day_ahead import BenchmarkingProductionObligationDayAhead, BenchmarkingProductionObligationDayAheadList, BenchmarkingProductionObligationDayAheadGraphQL, BenchmarkingProductionObligationDayAheadWrite, BenchmarkingProductionObligationDayAheadWriteList
    from cognite.powerops.client._generated.v1.data_classes._power_asset import PowerAsset, PowerAssetList, PowerAssetGraphQL, PowerAssetWrite, PowerAssetWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_commands import ShopCommands, ShopCommandsList, ShopCommandsGraphQL, ShopCommandsWrite, ShopCommandsWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_model import ShopModel, ShopModelList, ShopModelGraphQL, ShopModelWrite, ShopModelWriteList


__all__ = [
    "ShopModelWithAssets",
    "ShopModelWithAssetsWrite",
    "ShopModelWithAssetsApply",
    "ShopModelWithAssetsList",
    "ShopModelWithAssetsWriteList",
    "ShopModelWithAssetsApplyList",
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
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            shop_model=self.shop_model.as_read()
if isinstance(self.shop_model, GraphQLCore)
else self.shop_model,
            shop_commands=self.shop_commands.as_read()
if isinstance(self.shop_commands, GraphQLCore)
else self.shop_commands,
            power_assets=[power_asset.as_read() for power_asset in self.power_assets] if self.power_assets is not None else None,
            production_obligations=[production_obligation.as_read() for production_obligation in self.production_obligations] if self.production_obligations is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopModelWithAssetsWrite:
        """Convert this GraphQL format of shop model with asset to the writing format."""
        return ShopModelWithAssetsWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            shop_model=self.shop_model.as_write()
if isinstance(self.shop_model, GraphQLCore)
else self.shop_model,
            shop_commands=self.shop_commands.as_write()
if isinstance(self.shop_commands, GraphQLCore)
else self.shop_commands,
            power_assets=[power_asset.as_write() for power_asset in self.power_assets] if self.power_assets is not None else None,
            production_obligations=[production_obligation.as_write() for production_obligation in self.production_obligations] if self.production_obligations is not None else None,
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopModelWithAssetsWrite:
        """Convert this read version of shop model with asset to the writing version."""
        return ShopModelWithAssetsWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            shop_model=self.shop_model.as_write()
if isinstance(self.shop_model, DomainModel)
else self.shop_model,
            shop_commands=self.shop_commands.as_write()
if isinstance(self.shop_commands, DomainModel)
else self.shop_commands,
            power_assets=[power_asset.as_write() if isinstance(power_asset, DomainModel) else power_asset for power_asset in self.power_assets] if self.power_assets is not None else None,
            production_obligations=[production_obligation.as_write() if isinstance(production_obligation, DomainModel) else production_obligation for production_obligation in self.production_obligations] if self.production_obligations is not None else None,
        )

    def as_apply(self) -> ShopModelWithAssetsWrite:
        """Convert this read version of shop model with asset to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ShopModelWithAssets],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._benchmarking_production_obligation_day_ahead import BenchmarkingProductionObligationDayAhead
        from ._power_asset import PowerAsset
        from ._shop_commands import ShopCommands
        from ._shop_model import ShopModel
        for instance in instances.values():
            if isinstance(instance.shop_model, (dm.NodeId, str)) and (shop_model := nodes_by_id.get(instance.shop_model)) and isinstance(
                    shop_model, ShopModel
            ):
                instance.shop_model = shop_model
            if isinstance(instance.shop_commands, (dm.NodeId, str)) and (shop_commands := nodes_by_id.get(instance.shop_commands)) and isinstance(
                    shop_commands, ShopCommands
            ):
                instance.shop_commands = shop_commands
            if edges := edges_by_source_node.get(instance.as_id()):
                power_assets: list[PowerAsset | str | dm.NodeId] = []
                production_obligations: list[BenchmarkingProductionObligationDayAhead | str | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets") and isinstance(
                        value, (PowerAsset, str, dm.NodeId)
                    ):
                        power_assets.append(value)
                    if edge_type == dm.DirectRelationReference("power_ops_core", "ShopModelWithAssets") and isinstance(
                        value, (BenchmarkingProductionObligationDayAhead, str, dm.NodeId)
                    ):
                        production_obligations.append(value)

                instance.power_assets = power_assets or None
                instance.production_obligations = production_obligations or None



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
                type=as_direct_relation_reference(self.node_type),
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


class ShopModelWithAssetsApplyList(ShopModelWithAssetsWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _ShopModelQuery not in created_types:
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
            )

        if _ShopCommandsQuery not in created_types:
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
            )

        if _PowerAssetQuery not in created_types:
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
            )

        if _BenchmarkingProductionObligationDayAheadQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])

    def list_shop_model_with_asset(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopModelWithAssetsList:
        return self._list(limit=limit)


class ShopModelWithAssetsQuery(_ShopModelWithAssetsQuery[ShopModelWithAssetsList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopModelWithAssetsList)
