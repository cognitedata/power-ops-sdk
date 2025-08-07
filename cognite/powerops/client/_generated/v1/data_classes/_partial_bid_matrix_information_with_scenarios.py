from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
    Sequence as CogniteSequence,
    SequenceWrite as CogniteSequenceWrite,
)
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
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
    TimeSeries,
    TimeSeriesWrite,
    TimeSeriesGraphQL,
    TimeSeriesReferenceAPI,
    SequenceRead,
    SequenceWrite,
    SequenceGraphQL,
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
    FloatFilter,
)
from cognite.powerops.client._generated.v1.data_classes._partial_bid_matrix_information import PartialBidMatrixInformation, PartialBidMatrixInformationWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._bid_matrix import BidMatrix, BidMatrixList, BidMatrixGraphQL, BidMatrixWrite, BidMatrixWriteList
    from cognite.powerops.client._generated.v1.data_classes._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationList, PartialBidConfigurationGraphQL, PartialBidConfigurationWrite, PartialBidConfigurationWriteList
    from cognite.powerops.client._generated.v1.data_classes._power_asset import PowerAsset, PowerAssetList, PowerAssetGraphQL, PowerAssetWrite, PowerAssetWriteList
    from cognite.powerops.client._generated.v1.data_classes._price_production import PriceProduction, PriceProductionList, PriceProductionGraphQL, PriceProductionWrite, PriceProductionWriteList


__all__ = [
    "PartialBidMatrixInformationWithScenarios",
    "PartialBidMatrixInformationWithScenariosWrite",
    "PartialBidMatrixInformationWithScenariosList",
    "PartialBidMatrixInformationWithScenariosWriteList",
    "PartialBidMatrixInformationWithScenariosFields",
    "PartialBidMatrixInformationWithScenariosTextFields",
    "PartialBidMatrixInformationWithScenariosGraphQL",
]


PartialBidMatrixInformationWithScenariosTextFields = Literal["external_id", "state", "bid_matrix", "linked_time_series"]
PartialBidMatrixInformationWithScenariosFields = Literal["external_id", "state", "bid_matrix", "linked_time_series", "resource_cost"]

_PARTIALBIDMATRIXINFORMATIONWITHSCENARIOS_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "state": "state",
    "bid_matrix": "bidMatrix",
    "linked_time_series": "linkedTimeSeries",
    "resource_cost": "resourceCost",
}


class PartialBidMatrixInformationWithScenariosGraphQL(GraphQLCore):
    """This represents the reading version of partial bid matrix information with scenario, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial bid matrix information with scenario.
        data_record: The data record of the partial bid matrix information with scenario node.
        state: The state field.
        bid_matrix: The bid matrix field.
        linked_time_series: The linked time series field.
        alerts: An array of calculation level Alerts.
        underlying_bid_matrices: An array of intermediate BidMatrices.
        power_asset: The power asset field.
        resource_cost: The resource cost field.
        partial_bid_configuration: The partial bid configuration field.
        multi_scenario_input: TODO
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PartialBidMatrixInformationWithScenarios", "1")
    state: Optional[str] = None
    bid_matrix: Optional[SequenceGraphQL] = Field(None, alias="bidMatrix")
    linked_time_series: Optional[list[TimeSeriesGraphQL]] = Field(None, alias="linkedTimeSeries")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    underlying_bid_matrices: Optional[list[BidMatrixGraphQL]] = Field(default=None, repr=False, alias="underlyingBidMatrices")
    power_asset: Optional[PowerAssetGraphQL] = Field(default=None, repr=False, alias="powerAsset")
    resource_cost: Optional[float] = Field(None, alias="resourceCost")
    partial_bid_configuration: Optional[PartialBidConfigurationGraphQL] = Field(default=None, repr=False, alias="partialBidConfiguration")
    multi_scenario_input: Optional[list[PriceProductionGraphQL]] = Field(default=None, repr=False, alias="multiScenarioInput")

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

    @field_validator("linked_time_series", mode="before")
    def clean_list(cls, value: Any) -> Any:
        if isinstance(value, list):
            return [v for v in value if v is not None] or None
        return value

    @field_validator("alerts", "underlying_bid_matrices", "power_asset", "partial_bid_configuration", "multi_scenario_input", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> PartialBidMatrixInformationWithScenarios:
        """Convert this GraphQL format of partial bid matrix information with scenario to the reading format."""
        return PartialBidMatrixInformationWithScenarios.model_validate(as_read_args(self))

    def as_write(self) -> PartialBidMatrixInformationWithScenariosWrite:
        """Convert this GraphQL format of partial bid matrix information with scenario to the writing format."""
        return PartialBidMatrixInformationWithScenariosWrite.model_validate(as_write_args(self))


class PartialBidMatrixInformationWithScenarios(PartialBidMatrixInformation):
    """This represents the reading version of partial bid matrix information with scenario.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial bid matrix information with scenario.
        data_record: The data record of the partial bid matrix information with scenario node.
        state: The state field.
        bid_matrix: The bid matrix field.
        linked_time_series: The linked time series field.
        alerts: An array of calculation level Alerts.
        underlying_bid_matrices: An array of intermediate BidMatrices.
        power_asset: The power asset field.
        resource_cost: The resource cost field.
        partial_bid_configuration: The partial bid configuration field.
        multi_scenario_input: TODO
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PartialBidMatrixInformationWithScenarios", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    multi_scenario_input: Optional[list[Union[PriceProduction, str, dm.NodeId]]] = Field(default=None, repr=False, alias="multiScenarioInput")
    @field_validator("power_asset", "partial_bid_configuration", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("alerts", "underlying_bid_matrices", "multi_scenario_input", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> PartialBidMatrixInformationWithScenariosWrite:
        """Convert this read version of partial bid matrix information with scenario to the writing version."""
        return PartialBidMatrixInformationWithScenariosWrite.model_validate(as_write_args(self))



class PartialBidMatrixInformationWithScenariosWrite(PartialBidMatrixInformationWrite):
    """This represents the writing version of partial bid matrix information with scenario.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial bid matrix information with scenario.
        data_record: The data record of the partial bid matrix information with scenario node.
        state: The state field.
        bid_matrix: The bid matrix field.
        linked_time_series: The linked time series field.
        alerts: An array of calculation level Alerts.
        underlying_bid_matrices: An array of intermediate BidMatrices.
        power_asset: The power asset field.
        resource_cost: The resource cost field.
        partial_bid_configuration: The partial bid configuration field.
        multi_scenario_input: TODO
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("bid_matrix", "linked_time_series", "partial_bid_configuration", "power_asset", "resource_cost", "state",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("alerts", dm.DirectRelationReference("power_ops_types", "calculationIssue")), ("multi_scenario_input", dm.DirectRelationReference("power_ops_types", "calculationIssue")), ("underlying_bid_matrices", dm.DirectRelationReference("power_ops_types", "intermediateBidMatrix")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("partial_bid_configuration", "power_asset",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PartialBidMatrixInformationWithScenarios", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    multi_scenario_input: Optional[list[Union[PriceProductionWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="multiScenarioInput")

    @field_validator("multi_scenario_input", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class PartialBidMatrixInformationWithScenariosList(DomainModelList[PartialBidMatrixInformationWithScenarios]):
    """List of partial bid matrix information with scenarios in the read version."""

    _INSTANCE = PartialBidMatrixInformationWithScenarios
    def as_write(self) -> PartialBidMatrixInformationWithScenariosWriteList:
        """Convert these read versions of partial bid matrix information with scenario to the writing versions."""
        return PartialBidMatrixInformationWithScenariosWriteList([node.as_write() for node in self.data])


    @property
    def alerts(self) -> AlertList:
        from ._alert import Alert, AlertList
        return AlertList([item for items in self.data for item in items.alerts or [] if isinstance(item, Alert)])

    @property
    def underlying_bid_matrices(self) -> BidMatrixList:
        from ._bid_matrix import BidMatrix, BidMatrixList
        return BidMatrixList([item for items in self.data for item in items.underlying_bid_matrices or [] if isinstance(item, BidMatrix)])

    @property
    def power_asset(self) -> PowerAssetList:
        from ._power_asset import PowerAsset, PowerAssetList
        return PowerAssetList([item.power_asset for item in self.data if isinstance(item.power_asset, PowerAsset)])
    @property
    def partial_bid_configuration(self) -> PartialBidConfigurationList:
        from ._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationList
        return PartialBidConfigurationList([item.partial_bid_configuration for item in self.data if isinstance(item.partial_bid_configuration, PartialBidConfiguration)])
    @property
    def multi_scenario_input(self) -> PriceProductionList:
        from ._price_production import PriceProduction, PriceProductionList
        return PriceProductionList([item for items in self.data for item in items.multi_scenario_input or [] if isinstance(item, PriceProduction)])


class PartialBidMatrixInformationWithScenariosWriteList(DomainModelWriteList[PartialBidMatrixInformationWithScenariosWrite]):
    """List of partial bid matrix information with scenarios in the writing version."""

    _INSTANCE = PartialBidMatrixInformationWithScenariosWrite
    @property
    def alerts(self) -> AlertWriteList:
        from ._alert import AlertWrite, AlertWriteList
        return AlertWriteList([item for items in self.data for item in items.alerts or [] if isinstance(item, AlertWrite)])

    @property
    def underlying_bid_matrices(self) -> BidMatrixWriteList:
        from ._bid_matrix import BidMatrixWrite, BidMatrixWriteList
        return BidMatrixWriteList([item for items in self.data for item in items.underlying_bid_matrices or [] if isinstance(item, BidMatrixWrite)])

    @property
    def power_asset(self) -> PowerAssetWriteList:
        from ._power_asset import PowerAssetWrite, PowerAssetWriteList
        return PowerAssetWriteList([item.power_asset for item in self.data if isinstance(item.power_asset, PowerAssetWrite)])
    @property
    def partial_bid_configuration(self) -> PartialBidConfigurationWriteList:
        from ._partial_bid_configuration import PartialBidConfigurationWrite, PartialBidConfigurationWriteList
        return PartialBidConfigurationWriteList([item.partial_bid_configuration for item in self.data if isinstance(item.partial_bid_configuration, PartialBidConfigurationWrite)])
    @property
    def multi_scenario_input(self) -> PriceProductionWriteList:
        from ._price_production import PriceProductionWrite, PriceProductionWriteList
        return PriceProductionWriteList([item for items in self.data for item in items.multi_scenario_input or [] if isinstance(item, PriceProductionWrite)])



def _create_partial_bid_matrix_information_with_scenario_filter(
    view_id: dm.ViewId,
    state: str | list[str] | None = None,
    state_prefix: str | None = None,
    power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    min_resource_cost: float | None = None,
    max_resource_cost: float | None = None,
    partial_bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(state, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("state"), value=state))
    if state and isinstance(state, list):
        filters.append(dm.filters.In(view_id.as_property_ref("state"), values=state))
    if state_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("state"), value=state_prefix))
    if isinstance(power_asset, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(power_asset):
        filters.append(dm.filters.Equals(view_id.as_property_ref("powerAsset"), value=as_instance_dict_id(power_asset)))
    if power_asset and isinstance(power_asset, Sequence) and not isinstance(power_asset, str) and not is_tuple_id(power_asset):
        filters.append(dm.filters.In(view_id.as_property_ref("powerAsset"), values=[as_instance_dict_id(item) for item in power_asset]))
    if min_resource_cost is not None or max_resource_cost is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("resourceCost"), gte=min_resource_cost, lte=max_resource_cost))
    if isinstance(partial_bid_configuration, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(partial_bid_configuration):
        filters.append(dm.filters.Equals(view_id.as_property_ref("partialBidConfiguration"), value=as_instance_dict_id(partial_bid_configuration)))
    if partial_bid_configuration and isinstance(partial_bid_configuration, Sequence) and not isinstance(partial_bid_configuration, str) and not is_tuple_id(partial_bid_configuration):
        filters.append(dm.filters.In(view_id.as_property_ref("partialBidConfiguration"), values=[as_instance_dict_id(item) for item in partial_bid_configuration]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _PartialBidMatrixInformationWithScenariosQuery(NodeQueryCore[T_DomainModelList, PartialBidMatrixInformationWithScenariosList]):
    _view_id = PartialBidMatrixInformationWithScenarios._view_id
    _result_cls = PartialBidMatrixInformationWithScenarios
    _result_list_cls_end = PartialBidMatrixInformationWithScenariosList

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
        from ._alert import _AlertQuery
        from ._bid_matrix import _BidMatrixQuery
        from ._partial_bid_configuration import _PartialBidConfigurationQuery
        from ._power_asset import _PowerAssetQuery
        from ._price_production import _PriceProductionQuery

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

        if _AlertQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.alerts = _AlertQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="alerts",
                connection_property=ViewPropertyId(self._view_id, "alerts"),
            )

        if _BidMatrixQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.underlying_bid_matrices = _BidMatrixQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="underlying_bid_matrices",
                connection_property=ViewPropertyId(self._view_id, "underlyingBidMatrices"),
            )

        if _PowerAssetQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.power_asset = _PowerAssetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("powerAsset"),
                    direction="outwards",
                ),
                connection_name="power_asset",
                connection_property=ViewPropertyId(self._view_id, "powerAsset"),
            )

        if _PartialBidConfigurationQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.partial_bid_configuration = _PartialBidConfigurationQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("partialBidConfiguration"),
                    direction="outwards",
                ),
                connection_name="partial_bid_configuration",
                connection_property=ViewPropertyId(self._view_id, "partialBidConfiguration"),
            )

        if _PriceProductionQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.multi_scenario_input = _PriceProductionQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="multi_scenario_input",
                connection_property=ViewPropertyId(self._view_id, "multiScenarioInput"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.state = StringFilter(self, self._view_id.as_property_ref("state"))
        self.power_asset_filter = DirectRelationFilter(self, self._view_id.as_property_ref("powerAsset"))
        self.resource_cost = FloatFilter(self, self._view_id.as_property_ref("resourceCost"))
        self.partial_bid_configuration_filter = DirectRelationFilter(self, self._view_id.as_property_ref("partialBidConfiguration"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.state,
            self.power_asset_filter,
            self.resource_cost,
            self.partial_bid_configuration_filter,
        ])
        self.linked_time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            ts if isinstance(ts, str) else ts.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.linked_time_series is not None
            for ts in item.linked_time_series
            if ts is not None and
               (isinstance(ts, str) or ts.external_id is not None)
        ])

    def list_partial_bid_matrix_information_with_scenario(self, limit: int = DEFAULT_QUERY_LIMIT) -> PartialBidMatrixInformationWithScenariosList:
        return self._list(limit=limit)


class PartialBidMatrixInformationWithScenariosQuery(_PartialBidMatrixInformationWithScenariosQuery[PartialBidMatrixInformationWithScenariosList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PartialBidMatrixInformationWithScenariosList)
