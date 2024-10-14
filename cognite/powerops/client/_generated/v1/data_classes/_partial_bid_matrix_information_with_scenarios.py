from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

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
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)
from ._partial_bid_matrix_information import PartialBidMatrixInformation, PartialBidMatrixInformationWrite

if TYPE_CHECKING:
    from ._alert import Alert, AlertGraphQL, AlertWrite
    from ._bid_matrix import BidMatrix, BidMatrixGraphQL, BidMatrixWrite
    from ._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationGraphQL, PartialBidConfigurationWrite
    from ._power_asset import PowerAsset, PowerAssetGraphQL, PowerAssetWrite
    from ._price_production import PriceProduction, PriceProductionGraphQL, PriceProductionWrite


__all__ = [
    "PartialBidMatrixInformationWithScenarios",
    "PartialBidMatrixInformationWithScenariosWrite",
    "PartialBidMatrixInformationWithScenariosApply",
    "PartialBidMatrixInformationWithScenariosList",
    "PartialBidMatrixInformationWithScenariosWriteList",
    "PartialBidMatrixInformationWithScenariosApplyList",
    "PartialBidMatrixInformationWithScenariosFields",
    "PartialBidMatrixInformationWithScenariosTextFields",
    "PartialBidMatrixInformationWithScenariosGraphQL",
]


PartialBidMatrixInformationWithScenariosTextFields = Literal["state", "bid_matrix", "linked_time_series"]
PartialBidMatrixInformationWithScenariosFields = Literal["state", "bid_matrix", "linked_time_series", "resource_cost"]

_PARTIALBIDMATRIXINFORMATIONWITHSCENARIOS_PROPERTIES_BY_FIELD = {
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
    bid_matrix: Union[dict, None] = Field(None, alias="bidMatrix")
    linked_time_series: Union[list[TimeSeries], list[dict], None] = Field(None, alias="linkedTimeSeries")
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> PartialBidMatrixInformationWithScenarios:
        """Convert this GraphQL format of partial bid matrix information with scenario to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PartialBidMatrixInformationWithScenarios(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            state=self.state,
            bid_matrix=self.bid_matrix["externalId"] if self.bid_matrix and "externalId" in self.bid_matrix else None,
            linked_time_series=self.linked_time_series,
            alerts=[alert.as_read() for alert in self.alerts or []],
            underlying_bid_matrices=[underlying_bid_matrice.as_read() for underlying_bid_matrice in self.underlying_bid_matrices or []],
            power_asset=self.power_asset.as_read() if isinstance(self.power_asset, GraphQLCore) else self.power_asset,
            resource_cost=self.resource_cost,
            partial_bid_configuration=self.partial_bid_configuration.as_read() if isinstance(self.partial_bid_configuration, GraphQLCore) else self.partial_bid_configuration,
            multi_scenario_input=[multi_scenario_input.as_read() for multi_scenario_input in self.multi_scenario_input or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PartialBidMatrixInformationWithScenariosWrite:
        """Convert this GraphQL format of partial bid matrix information with scenario to the writing format."""
        return PartialBidMatrixInformationWithScenariosWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            state=self.state,
            bid_matrix=self.bid_matrix["externalId"] if self.bid_matrix and "externalId" in self.bid_matrix else None,
            linked_time_series=self.linked_time_series,
            alerts=[alert.as_write() for alert in self.alerts or []],
            underlying_bid_matrices=[underlying_bid_matrice.as_write() for underlying_bid_matrice in self.underlying_bid_matrices or []],
            power_asset=self.power_asset.as_write() if isinstance(self.power_asset, GraphQLCore) else self.power_asset,
            resource_cost=self.resource_cost,
            partial_bid_configuration=self.partial_bid_configuration.as_write() if isinstance(self.partial_bid_configuration, GraphQLCore) else self.partial_bid_configuration,
            multi_scenario_input=[multi_scenario_input.as_write() for multi_scenario_input in self.multi_scenario_input or []],
        )


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

    def as_write(self) -> PartialBidMatrixInformationWithScenariosWrite:
        """Convert this read version of partial bid matrix information with scenario to the writing version."""
        return PartialBidMatrixInformationWithScenariosWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            state=self.state,
            bid_matrix=self.bid_matrix,
            linked_time_series=self.linked_time_series,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            underlying_bid_matrices=[underlying_bid_matrice.as_write() if isinstance(underlying_bid_matrice, DomainModel) else underlying_bid_matrice for underlying_bid_matrice in self.underlying_bid_matrices or []],
            power_asset=self.power_asset.as_write() if isinstance(self.power_asset, DomainModel) else self.power_asset,
            resource_cost=self.resource_cost,
            partial_bid_configuration=self.partial_bid_configuration.as_write() if isinstance(self.partial_bid_configuration, DomainModel) else self.partial_bid_configuration,
            multi_scenario_input=[multi_scenario_input.as_write() if isinstance(multi_scenario_input, DomainModel) else multi_scenario_input for multi_scenario_input in self.multi_scenario_input or []],
        )

    def as_apply(self) -> PartialBidMatrixInformationWithScenariosWrite:
        """Convert this read version of partial bid matrix information with scenario to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PartialBidMatrixInformationWithScenarios", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    multi_scenario_input: Optional[list[Union[PriceProductionWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="multiScenarioInput")

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

        if self.state is not None:
            properties["state"] = self.state

        if self.bid_matrix is not None or write_none:
            properties["bidMatrix"] = self.bid_matrix

        if self.linked_time_series is not None or write_none:
            properties["linkedTimeSeries"] = [value if isinstance(value, str) else value.external_id for value in self.linked_time_series or []] or None

        if self.power_asset is not None:
            properties["powerAsset"] = {
                "space":  self.space if isinstance(self.power_asset, str) else self.power_asset.space,
                "externalId": self.power_asset if isinstance(self.power_asset, str) else self.power_asset.external_id,
            }

        if self.resource_cost is not None or write_none:
            properties["resourceCost"] = self.resource_cost

        if self.partial_bid_configuration is not None:
            properties["partialBidConfiguration"] = {
                "space":  self.space if isinstance(self.partial_bid_configuration, str) else self.partial_bid_configuration.space,
                "externalId": self.partial_bid_configuration if isinstance(self.partial_bid_configuration, str) else self.partial_bid_configuration.external_id,
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



        edge_type = dm.DirectRelationReference("power_ops_types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=alert,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power_ops_types", "intermediateBidMatrix")
        for underlying_bid_matrice in self.underlying_bid_matrices or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=underlying_bid_matrice,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power_ops_types", "calculationIssue")
        for multi_scenario_input in self.multi_scenario_input or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=multi_scenario_input,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.power_asset, DomainModelWrite):
            other_resources = self.power_asset._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.partial_bid_configuration, DomainModelWrite):
            other_resources = self.partial_bid_configuration._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.linked_time_series, CogniteTimeSeries):
            resources.time_series.append(self.linked_time_series)

        return resources


class PartialBidMatrixInformationWithScenariosApply(PartialBidMatrixInformationWithScenariosWrite):
    def __new__(cls, *args, **kwargs) -> PartialBidMatrixInformationWithScenariosApply:
        warnings.warn(
            "PartialBidMatrixInformationWithScenariosApply is deprecated and will be removed in v1.0. Use PartialBidMatrixInformationWithScenariosWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PartialBidMatrixInformationWithScenarios.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PartialBidMatrixInformationWithScenariosList(DomainModelList[PartialBidMatrixInformationWithScenarios]):
    """List of partial bid matrix information with scenarios in the read version."""

    _INSTANCE = PartialBidMatrixInformationWithScenarios

    def as_write(self) -> PartialBidMatrixInformationWithScenariosWriteList:
        """Convert these read versions of partial bid matrix information with scenario to the writing versions."""
        return PartialBidMatrixInformationWithScenariosWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PartialBidMatrixInformationWithScenariosWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PartialBidMatrixInformationWithScenariosWriteList(DomainModelWriteList[PartialBidMatrixInformationWithScenariosWrite]):
    """List of partial bid matrix information with scenarios in the writing version."""

    _INSTANCE = PartialBidMatrixInformationWithScenariosWrite

class PartialBidMatrixInformationWithScenariosApplyList(PartialBidMatrixInformationWithScenariosWriteList): ...



def _create_partial_bid_matrix_information_with_scenario_filter(
    view_id: dm.ViewId,
    state: str | list[str] | None = None,
    state_prefix: str | None = None,
    power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_resource_cost: float | None = None,
    max_resource_cost: float | None = None,
    partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if power_asset and isinstance(power_asset, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("powerAsset"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": power_asset}))
    if power_asset and isinstance(power_asset, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("powerAsset"), value={"space": power_asset[0], "externalId": power_asset[1]}))
    if power_asset and isinstance(power_asset, list) and isinstance(power_asset[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("powerAsset"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in power_asset]))
    if power_asset and isinstance(power_asset, list) and isinstance(power_asset[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("powerAsset"), values=[{"space": item[0], "externalId": item[1]} for item in power_asset]))
    if min_resource_cost is not None or max_resource_cost is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("resourceCost"), gte=min_resource_cost, lte=max_resource_cost))
    if partial_bid_configuration and isinstance(partial_bid_configuration, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("partialBidConfiguration"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": partial_bid_configuration}))
    if partial_bid_configuration and isinstance(partial_bid_configuration, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("partialBidConfiguration"), value={"space": partial_bid_configuration[0], "externalId": partial_bid_configuration[1]}))
    if partial_bid_configuration and isinstance(partial_bid_configuration, list) and isinstance(partial_bid_configuration[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("partialBidConfiguration"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in partial_bid_configuration]))
    if partial_bid_configuration and isinstance(partial_bid_configuration, list) and isinstance(partial_bid_configuration[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("partialBidConfiguration"), values=[{"space": item[0], "externalId": item[1]} for item in partial_bid_configuration]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
