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
from ._bid_matrix import BidMatrix, BidMatrixWrite

if TYPE_CHECKING:
    from ._alert import Alert, AlertGraphQL, AlertWrite
    from ._bid_method import BidMethod, BidMethodGraphQL, BidMethodWrite
    from ._shop_price_scenario_result import (
        SHOPPriceScenarioResult,
        SHOPPriceScenarioResultGraphQL,
        SHOPPriceScenarioResultWrite,
    )


__all__ = [
    "MultiScenarioMatrix",
    "MultiScenarioMatrixWrite",
    "MultiScenarioMatrixApply",
    "MultiScenarioMatrixList",
    "MultiScenarioMatrixWriteList",
    "MultiScenarioMatrixApplyList",
    "MultiScenarioMatrixFields",
    "MultiScenarioMatrixTextFields",
]


MultiScenarioMatrixTextFields = Literal["resource_cost", "matrix", "asset_type", "asset_id"]
MultiScenarioMatrixFields = Literal["resource_cost", "matrix", "asset_type", "asset_id"]

_MULTISCENARIOMATRIX_PROPERTIES_BY_FIELD = {
    "resource_cost": "resourceCost",
    "matrix": "matrix",
    "asset_type": "assetType",
    "asset_id": "assetId",
}


class MultiScenarioMatrixGraphQL(GraphQLCore):
    """This represents the reading version of multi scenario matrix, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the multi scenario matrix.
        data_record: The data record of the multi scenario matrix node.
        resource_cost: The resource cost field.
        matrix: The matrix field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        method: The method field.
        alerts: The alert field.
        scenario_results: An array of results, one for each scenario.
    """

    view_id = dm.ViewId("power-ops-day-ahead-bid", "MultiScenarioMatrix", "1")
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    matrix: Union[dict, None] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = Field(None, alias="assetId")
    method: Optional[BidMethodGraphQL] = Field(None, repr=False)
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    scenario_results: Optional[list[SHOPPriceScenarioResultGraphQL]] = Field(
        default=None, repr=False, alias="scenarioResults"
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

    @field_validator("method", "alerts", "scenario_results", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> MultiScenarioMatrix:
        """Convert this GraphQL format of multi scenario matrix to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return MultiScenarioMatrix(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            resource_cost=self.resource_cost,
            matrix=self.matrix["externalId"] if self.matrix and "externalId" in self.matrix else None,
            asset_type=self.asset_type,
            asset_id=self.asset_id,
            method=self.method.as_read() if isinstance(self.method, GraphQLCore) else self.method,
            alerts=[alert.as_read() if isinstance(alert, GraphQLCore) else alert for alert in self.alerts or []],
            scenario_results=[
                scenario_result.as_read() if isinstance(scenario_result, GraphQLCore) else scenario_result
                for scenario_result in self.scenario_results or []
            ],
        )

    def as_write(self) -> MultiScenarioMatrixWrite:
        """Convert this GraphQL format of multi scenario matrix to the writing format."""
        return MultiScenarioMatrixWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            resource_cost=self.resource_cost,
            matrix=self.matrix["externalId"] if self.matrix and "externalId" in self.matrix else None,
            asset_type=self.asset_type,
            asset_id=self.asset_id,
            method=self.method.as_write() if isinstance(self.method, DomainModel) else self.method,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            scenario_results=[
                scenario_result.as_write() if isinstance(scenario_result, DomainModel) else scenario_result
                for scenario_result in self.scenario_results or []
            ],
        )


class MultiScenarioMatrix(BidMatrix):
    """This represents the reading version of multi scenario matrix.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the multi scenario matrix.
        data_record: The data record of the multi scenario matrix node.
        resource_cost: The resource cost field.
        matrix: The matrix field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        method: The method field.
        alerts: The alert field.
        scenario_results: An array of results, one for each scenario.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "DayAheadMultiScenarioMatrix"
    )
    scenario_results: Union[list[SHOPPriceScenarioResult], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="scenarioResults"
    )

    def as_write(self) -> MultiScenarioMatrixWrite:
        """Convert this read version of multi scenario matrix to the writing version."""
        return MultiScenarioMatrixWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            resource_cost=self.resource_cost,
            matrix=self.matrix,
            asset_type=self.asset_type,
            asset_id=self.asset_id,
            method=self.method.as_write() if isinstance(self.method, DomainModel) else self.method,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            scenario_results=[
                scenario_result.as_write() if isinstance(scenario_result, DomainModel) else scenario_result
                for scenario_result in self.scenario_results or []
            ],
        )

    def as_apply(self) -> MultiScenarioMatrixWrite:
        """Convert this read version of multi scenario matrix to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class MultiScenarioMatrixWrite(BidMatrixWrite):
    """This represents the writing version of multi scenario matrix.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the multi scenario matrix.
        data_record: The data record of the multi scenario matrix node.
        resource_cost: The resource cost field.
        matrix: The matrix field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        method: The method field.
        alerts: The alert field.
        scenario_results: An array of results, one for each scenario.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "DayAheadMultiScenarioMatrix"
    )
    scenario_results: Union[list[SHOPPriceScenarioResultWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="scenarioResults"
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
            MultiScenarioMatrix, dm.ViewId("power-ops-day-ahead-bid", "MultiScenarioMatrix", "1")
        )

        properties: dict[str, Any] = {}

        if self.resource_cost is not None or write_none:
            properties["resourceCost"] = self.resource_cost

        if self.matrix is not None or write_none:
            properties["matrix"] = self.matrix

        if self.asset_type is not None or write_none:
            properties["assetType"] = self.asset_type

        if self.asset_id is not None or write_none:
            properties["assetId"] = self.asset_id

        if self.method is not None:
            properties["method"] = {
                "space": self.space if isinstance(self.method, str) else self.method.space,
                "externalId": self.method if isinstance(self.method, str) else self.method.external_id,
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

        edge_type = dm.DirectRelationReference("power-ops-types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=alert,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power-ops-types", "scenarioResult")
        for scenario_result in self.scenario_results or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=scenario_result,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.method, DomainModelWrite):
            other_resources = self.method._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class MultiScenarioMatrixApply(MultiScenarioMatrixWrite):
    def __new__(cls, *args, **kwargs) -> MultiScenarioMatrixApply:
        warnings.warn(
            "MultiScenarioMatrixApply is deprecated and will be removed in v1.0. Use MultiScenarioMatrixWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "MultiScenarioMatrix.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class MultiScenarioMatrixList(DomainModelList[MultiScenarioMatrix]):
    """List of multi scenario matrixes in the read version."""

    _INSTANCE = MultiScenarioMatrix

    def as_write(self) -> MultiScenarioMatrixWriteList:
        """Convert these read versions of multi scenario matrix to the writing versions."""
        return MultiScenarioMatrixWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> MultiScenarioMatrixWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class MultiScenarioMatrixWriteList(DomainModelWriteList[MultiScenarioMatrixWrite]):
    """List of multi scenario matrixes in the writing version."""

    _INSTANCE = MultiScenarioMatrixWrite


class MultiScenarioMatrixApplyList(MultiScenarioMatrixWriteList): ...


def _create_multi_scenario_matrix_filter(
    view_id: dm.ViewId,
    resource_cost: str | list[str] | None = None,
    resource_cost_prefix: str | None = None,
    asset_type: str | list[str] | None = None,
    asset_type_prefix: str | None = None,
    asset_id: str | list[str] | None = None,
    asset_id_prefix: str | None = None,
    method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(resource_cost, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("resourceCost"), value=resource_cost))
    if resource_cost and isinstance(resource_cost, list):
        filters.append(dm.filters.In(view_id.as_property_ref("resourceCost"), values=resource_cost))
    if resource_cost_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("resourceCost"), value=resource_cost_prefix))
    if isinstance(asset_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetType"), value=asset_type))
    if asset_type and isinstance(asset_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetType"), values=asset_type))
    if asset_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetType"), value=asset_type_prefix))
    if isinstance(asset_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetId"), value=asset_id))
    if asset_id and isinstance(asset_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetId"), values=asset_id))
    if asset_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetId"), value=asset_id_prefix))
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
