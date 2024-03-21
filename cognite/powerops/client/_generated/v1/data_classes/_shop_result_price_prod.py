from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

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
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)
from ._shop_result import SHOPResult, SHOPResultWrite

if TYPE_CHECKING:
    from ._alert import Alert, AlertGraphQL, AlertWrite
    from ._case import Case, CaseGraphQL, CaseWrite
    from ._shop_time_series import SHOPTimeSeries, SHOPTimeSeriesGraphQL, SHOPTimeSeriesWrite


__all__ = [
    "SHOPResultPriceProd",
    "SHOPResultPriceProdWrite",
    "SHOPResultPriceProdApply",
    "SHOPResultPriceProdList",
    "SHOPResultPriceProdWriteList",
    "SHOPResultPriceProdApplyList",
    "SHOPResultPriceProdFields",
    "SHOPResultPriceProdTextFields",
]


SHOPResultPriceProdTextFields = Literal[
    "output_timeseries", "objective_sequence", "pre_run", "post_run", "shop_messages", "cplex_logs"
]
SHOPResultPriceProdFields = Literal[
    "output_timeseries", "objective_sequence", "pre_run", "post_run", "shop_messages", "cplex_logs"
]

_SHOPRESULTPRICEPROD_PROPERTIES_BY_FIELD = {
    "output_timeseries": "outputTimeseries",
    "objective_sequence": "objectiveSequence",
    "pre_run": "preRun",
    "post_run": "postRun",
    "shop_messages": "shopMessages",
    "cplex_logs": "cplexLogs",
}


class SHOPResultPriceProdGraphQL(GraphQLCore):
    """This represents the reading version of shop result price prod, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop result price prod.
        data_record: The data record of the shop result price prod node.
        case: The case that was used to produce this result
        output_timeseries: A general placeholder for all timeseries that stem from a shop run
        objective_sequence: The sequence of the objective function
        pre_run: The pre-run data for the SHOP run
        post_run: The post-run data for the SHOP run
        shop_messages: The messages from the SHOP run
        cplex_logs: The logs from CPLEX
        alerts: An array of calculation level Alerts.
        price_timeseries: The market price timeseries from the Shop run
        production_timeseries: The production timeseries wrapped as a ShopTimeSeries object containing properties related to their names and types in the resulting output shop file
    """

    view_id = dm.ViewId("sp_powerops_models", "SHOPResultPriceProd", "1")
    case: Optional[CaseGraphQL] = Field(None, repr=False)
    output_timeseries: Union[list[TimeSeries], list[str], None] = Field(None, alias="outputTimeseries")
    objective_sequence: Union[str, None] = Field(None, alias="objectiveSequence")
    pre_run: Union[str, None] = Field(None, alias="preRun")
    post_run: Union[str, None] = Field(None, alias="postRun")
    shop_messages: Union[str, None] = Field(None, alias="shopMessages")
    cplex_logs: Union[str, None] = Field(None, alias="cplexLogs")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    price_timeseries: Optional[SHOPTimeSeriesGraphQL] = Field(None, repr=False, alias="priceTimeseries")
    production_timeseries: Optional[list[SHOPTimeSeriesGraphQL]] = Field(
        default=None, repr=False, alias="productionTimeseries"
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

    @field_validator("case", "alerts", "price_timeseries", "production_timeseries", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> SHOPResultPriceProd:
        """Convert this GraphQL format of shop result price prod to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return SHOPResultPriceProd(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            case=self.case.as_read() if isinstance(self.case, GraphQLCore) else self.case,
            output_timeseries=self.output_timeseries,
            objective_sequence=self.objective_sequence,
            pre_run=self.pre_run,
            post_run=self.post_run,
            shop_messages=self.shop_messages,
            cplex_logs=self.cplex_logs,
            alerts=[alert.as_read() if isinstance(alert, GraphQLCore) else alert for alert in self.alerts or []],
            price_timeseries=(
                self.price_timeseries.as_read()
                if isinstance(self.price_timeseries, GraphQLCore)
                else self.price_timeseries
            ),
            production_timeseries=[
                production_timesery.as_read() if isinstance(production_timesery, GraphQLCore) else production_timesery
                for production_timesery in self.production_timeseries or []
            ],
        )

    def as_write(self) -> SHOPResultPriceProdWrite:
        """Convert this GraphQL format of shop result price prod to the writing format."""
        return SHOPResultPriceProdWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            case=self.case.as_write() if isinstance(self.case, DomainModel) else self.case,
            output_timeseries=self.output_timeseries,
            objective_sequence=self.objective_sequence,
            pre_run=self.pre_run,
            post_run=self.post_run,
            shop_messages=self.shop_messages,
            cplex_logs=self.cplex_logs,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            price_timeseries=(
                self.price_timeseries.as_write()
                if isinstance(self.price_timeseries, DomainModel)
                else self.price_timeseries
            ),
            production_timeseries=[
                production_timesery.as_write() if isinstance(production_timesery, DomainModel) else production_timesery
                for production_timesery in self.production_timeseries or []
            ],
        )


class SHOPResultPriceProd(SHOPResult):
    """This represents the reading version of shop result price prod.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop result price prod.
        data_record: The data record of the shop result price prod node.
        case: The case that was used to produce this result
        output_timeseries: A general placeholder for all timeseries that stem from a shop run
        objective_sequence: The sequence of the objective function
        pre_run: The pre-run data for the SHOP run
        post_run: The post-run data for the SHOP run
        shop_messages: The messages from the SHOP run
        cplex_logs: The logs from CPLEX
        alerts: An array of calculation level Alerts.
        price_timeseries: The market price timeseries from the Shop run
        production_timeseries: The production timeseries wrapped as a ShopTimeSeries object containing properties related to their names and types in the resulting output shop file
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "SHOPResultPriceProd"
    )
    price_timeseries: Union[SHOPTimeSeries, str, dm.NodeId, None] = Field(None, repr=False, alias="priceTimeseries")
    production_timeseries: Union[list[SHOPTimeSeries], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="productionTimeseries"
    )

    def as_write(self) -> SHOPResultPriceProdWrite:
        """Convert this read version of shop result price prod to the writing version."""
        return SHOPResultPriceProdWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            case=self.case.as_write() if isinstance(self.case, DomainModel) else self.case,
            output_timeseries=self.output_timeseries,
            objective_sequence=self.objective_sequence,
            pre_run=self.pre_run,
            post_run=self.post_run,
            shop_messages=self.shop_messages,
            cplex_logs=self.cplex_logs,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            price_timeseries=(
                self.price_timeseries.as_write()
                if isinstance(self.price_timeseries, DomainModel)
                else self.price_timeseries
            ),
            production_timeseries=[
                production_timesery.as_write() if isinstance(production_timesery, DomainModel) else production_timesery
                for production_timesery in self.production_timeseries or []
            ],
        )

    def as_apply(self) -> SHOPResultPriceProdWrite:
        """Convert this read version of shop result price prod to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPResultPriceProdWrite(SHOPResultWrite):
    """This represents the writing version of shop result price prod.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop result price prod.
        data_record: The data record of the shop result price prod node.
        case: The case that was used to produce this result
        output_timeseries: A general placeholder for all timeseries that stem from a shop run
        objective_sequence: The sequence of the objective function
        pre_run: The pre-run data for the SHOP run
        post_run: The post-run data for the SHOP run
        shop_messages: The messages from the SHOP run
        cplex_logs: The logs from CPLEX
        alerts: An array of calculation level Alerts.
        price_timeseries: The market price timeseries from the Shop run
        production_timeseries: The production timeseries wrapped as a ShopTimeSeries object containing properties related to their names and types in the resulting output shop file
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "SHOPResultPriceProd"
    )
    price_timeseries: Union[SHOPTimeSeriesWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="priceTimeseries"
    )
    production_timeseries: Union[list[SHOPTimeSeriesWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="productionTimeseries"
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
            SHOPResultPriceProd, dm.ViewId("sp_powerops_models", "SHOPResultPriceProd", "1")
        )

        properties: dict[str, Any] = {}

        if self.case is not None:
            properties["case"] = {
                "space": self.space if isinstance(self.case, str) else self.case.space,
                "externalId": self.case if isinstance(self.case, str) else self.case.external_id,
            }

        if self.output_timeseries is not None or write_none:
            properties["outputTimeseries"] = [
                value if isinstance(value, str) else value.external_id for value in self.output_timeseries or []
            ] or None

        if self.objective_sequence is not None or write_none:
            properties["objectiveSequence"] = self.objective_sequence

        if self.pre_run is not None or write_none:
            properties["preRun"] = self.pre_run

        if self.post_run is not None or write_none:
            properties["postRun"] = self.post_run

        if self.shop_messages is not None or write_none:
            properties["shopMessages"] = self.shop_messages

        if self.cplex_logs is not None or write_none:
            properties["cplexLogs"] = self.cplex_logs

        if self.price_timeseries is not None:
            properties["priceTimeseries"] = {
                "space": self.space if isinstance(self.price_timeseries, str) else self.price_timeseries.space,
                "externalId": (
                    self.price_timeseries
                    if isinstance(self.price_timeseries, str)
                    else self.price_timeseries.external_id
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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "calculationIssue")
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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "SHOPResultPriceProd.productionTimeseries")
        for production_timesery in self.production_timeseries or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=production_timesery,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.case, DomainModelWrite):
            other_resources = self.case._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.price_timeseries, DomainModelWrite):
            other_resources = self.price_timeseries._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.output_timeseries, CogniteTimeSeries):
            resources.time_series.append(self.output_timeseries)

        return resources


class SHOPResultPriceProdApply(SHOPResultPriceProdWrite):
    def __new__(cls, *args, **kwargs) -> SHOPResultPriceProdApply:
        warnings.warn(
            "SHOPResultPriceProdApply is deprecated and will be removed in v1.0. Use SHOPResultPriceProdWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "SHOPResultPriceProd.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class SHOPResultPriceProdList(DomainModelList[SHOPResultPriceProd]):
    """List of shop result price prods in the read version."""

    _INSTANCE = SHOPResultPriceProd

    def as_write(self) -> SHOPResultPriceProdWriteList:
        """Convert these read versions of shop result price prod to the writing versions."""
        return SHOPResultPriceProdWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> SHOPResultPriceProdWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPResultPriceProdWriteList(DomainModelWriteList[SHOPResultPriceProdWrite]):
    """List of shop result price prods in the writing version."""

    _INSTANCE = SHOPResultPriceProdWrite


class SHOPResultPriceProdApplyList(SHOPResultPriceProdWriteList): ...


def _create_shop_result_price_prod_filter(
    view_id: dm.ViewId,
    case: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    price_timeseries: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if case and isinstance(case, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("case"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": case}
            )
        )
    if case and isinstance(case, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("case"), value={"space": case[0], "externalId": case[1]})
        )
    if case and isinstance(case, list) and isinstance(case[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("case"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in case],
            )
        )
    if case and isinstance(case, list) and isinstance(case[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("case"), values=[{"space": item[0], "externalId": item[1]} for item in case]
            )
        )
    if price_timeseries and isinstance(price_timeseries, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("priceTimeseries"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": price_timeseries},
            )
        )
    if price_timeseries and isinstance(price_timeseries, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("priceTimeseries"),
                value={"space": price_timeseries[0], "externalId": price_timeseries[1]},
            )
        )
    if price_timeseries and isinstance(price_timeseries, list) and isinstance(price_timeseries[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("priceTimeseries"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in price_timeseries],
            )
        )
    if price_timeseries and isinstance(price_timeseries, list) and isinstance(price_timeseries[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("priceTimeseries"),
                values=[{"space": item[0], "externalId": item[1]} for item in price_timeseries],
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
