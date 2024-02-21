from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._alert import Alert, AlertWrite
    from ._price_scenario import PriceScenario, PriceScenarioWrite
    from ._scenario import Scenario, ScenarioWrite


__all__ = [
    "SHOPResult",
    "SHOPResultWrite",
    "SHOPResultApply",
    "SHOPResultList",
    "SHOPResultWriteList",
    "SHOPResultApplyList",
    "SHOPResultFields",
    "SHOPResultTextFields",
]


SHOPResultTextFields = Literal["production", "price", "objective_sequence"]
SHOPResultFields = Literal["production", "price", "objective_sequence"]

_SHOPRESULT_PROPERTIES_BY_FIELD = {
    "production": "production",
    "price": "price",
    "objective_sequence": "objectiveSequence",
}


class SHOPResult(DomainModel):
    """This represents the reading version of shop result.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop result.
        data_record: The data record of the shop result node.
        scenario: The Shop scenario that was used to produce this result
        price_scenario: The price scenario that was used to produce this result
        production: The result production timeseries from a SHOP run
        price: The result price timeseries from a SHOP run
        objective_sequence: The sequence of the objective function
        alerts: An array of calculation level Alerts.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types", "SHOPResult")
    scenario: Union[Scenario, str, dm.NodeId, None] = Field(None, repr=False)
    price_scenario: Union[PriceScenario, str, dm.NodeId, None] = Field(None, repr=False)
    production: Union[list[TimeSeries], list[str], None] = None
    price: Union[list[TimeSeries], list[str], None] = None
    objective_sequence: Union[str, None] = Field(None, alias="objectiveSequence")
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)

    def as_write(self) -> SHOPResultWrite:
        """Convert this read version of shop result to the writing version."""
        return SHOPResultWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            scenario=self.scenario.as_write() if isinstance(self.scenario, DomainModel) else self.scenario,
            price_scenario=(
                self.price_scenario.as_write() if isinstance(self.price_scenario, DomainModel) else self.price_scenario
            ),
            production=self.production,
            price=self.price,
            objective_sequence=self.objective_sequence,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
        )

    def as_apply(self) -> SHOPResultWrite:
        """Convert this read version of shop result to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPResultWrite(DomainModelWrite):
    """This represents the writing version of shop result.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop result.
        data_record: The data record of the shop result node.
        scenario: The Shop scenario that was used to produce this result
        price_scenario: The price scenario that was used to produce this result
        production: The result production timeseries from a SHOP run
        price: The result price timeseries from a SHOP run
        objective_sequence: The sequence of the objective function
        alerts: An array of calculation level Alerts.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types", "SHOPResult")
    scenario: Union[ScenarioWrite, str, dm.NodeId, None] = Field(None, repr=False)
    price_scenario: Union[PriceScenarioWrite, str, dm.NodeId, None] = Field(None, repr=False)
    production: Union[list[TimeSeries], list[str], None] = None
    price: Union[list[TimeSeries], list[str], None] = None
    objective_sequence: Union[str, None] = Field(None, alias="objectiveSequence")
    alerts: Union[list[AlertWrite], list[str], None] = Field(default=None, repr=False)

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(SHOPResult, dm.ViewId("sp_powerops_models", "SHOPResult", "1"))

        properties: dict[str, Any] = {}

        if self.scenario is not None:
            properties["scenario"] = {
                "space": self.space if isinstance(self.scenario, str) else self.scenario.space,
                "externalId": self.scenario if isinstance(self.scenario, str) else self.scenario.external_id,
            }

        if self.price_scenario is not None:
            properties["price_scenario"] = {
                "space": self.space if isinstance(self.price_scenario, str) else self.price_scenario.space,
                "externalId": (
                    self.price_scenario if isinstance(self.price_scenario, str) else self.price_scenario.external_id
                ),
            }

        if self.production is not None or write_none:
            properties["production"] = [
                value if isinstance(value, str) else value.external_id for value in self.production or []
            ] or None

        if self.price is not None or write_none:
            properties["price"] = [
                value if isinstance(value, str) else value.external_id for value in self.price or []
            ] or None

        if self.objective_sequence is not None or write_none:
            properties["objectiveSequence"] = self.objective_sequence

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
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
                cache, start_node=self, end_node=alert, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.scenario, DomainModelWrite):
            other_resources = self.scenario._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.price_scenario, DomainModelWrite):
            other_resources = self.price_scenario._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.production, CogniteTimeSeries):
            resources.time_series.append(self.production)

        if isinstance(self.price, CogniteTimeSeries):
            resources.time_series.append(self.price)

        return resources


class SHOPResultApply(SHOPResultWrite):
    def __new__(cls, *args, **kwargs) -> SHOPResultApply:
        warnings.warn(
            "SHOPResultApply is deprecated and will be removed in v1.0. Use SHOPResultWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "SHOPResult.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class SHOPResultList(DomainModelList[SHOPResult]):
    """List of shop results in the read version."""

    _INSTANCE = SHOPResult

    def as_write(self) -> SHOPResultWriteList:
        """Convert these read versions of shop result to the writing versions."""
        return SHOPResultWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> SHOPResultWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPResultWriteList(DomainModelWriteList[SHOPResultWrite]):
    """List of shop results in the writing version."""

    _INSTANCE = SHOPResultWrite


class SHOPResultApplyList(SHOPResultWriteList): ...


def _create_shop_result_filter(
    view_id: dm.ViewId,
    scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if scenario and isinstance(scenario, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("scenario"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": scenario}
            )
        )
    if scenario and isinstance(scenario, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("scenario"), value={"space": scenario[0], "externalId": scenario[1]}
            )
        )
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("scenario"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in scenario],
            )
        )
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("scenario"),
                values=[{"space": item[0], "externalId": item[1]} for item in scenario],
            )
        )
    if price_scenario and isinstance(price_scenario, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("price_scenario"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": price_scenario},
            )
        )
    if price_scenario and isinstance(price_scenario, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("price_scenario"),
                value={"space": price_scenario[0], "externalId": price_scenario[1]},
            )
        )
    if price_scenario and isinstance(price_scenario, list) and isinstance(price_scenario[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("price_scenario"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in price_scenario],
            )
        )
    if price_scenario and isinstance(price_scenario, list) and isinstance(price_scenario[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("price_scenario"),
                values=[{"space": item[0], "externalId": item[1]} for item in price_scenario],
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
