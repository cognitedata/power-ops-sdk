from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

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


__all__ = [
    "PriceScenario",
    "PriceScenarioWrite",
    "PriceScenarioApply",
    "PriceScenarioList",
    "PriceScenarioWriteList",
    "PriceScenarioApplyList",
    "PriceScenarioFields",
    "PriceScenarioTextFields",
]


PriceScenarioTextFields = Literal["name", "timeseries"]
PriceScenarioFields = Literal["name", "timeseries"]

_PRICESCENARIO_PROPERTIES_BY_FIELD = {
    "name": "name",
    "timeseries": "timeseries",
}


class PriceScenario(DomainModel):
    """This represents the reading version of price scenario.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price scenario.
        data_record: The data record of the price scenario node.
        name: The name of the scenario
        timeseries: The price timeseries in this price scenario
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PriceScenario"
    )
    name: str
    timeseries: Union[TimeSeries, str, None] = None

    def as_write(self) -> PriceScenarioWrite:
        """Convert this read version of price scenario to the writing version."""
        return PriceScenarioWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            timeseries=self.timeseries,
        )

    def as_apply(self) -> PriceScenarioWrite:
        """Convert this read version of price scenario to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceScenarioWrite(DomainModelWrite):
    """This represents the writing version of price scenario.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price scenario.
        data_record: The data record of the price scenario node.
        name: The name of the scenario
        timeseries: The price timeseries in this price scenario
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PriceScenario"
    )
    name: str
    timeseries: Union[TimeSeries, str, None] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            PriceScenario, dm.ViewId("sp_powerops_models", "PriceScenario", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.timeseries is not None or write_none:
            if isinstance(self.timeseries, str) or self.timeseries is None:
                properties["timeseries"] = self.timeseries
            else:
                properties["timeseries"] = self.timeseries.external_id

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

        if isinstance(self.timeseries, CogniteTimeSeries):
            resources.time_series.append(self.timeseries)

        return resources


class PriceScenarioApply(PriceScenarioWrite):
    def __new__(cls, *args, **kwargs) -> PriceScenarioApply:
        warnings.warn(
            "PriceScenarioApply is deprecated and will be removed in v1.0. Use PriceScenarioWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PriceScenario.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PriceScenarioList(DomainModelList[PriceScenario]):
    """List of price scenarios in the read version."""

    _INSTANCE = PriceScenario

    def as_write(self) -> PriceScenarioWriteList:
        """Convert these read versions of price scenario to the writing versions."""
        return PriceScenarioWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PriceScenarioWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceScenarioWriteList(DomainModelWriteList[PriceScenarioWrite]):
    """List of price scenarios in the writing version."""

    _INSTANCE = PriceScenarioWrite


class PriceScenarioApplyList(PriceScenarioWriteList): ...


def _create_price_scenario_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
