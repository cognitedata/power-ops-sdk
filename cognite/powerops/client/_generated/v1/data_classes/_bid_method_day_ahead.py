from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
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
)
from ._bid_method import BidMethod, BidMethodWrite

if TYPE_CHECKING:
    from ._mapping import Mapping, MappingWrite


__all__ = [
    "BidMethodDayAhead",
    "BidMethodDayAheadWrite",
    "BidMethodDayAheadApply",
    "BidMethodDayAheadList",
    "BidMethodDayAheadWriteList",
    "BidMethodDayAheadApplyList",
    "BidMethodDayAheadFields",
    "BidMethodDayAheadTextFields",
]


BidMethodDayAheadTextFields = Literal["name"]
BidMethodDayAheadFields = Literal["name"]

_BIDMETHODDAYAHEAD_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class BidMethodDayAhead(BidMethod):
    """This represents the reading version of bid method day ahead.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid method day ahead.
        data_record: The data record of the bid method day ahead node.
        name: Name for the BidMethod
        main_scenario: The main scenario to use when running the bid method
    """

    node_type: Union[dm.DirectRelationReference, None] = None
    main_scenario: Union[Mapping, str, dm.NodeId, None] = Field(None, repr=False, alias="mainScenario")

    def as_write(self) -> BidMethodDayAheadWrite:
        """Convert this read version of bid method day ahead to the writing version."""
        return BidMethodDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            main_scenario=(
                self.main_scenario.as_write() if isinstance(self.main_scenario, DomainModel) else self.main_scenario
            ),
        )

    def as_apply(self) -> BidMethodDayAheadWrite:
        """Convert this read version of bid method day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMethodDayAheadWrite(BidMethodWrite):
    """This represents the writing version of bid method day ahead.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid method day ahead.
        data_record: The data record of the bid method day ahead node.
        name: Name for the BidMethod
        main_scenario: The main scenario to use when running the bid method
    """

    node_type: Union[dm.DirectRelationReference, None] = None
    main_scenario: Union[MappingWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="mainScenario")

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
            BidMethodDayAhead, dm.ViewId("sp_powerops_models", "BidMethodDayAhead", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.main_scenario is not None:
            properties["mainScenario"] = {
                "space": self.space if isinstance(self.main_scenario, str) else self.main_scenario.space,
                "externalId": (
                    self.main_scenario if isinstance(self.main_scenario, str) else self.main_scenario.external_id
                ),
            }

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

        if isinstance(self.main_scenario, DomainModelWrite):
            other_resources = self.main_scenario._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class BidMethodDayAheadApply(BidMethodDayAheadWrite):
    def __new__(cls, *args, **kwargs) -> BidMethodDayAheadApply:
        warnings.warn(
            "BidMethodDayAheadApply is deprecated and will be removed in v1.0. Use BidMethodDayAheadWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidMethodDayAhead.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidMethodDayAheadList(DomainModelList[BidMethodDayAhead]):
    """List of bid method day aheads in the read version."""

    _INSTANCE = BidMethodDayAhead

    def as_write(self) -> BidMethodDayAheadWriteList:
        """Convert these read versions of bid method day ahead to the writing versions."""
        return BidMethodDayAheadWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidMethodDayAheadWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMethodDayAheadWriteList(DomainModelWriteList[BidMethodDayAheadWrite]):
    """List of bid method day aheads in the writing version."""

    _INSTANCE = BidMethodDayAheadWrite


class BidMethodDayAheadApplyList(BidMethodDayAheadWriteList): ...


def _create_bid_method_day_ahead_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if main_scenario and isinstance(main_scenario, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("mainScenario"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": main_scenario},
            )
        )
    if main_scenario and isinstance(main_scenario, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("mainScenario"),
                value={"space": main_scenario[0], "externalId": main_scenario[1]},
            )
        )
    if main_scenario and isinstance(main_scenario, list) and isinstance(main_scenario[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("mainScenario"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in main_scenario],
            )
        )
    if main_scenario and isinstance(main_scenario, list) and isinstance(main_scenario[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("mainScenario"),
                values=[{"space": item[0], "externalId": item[1]} for item in main_scenario],
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
