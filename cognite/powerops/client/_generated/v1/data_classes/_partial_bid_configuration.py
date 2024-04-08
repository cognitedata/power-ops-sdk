from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

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


__all__ = [
    "PartialBidConfiguration",
    "PartialBidConfigurationWrite",
    "PartialBidConfigurationApply",
    "PartialBidConfigurationList",
    "PartialBidConfigurationWriteList",
    "PartialBidConfigurationApplyList",
    "PartialBidConfigurationFields",
    "PartialBidConfigurationTextFields",
]


PartialBidConfigurationTextFields = Literal["name", "method"]
PartialBidConfigurationFields = Literal["name", "method", "add_steps"]

_PARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD = {
    "name": "name",
    "method": "method",
    "add_steps": "addSteps",
}


class PartialBidConfiguration(DomainModel):
    """This represents the reading version of partial bid configuration.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial bid configuration.
        data_record: The data record of the partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        add_steps: TODO definition
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    method: Optional[str] = None
    add_steps: bool = Field(alias="addSteps")

    def as_write(self) -> PartialBidConfigurationWrite:
        """Convert this read version of partial bid configuration to the writing version."""
        return PartialBidConfigurationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            method=self.method,
            add_steps=self.add_steps,
        )

    def as_apply(self) -> PartialBidConfigurationWrite:
        """Convert this read version of partial bid configuration to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PartialBidConfigurationWrite(DomainModelWrite):
    """This represents the writing version of partial bid configuration.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial bid configuration.
        data_record: The data record of the partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        add_steps: TODO definition
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    method: Optional[str] = None
    add_steps: bool = Field(alias="addSteps")

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
            PartialBidConfiguration, dm.ViewId("sp_powerops_models_temp", "PartialBidConfiguration", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.method is not None or write_none:
            properties["method"] = self.method

        if self.add_steps is not None:
            properties["addSteps"] = self.add_steps

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

        return resources


class PartialBidConfigurationApply(PartialBidConfigurationWrite):
    def __new__(cls, *args, **kwargs) -> PartialBidConfigurationApply:
        warnings.warn(
            "PartialBidConfigurationApply is deprecated and will be removed in v1.0. Use PartialBidConfigurationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PartialBidConfiguration.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PartialBidConfigurationList(DomainModelList[PartialBidConfiguration]):
    """List of partial bid configurations in the read version."""

    _INSTANCE = PartialBidConfiguration

    def as_write(self) -> PartialBidConfigurationWriteList:
        """Convert these read versions of partial bid configuration to the writing versions."""
        return PartialBidConfigurationWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PartialBidConfigurationWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PartialBidConfigurationWriteList(DomainModelWriteList[PartialBidConfigurationWrite]):
    """List of partial bid configurations in the writing version."""

    _INSTANCE = PartialBidConfigurationWrite


class PartialBidConfigurationApplyList(PartialBidConfigurationWriteList): ...


def _create_partial_bid_configuration_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    add_steps: bool | None = None,
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
    if isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if isinstance(add_steps, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("addSteps"), value=add_steps))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
