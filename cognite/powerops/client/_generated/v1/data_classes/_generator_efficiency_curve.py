from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm
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
)


__all__ = [
    "GeneratorEfficiencyCurve",
    "GeneratorEfficiencyCurveWrite",
    "GeneratorEfficiencyCurveApply",
    "GeneratorEfficiencyCurveList",
    "GeneratorEfficiencyCurveWriteList",
    "GeneratorEfficiencyCurveApplyList",
    "GeneratorEfficiencyCurveFields",

    "GeneratorEfficiencyCurveGraphQL",
]

GeneratorEfficiencyCurveFields = Literal["power", "efficiency"]

_GENERATOREFFICIENCYCURVE_PROPERTIES_BY_FIELD = {
    "power": "power",
    "efficiency": "efficiency",
}

class GeneratorEfficiencyCurveGraphQL(GraphQLCore):
    """This represents the reading version of generator efficiency curve, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator efficiency curve.
        data_record: The data record of the generator efficiency curve node.
        power: The generator power values
        efficiency: The generator efficiency values
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "GeneratorEfficiencyCurve", "1")
    power: Optional[list[float]] = None
    efficiency: Optional[list[float]] = None

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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> GeneratorEfficiencyCurve:
        """Convert this GraphQL format of generator efficiency curve to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return GeneratorEfficiencyCurve(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            power=self.power,
            efficiency=self.efficiency,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> GeneratorEfficiencyCurveWrite:
        """Convert this GraphQL format of generator efficiency curve to the writing format."""
        return GeneratorEfficiencyCurveWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            power=self.power,
            efficiency=self.efficiency,
        )


class GeneratorEfficiencyCurve(DomainModel):
    """This represents the reading version of generator efficiency curve.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator efficiency curve.
        data_record: The data record of the generator efficiency curve node.
        power: The generator power values
        efficiency: The generator efficiency values
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "GeneratorEfficiencyCurve", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "GeneratorEfficiencyCurve")
    power: list[float]
    efficiency: list[float]

    def as_write(self) -> GeneratorEfficiencyCurveWrite:
        """Convert this read version of generator efficiency curve to the writing version."""
        return GeneratorEfficiencyCurveWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            power=self.power,
            efficiency=self.efficiency,
        )

    def as_apply(self) -> GeneratorEfficiencyCurveWrite:
        """Convert this read version of generator efficiency curve to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class GeneratorEfficiencyCurveWrite(DomainModelWrite):
    """This represents the writing version of generator efficiency curve.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator efficiency curve.
        data_record: The data record of the generator efficiency curve node.
        power: The generator power values
        efficiency: The generator efficiency values
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "GeneratorEfficiencyCurve", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "GeneratorEfficiencyCurve")
    power: list[float]
    efficiency: list[float]

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

        if self.power is not None:
            properties["power"] = self.power

        if self.efficiency is not None:
            properties["efficiency"] = self.efficiency


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



        return resources


class GeneratorEfficiencyCurveApply(GeneratorEfficiencyCurveWrite):
    def __new__(cls, *args, **kwargs) -> GeneratorEfficiencyCurveApply:
        warnings.warn(
            "GeneratorEfficiencyCurveApply is deprecated and will be removed in v1.0. Use GeneratorEfficiencyCurveWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "GeneratorEfficiencyCurve.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class GeneratorEfficiencyCurveList(DomainModelList[GeneratorEfficiencyCurve]):
    """List of generator efficiency curves in the read version."""

    _INSTANCE = GeneratorEfficiencyCurve

    def as_write(self) -> GeneratorEfficiencyCurveWriteList:
        """Convert these read versions of generator efficiency curve to the writing versions."""
        return GeneratorEfficiencyCurveWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> GeneratorEfficiencyCurveWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class GeneratorEfficiencyCurveWriteList(DomainModelWriteList[GeneratorEfficiencyCurveWrite]):
    """List of generator efficiency curves in the writing version."""

    _INSTANCE = GeneratorEfficiencyCurveWrite

class GeneratorEfficiencyCurveApplyList(GeneratorEfficiencyCurveWriteList): ...



def _create_generator_efficiency_curve_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
