from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = [
    "GeneratorEfficiencyCurve",
    "GeneratorEfficiencyCurveApply",
    "GeneratorEfficiencyCurveList",
    "GeneratorEfficiencyCurveApplyList",
    "GeneratorEfficiencyCurveFields",
]

GeneratorEfficiencyCurveFields = Literal["generator_power", "generator_efficiency"]

_GENERATOREFFICIENCYCURVE_PROPERTIES_BY_FIELD = {
    "generator_power": "generatorPower",
    "generator_efficiency": "generatorEfficiency",
}


class GeneratorEfficiencyCurve(DomainModel):
    """This represents the reading version of generator efficiency curve.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator efficiency curve.
        generator_power: The generator power values
        generator_efficiency: The generator efficiency values
        created_time: The created time of the generator efficiency curve node.
        last_updated_time: The last updated time of the generator efficiency curve node.
        deleted_time: If present, the deleted time of the generator efficiency curve node.
        version: The version of the generator efficiency curve node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    generator_power: Optional[list[float]] = Field(None, alias="generatorPower")
    generator_efficiency: Optional[list[float]] = Field(None, alias="generatorEfficiency")

    def as_apply(self) -> GeneratorEfficiencyCurveApply:
        """Convert this read version of generator efficiency curve to the writing version."""
        return GeneratorEfficiencyCurveApply(
            space=self.space,
            external_id=self.external_id,
            generator_power=self.generator_power,
            generator_efficiency=self.generator_efficiency,
        )


class GeneratorEfficiencyCurveApply(DomainModelApply):
    """This represents the writing version of generator efficiency curve.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator efficiency curve.
        generator_power: The generator power values
        generator_efficiency: The generator efficiency values
        existing_version: Fail the ingestion request if the generator efficiency curve version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    generator_power: list[float] = Field(alias="generatorPower")
    generator_efficiency: list[float] = Field(alias="generatorEfficiency")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-assets", "GeneratorEfficiencyCurve", "1"
        )

        properties = {}
        if self.generator_power is not None:
            properties["generatorPower"] = self.generator_power
        if self.generator_efficiency is not None:
            properties["generatorEfficiency"] = self.generator_efficiency

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
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


class GeneratorEfficiencyCurveList(DomainModelList[GeneratorEfficiencyCurve]):
    """List of generator efficiency curves in the read version."""

    _INSTANCE = GeneratorEfficiencyCurve

    def as_apply(self) -> GeneratorEfficiencyCurveApplyList:
        """Convert these read versions of generator efficiency curve to the writing versions."""
        return GeneratorEfficiencyCurveApplyList([node.as_apply() for node in self.data])


class GeneratorEfficiencyCurveApplyList(DomainModelApplyList[GeneratorEfficiencyCurveApply]):
    """List of generator efficiency curves in the writing version."""

    _INSTANCE = GeneratorEfficiencyCurveApply


def _create_generator_efficiency_curve_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None