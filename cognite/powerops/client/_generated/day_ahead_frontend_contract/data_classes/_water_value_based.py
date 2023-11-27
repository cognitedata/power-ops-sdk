from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "WaterValueBased",
    "WaterValueBasedApply",
    "WaterValueBasedList",
    "WaterValueBasedApplyList",
    "WaterValueBasedFields",
    "WaterValueBasedTextFields",
]


WaterValueBasedTextFields = Literal["name"]
WaterValueBasedFields = Literal["name"]

_WATERVALUEBASED_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class WaterValueBased(DomainModel):
    """This represent a read version of water value based.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based.
        name: The name field.
        created_time: The created time of the water value based node.
        last_updated_time: The last updated time of the water value based node.
        deleted_time: If present, the deleted time of the water value based node.
        version: The version of the water value based node.
    """

    space: str = "dayAheadFrontendContractModel"
    name: Optional[str] = None

    def as_apply(self) -> WaterValueBasedApply:
        """Convert this read version of water value based to a write version."""
        return WaterValueBasedApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
        )


class WaterValueBasedApply(DomainModelApply):
    """This represent a write version of water value based.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based.
        name: The name field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "dayAheadFrontendContractModel"
    name: str

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("dayAheadFrontendContractModel", "WaterValueBased", "1"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class WaterValueBasedList(TypeList[WaterValueBased]):
    """List of water value baseds in read version."""

    _NODE = WaterValueBased

    def as_apply(self) -> WaterValueBasedApplyList:
        """Convert this read version of water value based to a write version."""
        return WaterValueBasedApplyList([node.as_apply() for node in self.data])


class WaterValueBasedApplyList(TypeApplyList[WaterValueBasedApply]):
    """List of water value baseds in write version."""

    _NODE = WaterValueBasedApply
