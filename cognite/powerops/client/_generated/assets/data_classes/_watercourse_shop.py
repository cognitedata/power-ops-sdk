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
    "WatercourseSHOP",
    "WatercourseSHOPApply",
    "WatercourseSHOPList",
    "WatercourseSHOPApplyList",
    "WatercourseSHOPFields",
]

WatercourseSHOPFields = Literal["penalty_limit"]

_WATERCOURSESHOP_PROPERTIES_BY_FIELD = {
    "penalty_limit": "penaltyLimit",
}


class WatercourseSHOP(DomainModel):
    """This represents the reading version of watercourse shop.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the watercourse shop.
        penalty_limit: The penalty limit for the watercourse
        created_time: The created time of the watercourse shop node.
        last_updated_time: The last updated time of the watercourse shop node.
        deleted_time: If present, the deleted time of the watercourse shop node.
        version: The version of the watercourse shop node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")

    def as_apply(self) -> WatercourseSHOPApply:
        """Convert this read version of watercourse shop to the writing version."""
        return WatercourseSHOPApply(
            space=self.space,
            external_id=self.external_id,
            penalty_limit=self.penalty_limit,
        )


class WatercourseSHOPApply(DomainModelApply):
    """This represents the writing version of watercourse shop.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the watercourse shop.
        penalty_limit: The penalty limit for the watercourse
        existing_version: Fail the ingestion request if the watercourse shop version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-assets", "WatercourseSHOP", "1"
        )

        properties = {}
        if self.penalty_limit is not None:
            properties["penaltyLimit"] = self.penalty_limit

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


class WatercourseSHOPList(DomainModelList[WatercourseSHOP]):
    """List of watercourse shops in the read version."""

    _INSTANCE = WatercourseSHOP

    def as_apply(self) -> WatercourseSHOPApplyList:
        """Convert these read versions of watercourse shop to the writing versions."""
        return WatercourseSHOPApplyList([node.as_apply() for node in self.data])


class WatercourseSHOPApplyList(DomainModelApplyList[WatercourseSHOPApply]):
    """List of watercourse shops in the writing version."""

    _INSTANCE = WatercourseSHOPApply


def _create_watercourse_shop_filter(
    view_id: dm.ViewId,
    min_penalty_limit: float | None = None,
    max_penalty_limit: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_penalty_limit or max_penalty_limit:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("penaltyLimit"), gte=min_penalty_limit, lte=max_penalty_limit)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
