from __future__ import annotations

import datetime
import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
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

if TYPE_CHECKING:
    from ._shop_file import ShopFile, ShopFileGraphQL, ShopFileWrite
    from ._shop_scenario import ShopScenario, ShopScenarioGraphQL, ShopScenarioWrite


__all__ = [
    "ShopCase",
    "ShopCaseWrite",
    "ShopCaseApply",
    "ShopCaseList",
    "ShopCaseWriteList",
    "ShopCaseApplyList",
    "ShopCaseFields",

    "ShopCaseGraphQL",
]

ShopCaseFields = Literal["start_time", "end_time"]

_SHOPCASE_PROPERTIES_BY_FIELD = {
    "start_time": "startTime",
    "end_time": "endTime",
}

class ShopCaseGraphQL(GraphQLCore):
    """This represents the reading version of shop case, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop case.
        data_record: The data record of the shop case node.
        scenario: The Shop scenario that was used to produce this result
        start_time: The start time of the case
        end_time: The end time of the case
        shop_files: The list of shop files that are used in a shop run. This encompasses all shop files such as case, module series, cut files etc.
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopCase", "1")
    scenario: Optional[ShopScenarioGraphQL] = Field(default=None, repr=False)
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    shop_files: Optional[list[ShopFileGraphQL]] = Field(default=None, repr=False, alias="shopFiles")

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
    @field_validator("scenario", "shop_files", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopCase:
        """Convert this GraphQL format of shop case to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopCase(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            scenario=self.scenario.as_read() if isinstance(self.scenario, GraphQLCore) else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
            shop_files=[shop_file.as_read() for shop_file in self.shop_files or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopCaseWrite:
        """Convert this GraphQL format of shop case to the writing format."""
        return ShopCaseWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            scenario=self.scenario.as_write() if isinstance(self.scenario, GraphQLCore) else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
            shop_files=[shop_file.as_write() for shop_file in self.shop_files or []],
        )


class ShopCase(DomainModel):
    """This represents the reading version of shop case.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop case.
        data_record: The data record of the shop case node.
        scenario: The Shop scenario that was used to produce this result
        start_time: The start time of the case
        end_time: The end time of the case
        shop_files: The list of shop files that are used in a shop run. This encompasses all shop files such as case, module series, cut files etc.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopCase", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    scenario: Union[ShopScenario, str, dm.NodeId, None] = Field(default=None, repr=False)
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    shop_files: Optional[list[Union[ShopFile, str, dm.NodeId]]] = Field(default=None, repr=False, alias="shopFiles")

    def as_write(self) -> ShopCaseWrite:
        """Convert this read version of shop case to the writing version."""
        return ShopCaseWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            scenario=self.scenario.as_write() if isinstance(self.scenario, DomainModel) else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
            shop_files=[shop_file.as_write() if isinstance(shop_file, DomainModel) else shop_file for shop_file in self.shop_files or []],
        )

    def as_apply(self) -> ShopCaseWrite:
        """Convert this read version of shop case to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopCaseWrite(DomainModelWrite):
    """This represents the writing version of shop case.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop case.
        data_record: The data record of the shop case node.
        scenario: The Shop scenario that was used to produce this result
        start_time: The start time of the case
        end_time: The end time of the case
        shop_files: The list of shop files that are used in a shop run. This encompasses all shop files such as case, module series, cut files etc.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopCase", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    scenario: Union[ShopScenarioWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    shop_files: Optional[list[Union[ShopFileWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="shopFiles")

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

        if self.scenario is not None:
            properties["scenario"] = {
                "space":  self.space if isinstance(self.scenario, str) else self.scenario.space,
                "externalId": self.scenario if isinstance(self.scenario, str) else self.scenario.external_id,
            }

        if self.start_time is not None or write_none:
            properties["startTime"] = self.start_time.isoformat(timespec="milliseconds") if self.start_time else None

        if self.end_time is not None or write_none:
            properties["endTime"] = self.end_time.isoformat(timespec="milliseconds") if self.end_time else None


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



        edge_type = dm.DirectRelationReference("power_ops_types", "ShopCase.shopFiles")
        for shop_file in self.shop_files or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=shop_file,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.scenario, DomainModelWrite):
            other_resources = self.scenario._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class ShopCaseApply(ShopCaseWrite):
    def __new__(cls, *args, **kwargs) -> ShopCaseApply:
        warnings.warn(
            "ShopCaseApply is deprecated and will be removed in v1.0. Use ShopCaseWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopCase.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopCaseList(DomainModelList[ShopCase]):
    """List of shop cases in the read version."""

    _INSTANCE = ShopCase

    def as_write(self) -> ShopCaseWriteList:
        """Convert these read versions of shop case to the writing versions."""
        return ShopCaseWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopCaseWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopCaseWriteList(DomainModelWriteList[ShopCaseWrite]):
    """List of shop cases in the writing version."""

    _INSTANCE = ShopCaseWrite

class ShopCaseApplyList(ShopCaseWriteList): ...



def _create_shop_case_filter(
    view_id: dm.ViewId,
    scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if scenario and isinstance(scenario, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": scenario}))
    if scenario and isinstance(scenario, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value={"space": scenario[0], "externalId": scenario[1]}))
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in scenario]))
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=[{"space": item[0], "externalId": item[1]} for item in scenario]))
    if min_start_time is not None or max_start_time is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("startTime"), gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None, lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None))
    if min_end_time is not None or max_end_time is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("endTime"), gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None, lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
