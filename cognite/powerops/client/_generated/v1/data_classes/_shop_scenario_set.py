from __future__ import annotations

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
    from ._date_specification import DateSpecification, DateSpecificationGraphQL, DateSpecificationWrite
    from ._shop_scenario import ShopScenario, ShopScenarioGraphQL, ShopScenarioWrite


__all__ = [
    "ShopScenarioSet",
    "ShopScenarioSetWrite",
    "ShopScenarioSetApply",
    "ShopScenarioSetList",
    "ShopScenarioSetWriteList",
    "ShopScenarioSetApplyList",
    "ShopScenarioSetFields",
    "ShopScenarioSetTextFields",
    "ShopScenarioSetGraphQL",
]


ShopScenarioSetTextFields = Literal["name"]
ShopScenarioSetFields = Literal["name"]

_SHOPSCENARIOSET_PROPERTIES_BY_FIELD = {
    "name": "name",
}

class ShopScenarioSetGraphQL(GraphQLCore):
    """This represents the reading version of shop scenario set, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop scenario set.
        data_record: The data record of the shop scenario set node.
        name: The name of the scenario set to run
        start_specification: TODO description
        end_specification: TODO description
        scenarios: Configuration of the partial bids that make up the total bid configuration
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopScenarioSet", "1")
    name: Optional[str] = None
    start_specification: Optional[DateSpecificationGraphQL] = Field(default=None, repr=False, alias="startSpecification")
    end_specification: Optional[DateSpecificationGraphQL] = Field(default=None, repr=False, alias="endSpecification")
    scenarios: Optional[list[ShopScenarioGraphQL]] = Field(default=None, repr=False)

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
    @field_validator("start_specification", "end_specification", "scenarios", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopScenarioSet:
        """Convert this GraphQL format of shop scenario set to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopScenarioSet(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            start_specification=self.start_specification.as_read() if isinstance(self.start_specification, GraphQLCore) else self.start_specification,
            end_specification=self.end_specification.as_read() if isinstance(self.end_specification, GraphQLCore) else self.end_specification,
            scenarios=[scenario.as_read() for scenario in self.scenarios or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopScenarioSetWrite:
        """Convert this GraphQL format of shop scenario set to the writing format."""
        return ShopScenarioSetWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            start_specification=self.start_specification.as_write() if isinstance(self.start_specification, GraphQLCore) else self.start_specification,
            end_specification=self.end_specification.as_write() if isinstance(self.end_specification, GraphQLCore) else self.end_specification,
            scenarios=[scenario.as_write() for scenario in self.scenarios or []],
        )


class ShopScenarioSet(DomainModel):
    """This represents the reading version of shop scenario set.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop scenario set.
        data_record: The data record of the shop scenario set node.
        name: The name of the scenario set to run
        start_specification: TODO description
        end_specification: TODO description
        scenarios: Configuration of the partial bids that make up the total bid configuration
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopScenarioSet", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    start_specification: Union[DateSpecification, str, dm.NodeId, None] = Field(default=None, repr=False, alias="startSpecification")
    end_specification: Union[DateSpecification, str, dm.NodeId, None] = Field(default=None, repr=False, alias="endSpecification")
    scenarios: Optional[list[Union[ShopScenario, str, dm.NodeId]]] = Field(default=None, repr=False)

    def as_write(self) -> ShopScenarioSetWrite:
        """Convert this read version of shop scenario set to the writing version."""
        return ShopScenarioSetWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            start_specification=self.start_specification.as_write() if isinstance(self.start_specification, DomainModel) else self.start_specification,
            end_specification=self.end_specification.as_write() if isinstance(self.end_specification, DomainModel) else self.end_specification,
            scenarios=[scenario.as_write() if isinstance(scenario, DomainModel) else scenario for scenario in self.scenarios or []],
        )

    def as_apply(self) -> ShopScenarioSetWrite:
        """Convert this read version of shop scenario set to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopScenarioSetWrite(DomainModelWrite):
    """This represents the writing version of shop scenario set.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop scenario set.
        data_record: The data record of the shop scenario set node.
        name: The name of the scenario set to run
        start_specification: TODO description
        end_specification: TODO description
        scenarios: Configuration of the partial bids that make up the total bid configuration
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopScenarioSet", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    start_specification: Union[DateSpecificationWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="startSpecification")
    end_specification: Union[DateSpecificationWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="endSpecification")
    scenarios: Optional[list[Union[ShopScenarioWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

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

        if self.name is not None:
            properties["name"] = self.name

        if self.start_specification is not None:
            properties["startSpecification"] = {
                "space":  self.space if isinstance(self.start_specification, str) else self.start_specification.space,
                "externalId": self.start_specification if isinstance(self.start_specification, str) else self.start_specification.external_id,
            }

        if self.end_specification is not None:
            properties["endSpecification"] = {
                "space":  self.space if isinstance(self.end_specification, str) else self.end_specification.space,
                "externalId": self.end_specification if isinstance(self.end_specification, str) else self.end_specification.external_id,
            }


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



        edge_type = dm.DirectRelationReference("power_ops_types", "ShopScenarioSet.scenarios")
        for scenario in self.scenarios or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=scenario,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.start_specification, DomainModelWrite):
            other_resources = self.start_specification._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.end_specification, DomainModelWrite):
            other_resources = self.end_specification._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class ShopScenarioSetApply(ShopScenarioSetWrite):
    def __new__(cls, *args, **kwargs) -> ShopScenarioSetApply:
        warnings.warn(
            "ShopScenarioSetApply is deprecated and will be removed in v1.0. Use ShopScenarioSetWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopScenarioSet.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopScenarioSetList(DomainModelList[ShopScenarioSet]):
    """List of shop scenario sets in the read version."""

    _INSTANCE = ShopScenarioSet

    def as_write(self) -> ShopScenarioSetWriteList:
        """Convert these read versions of shop scenario set to the writing versions."""
        return ShopScenarioSetWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopScenarioSetWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopScenarioSetWriteList(DomainModelWriteList[ShopScenarioSetWrite]):
    """List of shop scenario sets in the writing version."""

    _INSTANCE = ShopScenarioSetWrite

class ShopScenarioSetApplyList(ShopScenarioSetWriteList): ...



def _create_shop_scenario_set_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    start_specification: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    end_specification: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if start_specification and isinstance(start_specification, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("startSpecification"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": start_specification}))
    if start_specification and isinstance(start_specification, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("startSpecification"), value={"space": start_specification[0], "externalId": start_specification[1]}))
    if start_specification and isinstance(start_specification, list) and isinstance(start_specification[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("startSpecification"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in start_specification]))
    if start_specification and isinstance(start_specification, list) and isinstance(start_specification[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("startSpecification"), values=[{"space": item[0], "externalId": item[1]} for item in start_specification]))
    if end_specification and isinstance(end_specification, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("endSpecification"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": end_specification}))
    if end_specification and isinstance(end_specification, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("endSpecification"), value={"space": end_specification[0], "externalId": end_specification[1]}))
    if end_specification and isinstance(end_specification, list) and isinstance(end_specification[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("endSpecification"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in end_specification]))
    if end_specification and isinstance(end_specification, list) and isinstance(end_specification[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("endSpecification"), values=[{"space": item[0], "externalId": item[1]} for item in end_specification]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
