from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    T_DomainModelList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,

)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._date_specification import DateSpecification, DateSpecificationList, DateSpecificationGraphQL, DateSpecificationWrite, DateSpecificationWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_scenario import ShopScenario, ShopScenarioList, ShopScenarioGraphQL, ShopScenarioWrite, ShopScenarioWriteList


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


ShopScenarioSetTextFields = Literal["external_id", "name"]
ShopScenarioSetFields = Literal["external_id", "name"]

_SHOPSCENARIOSET_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            start_specification=self.start_specification.as_read()
if isinstance(self.start_specification, GraphQLCore)
else self.start_specification,
            end_specification=self.end_specification.as_read()
if isinstance(self.end_specification, GraphQLCore)
else self.end_specification,
            scenarios=[scenario.as_read() for scenario in self.scenarios] if self.scenarios is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopScenarioSetWrite:
        """Convert this GraphQL format of shop scenario set to the writing format."""
        return ShopScenarioSetWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            start_specification=self.start_specification.as_write()
if isinstance(self.start_specification, GraphQLCore)
else self.start_specification,
            end_specification=self.end_specification.as_write()
if isinstance(self.end_specification, GraphQLCore)
else self.end_specification,
            scenarios=[scenario.as_write() for scenario in self.scenarios] if self.scenarios is not None else None,
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopScenarioSetWrite:
        """Convert this read version of shop scenario set to the writing version."""
        return ShopScenarioSetWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            start_specification=self.start_specification.as_write()
if isinstance(self.start_specification, DomainModel)
else self.start_specification,
            end_specification=self.end_specification.as_write()
if isinstance(self.end_specification, DomainModel)
else self.end_specification,
            scenarios=[scenario.as_write() if isinstance(scenario, DomainModel) else scenario for scenario in self.scenarios] if self.scenarios is not None else None,
        )

    def as_apply(self) -> ShopScenarioSetWrite:
        """Convert this read version of shop scenario set to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ShopScenarioSet],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._date_specification import DateSpecification
        from ._shop_scenario import ShopScenario
        for instance in instances.values():
            if isinstance(instance.start_specification, (dm.NodeId, str)) and (start_specification := nodes_by_id.get(instance.start_specification)) and isinstance(
                    start_specification, DateSpecification
            ):
                instance.start_specification = start_specification
            if isinstance(instance.end_specification, (dm.NodeId, str)) and (end_specification := nodes_by_id.get(instance.end_specification)) and isinstance(
                    end_specification, DateSpecification
            ):
                instance.end_specification = end_specification
            if edges := edges_by_source_node.get(instance.as_id()):
                scenarios: list[ShopScenario | str | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("power_ops_types", "ShopScenarioSet.scenarios") and isinstance(
                        value, (ShopScenario, str, dm.NodeId)
                    ):
                        scenarios.append(value)

                instance.scenarios = scenarios or None



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
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    name: str
    start_specification: Union[DateSpecificationWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="startSpecification")
    end_specification: Union[DateSpecificationWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="endSpecification")
    scenarios: Optional[list[Union[ShopScenarioWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

    @field_validator("start_specification", "end_specification", "scenarios", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value

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
                type=as_direct_relation_reference(self.node_type),
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

    @property
    def start_specification(self) -> DateSpecificationList:
        from ._date_specification import DateSpecification, DateSpecificationList
        return DateSpecificationList([item.start_specification for item in self.data if isinstance(item.start_specification, DateSpecification)])
    @property
    def end_specification(self) -> DateSpecificationList:
        from ._date_specification import DateSpecification, DateSpecificationList
        return DateSpecificationList([item.end_specification for item in self.data if isinstance(item.end_specification, DateSpecification)])
    @property
    def scenarios(self) -> ShopScenarioList:
        from ._shop_scenario import ShopScenario, ShopScenarioList
        return ShopScenarioList([item for items in self.data for item in items.scenarios or [] if isinstance(item, ShopScenario)])


class ShopScenarioSetWriteList(DomainModelWriteList[ShopScenarioSetWrite]):
    """List of shop scenario sets in the writing version."""

    _INSTANCE = ShopScenarioSetWrite
    @property
    def start_specification(self) -> DateSpecificationWriteList:
        from ._date_specification import DateSpecificationWrite, DateSpecificationWriteList
        return DateSpecificationWriteList([item.start_specification for item in self.data if isinstance(item.start_specification, DateSpecificationWrite)])
    @property
    def end_specification(self) -> DateSpecificationWriteList:
        from ._date_specification import DateSpecificationWrite, DateSpecificationWriteList
        return DateSpecificationWriteList([item.end_specification for item in self.data if isinstance(item.end_specification, DateSpecificationWrite)])
    @property
    def scenarios(self) -> ShopScenarioWriteList:
        from ._shop_scenario import ShopScenarioWrite, ShopScenarioWriteList
        return ShopScenarioWriteList([item for items in self.data for item in items.scenarios or [] if isinstance(item, ShopScenarioWrite)])


class ShopScenarioSetApplyList(ShopScenarioSetWriteList): ...


def _create_shop_scenario_set_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    start_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    end_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(start_specification, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(start_specification):
        filters.append(dm.filters.Equals(view_id.as_property_ref("startSpecification"), value=as_instance_dict_id(start_specification)))
    if start_specification and isinstance(start_specification, Sequence) and not isinstance(start_specification, str) and not is_tuple_id(start_specification):
        filters.append(dm.filters.In(view_id.as_property_ref("startSpecification"), values=[as_instance_dict_id(item) for item in start_specification]))
    if isinstance(end_specification, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(end_specification):
        filters.append(dm.filters.Equals(view_id.as_property_ref("endSpecification"), value=as_instance_dict_id(end_specification)))
    if end_specification and isinstance(end_specification, Sequence) and not isinstance(end_specification, str) and not is_tuple_id(end_specification):
        filters.append(dm.filters.In(view_id.as_property_ref("endSpecification"), values=[as_instance_dict_id(item) for item in end_specification]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopScenarioSetQuery(NodeQueryCore[T_DomainModelList, ShopScenarioSetList]):
    _view_id = ShopScenarioSet._view_id
    _result_cls = ShopScenarioSet
    _result_list_cls_end = ShopScenarioSetList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._date_specification import _DateSpecificationQuery
        from ._shop_scenario import _ShopScenarioQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_type,
            reverse_expression,
        )

        if _DateSpecificationQuery not in created_types:
            self.start_specification = _DateSpecificationQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("startSpecification"),
                    direction="outwards",
                ),
                connection_name="start_specification",
            )

        if _DateSpecificationQuery not in created_types:
            self.end_specification = _DateSpecificationQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("endSpecification"),
                    direction="outwards",
                ),
                connection_name="end_specification",
            )

        if _ShopScenarioQuery not in created_types:
            self.scenarios = _ShopScenarioQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="scenarios",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
        ])

    def list_shop_scenario_set(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopScenarioSetList:
        return self._list(limit=limit)


class ShopScenarioSetQuery(_ShopScenarioSetQuery[ShopScenarioSetList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopScenarioSetList)
