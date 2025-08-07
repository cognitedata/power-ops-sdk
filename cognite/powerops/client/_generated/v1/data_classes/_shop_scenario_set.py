from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from cognite.powerops.client._generated.v1.config import global_config
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
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    DirectRelationFilter,
)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._date_specification import DateSpecification, DateSpecificationList, DateSpecificationGraphQL, DateSpecificationWrite, DateSpecificationWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_scenario import ShopScenario, ShopScenarioList, ShopScenarioGraphQL, ShopScenarioWrite, ShopScenarioWriteList


__all__ = [
    "ShopScenarioSet",
    "ShopScenarioSetWrite",
    "ShopScenarioSetList",
    "ShopScenarioSetWriteList",
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

    def as_read(self) -> ShopScenarioSet:
        """Convert this GraphQL format of shop scenario set to the reading format."""
        return ShopScenarioSet.model_validate(as_read_args(self))

    def as_write(self) -> ShopScenarioSetWrite:
        """Convert this GraphQL format of shop scenario set to the writing format."""
        return ShopScenarioSetWrite.model_validate(as_write_args(self))


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
    @field_validator("start_specification", "end_specification", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("scenarios", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ShopScenarioSetWrite:
        """Convert this read version of shop scenario set to the writing version."""
        return ShopScenarioSetWrite.model_validate(as_write_args(self))



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
    _container_fields: ClassVar[tuple[str, ...]] = ("end_specification", "name", "start_specification",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("scenarios", dm.DirectRelationReference("power_ops_types", "ShopScenarioSet.scenarios")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("end_specification", "start_specification",)

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


class ShopScenarioSetList(DomainModelList[ShopScenarioSet]):
    """List of shop scenario sets in the read version."""

    _INSTANCE = ShopScenarioSet
    def as_write(self) -> ShopScenarioSetWriteList:
        """Convert these read versions of shop scenario set to the writing versions."""
        return ShopScenarioSetWriteList([node.as_write() for node in self.data])


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
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _DateSpecificationQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "startSpecification"),
            )

        if _DateSpecificationQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "endSpecification"),
            )

        if _ShopScenarioQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "scenarios"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.start_specification_filter = DirectRelationFilter(self, self._view_id.as_property_ref("startSpecification"))
        self.end_specification_filter = DirectRelationFilter(self, self._view_id.as_property_ref("endSpecification"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.start_specification_filter,
            self.end_specification_filter,
        ])

    def list_shop_scenario_set(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopScenarioSetList:
        return self._list(limit=limit)


class ShopScenarioSetQuery(_ShopScenarioSetQuery[ShopScenarioSetList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopScenarioSetList)
