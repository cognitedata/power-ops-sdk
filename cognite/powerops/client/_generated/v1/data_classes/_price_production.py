from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
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
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
    TimeSeries,
    TimeSeriesWrite,
    TimeSeriesGraphQL,
    TimeSeriesReferenceAPI,
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
    from cognite.powerops.client._generated.v1.data_classes._shop_result import ShopResult, ShopResultList, ShopResultGraphQL, ShopResultWrite, ShopResultWriteList


__all__ = [
    "PriceProduction",
    "PriceProductionWrite",
    "PriceProductionList",
    "PriceProductionWriteList",
    "PriceProductionFields",
    "PriceProductionTextFields",
    "PriceProductionGraphQL",
]


PriceProductionTextFields = Literal["external_id", "name", "price", "production"]
PriceProductionFields = Literal["external_id", "name", "price", "production"]

_PRICEPRODUCTION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "price": "price",
    "production": "production",
}


class PriceProductionGraphQL(GraphQLCore):
    """This represents the reading version of price production, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price production.
        data_record: The data record of the price production node.
        name: The name field.
        price: The price field.
        production: The production field.
        shop_result: The shop result field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceProduction", "1")
    name: Optional[str] = None
    price: Optional[TimeSeriesGraphQL] = None
    production: Optional[TimeSeriesGraphQL] = None
    shop_result: Optional[ShopResultGraphQL] = Field(default=None, repr=False, alias="shopResult")

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


    @field_validator("shop_result", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> PriceProduction:
        """Convert this GraphQL format of price production to the reading format."""
        return PriceProduction.model_validate(as_read_args(self))

    def as_write(self) -> PriceProductionWrite:
        """Convert this GraphQL format of price production to the writing format."""
        return PriceProductionWrite.model_validate(as_write_args(self))


class PriceProduction(DomainModel):
    """This represents the reading version of price production.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price production.
        data_record: The data record of the price production node.
        name: The name field.
        price: The price field.
        production: The production field.
        shop_result: The shop result field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceProduction", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "PriceProduction")
    name: str
    price: Union[TimeSeries, str, None] = None
    production: Union[TimeSeries, str, None] = None
    shop_result: Union[ShopResult, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopResult")
    @field_validator("shop_result", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)


    def as_write(self) -> PriceProductionWrite:
        """Convert this read version of price production to the writing version."""
        return PriceProductionWrite.model_validate(as_write_args(self))



class PriceProductionWrite(DomainModelWrite):
    """This represents the writing version of price production.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price production.
        data_record: The data record of the price production node.
        name: The name field.
        price: The price field.
        production: The production field.
        shop_result: The shop result field.
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("name", "price", "production", "shop_result",)
    _direct_relations: ClassVar[tuple[str, ...]] = ("shop_result",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceProduction", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "PriceProduction")
    name: str
    price: Union[TimeSeriesWrite, str, None] = None
    production: Union[TimeSeriesWrite, str, None] = None
    shop_result: Union[ShopResultWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopResult")

    @field_validator("shop_result", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class PriceProductionList(DomainModelList[PriceProduction]):
    """List of price productions in the read version."""

    _INSTANCE = PriceProduction
    def as_write(self) -> PriceProductionWriteList:
        """Convert these read versions of price production to the writing versions."""
        return PriceProductionWriteList([node.as_write() for node in self.data])


    @property
    def shop_result(self) -> ShopResultList:
        from ._shop_result import ShopResult, ShopResultList
        return ShopResultList([item.shop_result for item in self.data if isinstance(item.shop_result, ShopResult)])

class PriceProductionWriteList(DomainModelWriteList[PriceProductionWrite]):
    """List of price productions in the writing version."""

    _INSTANCE = PriceProductionWrite
    @property
    def shop_result(self) -> ShopResultWriteList:
        from ._shop_result import ShopResultWrite, ShopResultWriteList
        return ShopResultWriteList([item.shop_result for item in self.data if isinstance(item.shop_result, ShopResultWrite)])


def _create_price_production_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    shop_result: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(shop_result, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(shop_result):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopResult"), value=as_instance_dict_id(shop_result)))
    if shop_result and isinstance(shop_result, Sequence) and not isinstance(shop_result, str) and not is_tuple_id(shop_result):
        filters.append(dm.filters.In(view_id.as_property_ref("shopResult"), values=[as_instance_dict_id(item) for item in shop_result]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _PriceProductionQuery(NodeQueryCore[T_DomainModelList, PriceProductionList]):
    _view_id = PriceProduction._view_id
    _result_cls = PriceProduction
    _result_list_cls_end = PriceProductionList

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
        from ._shop_result import _ShopResultQuery

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

        if _ShopResultQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.shop_result = _ShopResultQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("shopResult"),
                    direction="outwards",
                ),
                connection_name="shop_result",
                connection_property=ViewPropertyId(self._view_id, "shopResult"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.shop_result_filter = DirectRelationFilter(self, self._view_id.as_property_ref("shopResult"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.shop_result_filter,
        ])
        self.price = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.price if isinstance(item.price, str) else item.price.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.price is not None and
               (isinstance(item.price, str) or item.price.external_id is not None)
        ])
        self.production = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.production if isinstance(item.production, str) else item.production.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.production is not None and
               (isinstance(item.production, str) or item.production.external_id is not None)
        ])

    def list_price_production(self, limit: int = DEFAULT_QUERY_LIMIT) -> PriceProductionList:
        return self._list(limit=limit)


class PriceProductionQuery(_PriceProductionQuery[PriceProductionList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PriceProductionList)
