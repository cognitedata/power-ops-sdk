from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
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
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
    TimeSeries,
    TimeSeriesWrite,
    TimeSeriesGraphQL,
    TimeSeriesReferenceAPI,
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
    from cognite.powerops.client._generated.v1.data_classes._shop_result import ShopResult, ShopResultList, ShopResultGraphQL, ShopResultWrite, ShopResultWriteList


__all__ = [
    "PriceProduction",
    "PriceProductionWrite",
    "PriceProductionApply",
    "PriceProductionList",
    "PriceProductionWriteList",
    "PriceProductionApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> PriceProduction:
        """Convert this GraphQL format of price production to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PriceProduction(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            price=self.price.as_read() if self.price else None,
            production=self.production.as_read() if self.production else None,
            shop_result=self.shop_result.as_read()
if isinstance(self.shop_result, GraphQLCore)
else self.shop_result,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PriceProductionWrite:
        """Convert this GraphQL format of price production to the writing format."""
        return PriceProductionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            price=self.price.as_write() if self.price else None,
            production=self.production.as_write() if self.production else None,
            shop_result=self.shop_result.as_write()
if isinstance(self.shop_result, GraphQLCore)
else self.shop_result,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PriceProductionWrite:
        """Convert this read version of price production to the writing version."""
        return PriceProductionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            price=self.price.as_write() if isinstance(self.price, CogniteTimeSeries) else self.price,
            production=self.production.as_write() if isinstance(self.production, CogniteTimeSeries) else self.production,
            shop_result=self.shop_result.as_write()
if isinstance(self.shop_result, DomainModel)
else self.shop_result,
        )

    def as_apply(self) -> PriceProductionWrite:
        """Convert this read version of price production to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, PriceProduction],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._shop_result import ShopResult
        for instance in instances.values():
            if isinstance(instance.shop_result, (dm.NodeId, str)) and (shop_result := nodes_by_id.get(instance.shop_result)) and isinstance(
                    shop_result, ShopResult
            ):
                instance.shop_result = shop_result


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

        if self.price is not None or write_none:
            properties["price"] = self.price if isinstance(self.price, str) or self.price is None else self.price.external_id

        if self.production is not None or write_none:
            properties["production"] = self.production if isinstance(self.production, str) or self.production is None else self.production.external_id

        if self.shop_result is not None:
            properties["shopResult"] = {
                "space":  self.space if isinstance(self.shop_result, str) else self.shop_result.space,
                "externalId": self.shop_result if isinstance(self.shop_result, str) else self.shop_result.external_id,
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

        if isinstance(self.shop_result, DomainModelWrite):
            other_resources = self.shop_result._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.price, CogniteTimeSeriesWrite):
            resources.time_series.append(self.price)

        if isinstance(self.production, CogniteTimeSeriesWrite):
            resources.time_series.append(self.production)

        return resources


class PriceProductionApply(PriceProductionWrite):
    def __new__(cls, *args, **kwargs) -> PriceProductionApply:
        warnings.warn(
            "PriceProductionApply is deprecated and will be removed in v1.0. Use PriceProductionWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PriceProduction.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class PriceProductionList(DomainModelList[PriceProduction]):
    """List of price productions in the read version."""

    _INSTANCE = PriceProduction
    def as_write(self) -> PriceProductionWriteList:
        """Convert these read versions of price production to the writing versions."""
        return PriceProductionWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PriceProductionWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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

class PriceProductionApplyList(PriceProductionWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _ShopResultQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
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
