from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._case import Case, CaseGraphQL, CaseWrite


__all__ = [
    "PriceProdCase",
    "PriceProdCaseWrite",
    "PriceProdCaseApply",
    "PriceProdCaseList",
    "PriceProdCaseWriteList",
    "PriceProdCaseApplyList",
    "PriceProdCaseFields",
    "PriceProdCaseTextFields",
]


PriceProdCaseTextFields = Literal["price", "production"]
PriceProdCaseFields = Literal["price", "production"]

_PRICEPRODCASE_PROPERTIES_BY_FIELD = {
    "price": "price",
    "production": "production",
}


class PriceProdCaseGraphQL(GraphQLCore):
    """This represents the reading version of price prod case, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price prod case.
        data_record: The data record of the price prod case node.
        price: The price field.
        production: The production field.
        case: The case field.
    """

    view_id = dm.ViewId("sp_powerops_models", "PriceProdCase", "1")
    price: Union[TimeSeries, str, None] = None
    production: Union[TimeSeries, str, None] = None
    case: Optional[CaseGraphQL] = Field(None, repr=False)

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

    @field_validator("case", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> PriceProdCase:
        """Convert this GraphQL format of price prod case to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PriceProdCase(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            price=self.price,
            production=self.production,
            case=self.case.as_read() if isinstance(self.case, GraphQLCore) else self.case,
        )

    def as_write(self) -> PriceProdCaseWrite:
        """Convert this GraphQL format of price prod case to the writing format."""
        return PriceProdCaseWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            price=self.price,
            production=self.production,
            case=self.case.as_write() if isinstance(self.case, DomainModel) else self.case,
        )


class PriceProdCase(DomainModel):
    """This represents the reading version of price prod case.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price prod case.
        data_record: The data record of the price prod case node.
        price: The price field.
        production: The production field.
        case: The case field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PriceProdCase"
    )
    price: Union[TimeSeries, str, None] = None
    production: Union[TimeSeries, str, None] = None
    case: Union[Case, str, dm.NodeId, None] = Field(None, repr=False)

    def as_write(self) -> PriceProdCaseWrite:
        """Convert this read version of price prod case to the writing version."""
        return PriceProdCaseWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            price=self.price,
            production=self.production,
            case=self.case.as_write() if isinstance(self.case, DomainModel) else self.case,
        )

    def as_apply(self) -> PriceProdCaseWrite:
        """Convert this read version of price prod case to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceProdCaseWrite(DomainModelWrite):
    """This represents the writing version of price prod case.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price prod case.
        data_record: The data record of the price prod case node.
        price: The price field.
        production: The production field.
        case: The case field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PriceProdCase"
    )
    price: Union[TimeSeries, str, None] = None
    production: Union[TimeSeries, str, None] = None
    case: Union[CaseWrite, str, dm.NodeId, None] = Field(None, repr=False)

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            PriceProdCase, dm.ViewId("sp_powerops_models", "PriceProdCase", "1")
        )

        properties: dict[str, Any] = {}

        if self.price is not None or write_none:
            if isinstance(self.price, str) or self.price is None:
                properties["price"] = self.price
            else:
                properties["price"] = self.price.external_id

        if self.production is not None or write_none:
            if isinstance(self.production, str) or self.production is None:
                properties["production"] = self.production
            else:
                properties["production"] = self.production.external_id

        if self.case is not None:
            properties["case"] = {
                "space": self.space if isinstance(self.case, str) else self.case.space,
                "externalId": self.case if isinstance(self.case, str) else self.case.external_id,
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.case, DomainModelWrite):
            other_resources = self.case._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.price, CogniteTimeSeries):
            resources.time_series.append(self.price)

        if isinstance(self.production, CogniteTimeSeries):
            resources.time_series.append(self.production)

        return resources


class PriceProdCaseApply(PriceProdCaseWrite):
    def __new__(cls, *args, **kwargs) -> PriceProdCaseApply:
        warnings.warn(
            "PriceProdCaseApply is deprecated and will be removed in v1.0. Use PriceProdCaseWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PriceProdCase.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PriceProdCaseList(DomainModelList[PriceProdCase]):
    """List of price prod cases in the read version."""

    _INSTANCE = PriceProdCase

    def as_write(self) -> PriceProdCaseWriteList:
        """Convert these read versions of price prod case to the writing versions."""
        return PriceProdCaseWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PriceProdCaseWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceProdCaseWriteList(DomainModelWriteList[PriceProdCaseWrite]):
    """List of price prod cases in the writing version."""

    _INSTANCE = PriceProdCaseWrite


class PriceProdCaseApplyList(PriceProdCaseWriteList): ...


def _create_price_prod_case_filter(
    view_id: dm.ViewId,
    case: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if case and isinstance(case, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("case"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": case}
            )
        )
    if case and isinstance(case, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("case"), value={"space": case[0], "externalId": case[1]})
        )
    if case and isinstance(case, list) and isinstance(case[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("case"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in case],
            )
        )
    if case and isinstance(case, list) and isinstance(case[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("case"), values=[{"space": item[0], "externalId": item[1]} for item in case]
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
