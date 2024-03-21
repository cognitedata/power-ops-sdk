from __future__ import annotations

import datetime
import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
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
)

if TYPE_CHECKING:
    from ._plant import Plant, PlantGraphQL, PlantWrite
    from ._price_area import PriceArea, PriceAreaGraphQL, PriceAreaWrite


__all__ = [
    "BidCalculationTask",
    "BidCalculationTaskWrite",
    "BidCalculationTaskApply",
    "BidCalculationTaskList",
    "BidCalculationTaskWriteList",
    "BidCalculationTaskApplyList",
    "BidCalculationTaskFields",
]

BidCalculationTaskFields = Literal["bid_date"]

_BIDCALCULATIONTASK_PROPERTIES_BY_FIELD = {
    "bid_date": "bidDate",
}


class BidCalculationTaskGraphQL(GraphQLCore):
    """This represents the reading version of bid calculation task, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid calculation task.
        data_record: The data record of the bid calculation task node.
        plant: The plant field.
        bid_date: The bid date that the task is for
        price_area: The price area related to the bid calculation task
    """

    view_id = dm.ViewId("sp_powerops_models", "BidCalculationTask", "1")
    plant: Optional[PlantGraphQL] = Field(None, repr=False)
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    price_area: Optional[PriceAreaGraphQL] = Field(None, repr=False, alias="priceArea")

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

    @field_validator("plant", "price_area", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> BidCalculationTask:
        """Convert this GraphQL format of bid calculation task to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidCalculationTask(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            plant=self.plant.as_read() if isinstance(self.plant, GraphQLCore) else self.plant,
            bid_date=self.bid_date,
            price_area=self.price_area.as_read() if isinstance(self.price_area, GraphQLCore) else self.price_area,
        )

    def as_write(self) -> BidCalculationTaskWrite:
        """Convert this GraphQL format of bid calculation task to the writing format."""
        return BidCalculationTaskWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            plant=self.plant.as_write() if isinstance(self.plant, DomainModel) else self.plant,
            bid_date=self.bid_date,
            price_area=self.price_area.as_write() if isinstance(self.price_area, DomainModel) else self.price_area,
        )


class BidCalculationTask(DomainModel):
    """This represents the reading version of bid calculation task.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid calculation task.
        data_record: The data record of the bid calculation task node.
        plant: The plant field.
        bid_date: The bid date that the task is for
        price_area: The price area related to the bid calculation task
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "BidCalculationTask"
    )
    plant: Union[Plant, str, dm.NodeId, None] = Field(None, repr=False)
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    price_area: Union[PriceArea, str, dm.NodeId, None] = Field(None, repr=False, alias="priceArea")

    def as_write(self) -> BidCalculationTaskWrite:
        """Convert this read version of bid calculation task to the writing version."""
        return BidCalculationTaskWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            plant=self.plant.as_write() if isinstance(self.plant, DomainModel) else self.plant,
            bid_date=self.bid_date,
            price_area=self.price_area.as_write() if isinstance(self.price_area, DomainModel) else self.price_area,
        )

    def as_apply(self) -> BidCalculationTaskWrite:
        """Convert this read version of bid calculation task to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidCalculationTaskWrite(DomainModelWrite):
    """This represents the writing version of bid calculation task.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid calculation task.
        data_record: The data record of the bid calculation task node.
        plant: The plant field.
        bid_date: The bid date that the task is for
        price_area: The price area related to the bid calculation task
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "BidCalculationTask"
    )
    plant: Union[PlantWrite, str, dm.NodeId, None] = Field(None, repr=False)
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    price_area: Union[PriceAreaWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="priceArea")

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
            BidCalculationTask, dm.ViewId("sp_powerops_models", "BidCalculationTask", "1")
        )

        properties: dict[str, Any] = {}

        if self.plant is not None:
            properties["plant"] = {
                "space": self.space if isinstance(self.plant, str) else self.plant.space,
                "externalId": self.plant if isinstance(self.plant, str) else self.plant.external_id,
            }

        if self.bid_date is not None or write_none:
            properties["bidDate"] = self.bid_date.isoformat() if self.bid_date else None

        if self.price_area is not None:
            properties["priceArea"] = {
                "space": self.space if isinstance(self.price_area, str) else self.price_area.space,
                "externalId": self.price_area if isinstance(self.price_area, str) else self.price_area.external_id,
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

        if isinstance(self.plant, DomainModelWrite):
            other_resources = self.plant._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.price_area, DomainModelWrite):
            other_resources = self.price_area._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class BidCalculationTaskApply(BidCalculationTaskWrite):
    def __new__(cls, *args, **kwargs) -> BidCalculationTaskApply:
        warnings.warn(
            "BidCalculationTaskApply is deprecated and will be removed in v1.0. Use BidCalculationTaskWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidCalculationTask.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidCalculationTaskList(DomainModelList[BidCalculationTask]):
    """List of bid calculation tasks in the read version."""

    _INSTANCE = BidCalculationTask

    def as_write(self) -> BidCalculationTaskWriteList:
        """Convert these read versions of bid calculation task to the writing versions."""
        return BidCalculationTaskWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidCalculationTaskWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidCalculationTaskWriteList(DomainModelWriteList[BidCalculationTaskWrite]):
    """List of bid calculation tasks in the writing version."""

    _INSTANCE = BidCalculationTaskWrite


class BidCalculationTaskApplyList(BidCalculationTaskWriteList): ...


def _create_bid_calculation_task_filter(
    view_id: dm.ViewId,
    plant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_bid_date: datetime.date | None = None,
    max_bid_date: datetime.date | None = None,
    price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if plant and isinstance(plant, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("plant"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": plant}
            )
        )
    if plant and isinstance(plant, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("plant"), value={"space": plant[0], "externalId": plant[1]})
        )
    if plant and isinstance(plant, list) and isinstance(plant[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("plant"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in plant],
            )
        )
    if plant and isinstance(plant, list) and isinstance(plant[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("plant"), values=[{"space": item[0], "externalId": item[1]} for item in plant]
            )
        )
    if min_bid_date is not None or max_bid_date is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("bidDate"),
                gte=min_bid_date.isoformat() if min_bid_date else None,
                lte=max_bid_date.isoformat() if max_bid_date else None,
            )
        )
    if price_area and isinstance(price_area, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("priceArea"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": price_area}
            )
        )
    if price_area and isinstance(price_area, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("priceArea"), value={"space": price_area[0], "externalId": price_area[1]}
            )
        )
    if price_area and isinstance(price_area, list) and isinstance(price_area[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("priceArea"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in price_area],
            )
        )
    if price_area and isinstance(price_area, list) and isinstance(price_area[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("priceArea"),
                values=[{"space": item[0], "externalId": item[1]} for item in price_area],
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
