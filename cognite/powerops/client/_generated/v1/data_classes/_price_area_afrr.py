from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

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
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)
from ._price_area import PriceArea, PriceAreaWrite


__all__ = [
    "PriceAreaAFRR",
    "PriceAreaAFRRWrite",
    "PriceAreaAFRRApply",
    "PriceAreaAFRRList",
    "PriceAreaAFRRWriteList",
    "PriceAreaAFRRApplyList",
    "PriceAreaAFRRFields",
    "PriceAreaAFRRTextFields",
    "PriceAreaAFRRGraphQL",
]


PriceAreaAFRRTextFields = Literal["name", "display_name", "asset_type", "capacity_price_up", "capacity_price_down", "activation_price_up", "activation_price_down", "relative_activation", "total_capacity_allocation_up", "total_capacity_allocation_down", "own_capacity_allocation_up", "own_capacity_allocation_down"]
PriceAreaAFRRFields = Literal["name", "display_name", "ordering", "asset_type", "capacity_price_up", "capacity_price_down", "activation_price_up", "activation_price_down", "relative_activation", "total_capacity_allocation_up", "total_capacity_allocation_down", "own_capacity_allocation_up", "own_capacity_allocation_down"]

_PRICEAREAAFRR_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
    "asset_type": "assetType",
    "capacity_price_up": "capacityPriceUp",
    "capacity_price_down": "capacityPriceDown",
    "activation_price_up": "activationPriceUp",
    "activation_price_down": "activationPriceDown",
    "relative_activation": "relativeActivation",
    "total_capacity_allocation_up": "totalCapacityAllocationUp",
    "total_capacity_allocation_down": "totalCapacityAllocationDown",
    "own_capacity_allocation_up": "ownCapacityAllocationUp",
    "own_capacity_allocation_down": "ownCapacityAllocationDown",
}

class PriceAreaAFRRGraphQL(GraphQLCore):
    """This represents the reading version of price area afrr, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area afrr.
        data_record: The data record of the price area afrr node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        capacity_price_up: The capacity price up field.
        capacity_price_down: The capacity price down field.
        activation_price_up: The mFRR activation price (TBC)
        activation_price_down: The mFRR activate price (TBC)
        relative_activation: Value between -1 (100 % activation down) and 1 (100 % activation down)
        total_capacity_allocation_up: The total capacity allocation up field.
        total_capacity_allocation_down: The total capacity allocation down field.
        own_capacity_allocation_up: The own capacity allocation up field.
        own_capacity_allocation_down: The own capacity allocation down field.
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaAFRR", "1")
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    capacity_price_up: Union[TimeSeries, dict, None] = Field(None, alias="capacityPriceUp")
    capacity_price_down: Union[TimeSeries, dict, None] = Field(None, alias="capacityPriceDown")
    activation_price_up: Union[TimeSeries, dict, None] = Field(None, alias="activationPriceUp")
    activation_price_down: Union[TimeSeries, dict, None] = Field(None, alias="activationPriceDown")
    relative_activation: Union[TimeSeries, dict, None] = Field(None, alias="relativeActivation")
    total_capacity_allocation_up: Union[TimeSeries, dict, None] = Field(None, alias="totalCapacityAllocationUp")
    total_capacity_allocation_down: Union[TimeSeries, dict, None] = Field(None, alias="totalCapacityAllocationDown")
    own_capacity_allocation_up: Union[TimeSeries, dict, None] = Field(None, alias="ownCapacityAllocationUp")
    own_capacity_allocation_down: Union[TimeSeries, dict, None] = Field(None, alias="ownCapacityAllocationDown")

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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> PriceAreaAFRR:
        """Convert this GraphQL format of price area afrr to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PriceAreaAFRR(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            capacity_price_up=self.capacity_price_up,
            capacity_price_down=self.capacity_price_down,
            activation_price_up=self.activation_price_up,
            activation_price_down=self.activation_price_down,
            relative_activation=self.relative_activation,
            total_capacity_allocation_up=self.total_capacity_allocation_up,
            total_capacity_allocation_down=self.total_capacity_allocation_down,
            own_capacity_allocation_up=self.own_capacity_allocation_up,
            own_capacity_allocation_down=self.own_capacity_allocation_down,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PriceAreaAFRRWrite:
        """Convert this GraphQL format of price area afrr to the writing format."""
        return PriceAreaAFRRWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            capacity_price_up=self.capacity_price_up,
            capacity_price_down=self.capacity_price_down,
            activation_price_up=self.activation_price_up,
            activation_price_down=self.activation_price_down,
            relative_activation=self.relative_activation,
            total_capacity_allocation_up=self.total_capacity_allocation_up,
            total_capacity_allocation_down=self.total_capacity_allocation_down,
            own_capacity_allocation_up=self.own_capacity_allocation_up,
            own_capacity_allocation_down=self.own_capacity_allocation_down,
        )


class PriceAreaAFRR(PriceArea):
    """This represents the reading version of price area afrr.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area afrr.
        data_record: The data record of the price area afrr node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        capacity_price_up: The capacity price up field.
        capacity_price_down: The capacity price down field.
        activation_price_up: The mFRR activation price (TBC)
        activation_price_down: The mFRR activate price (TBC)
        relative_activation: Value between -1 (100 % activation down) and 1 (100 % activation down)
        total_capacity_allocation_up: The total capacity allocation up field.
        total_capacity_allocation_down: The total capacity allocation down field.
        own_capacity_allocation_up: The own capacity allocation up field.
        own_capacity_allocation_down: The own capacity allocation down field.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaAFRR", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    capacity_price_up: Union[TimeSeries, str, None] = Field(None, alias="capacityPriceUp")
    capacity_price_down: Union[TimeSeries, str, None] = Field(None, alias="capacityPriceDown")
    activation_price_up: Union[TimeSeries, str, None] = Field(None, alias="activationPriceUp")
    activation_price_down: Union[TimeSeries, str, None] = Field(None, alias="activationPriceDown")
    relative_activation: Union[TimeSeries, str, None] = Field(None, alias="relativeActivation")
    total_capacity_allocation_up: Union[TimeSeries, str, None] = Field(None, alias="totalCapacityAllocationUp")
    total_capacity_allocation_down: Union[TimeSeries, str, None] = Field(None, alias="totalCapacityAllocationDown")
    own_capacity_allocation_up: Union[TimeSeries, str, None] = Field(None, alias="ownCapacityAllocationUp")
    own_capacity_allocation_down: Union[TimeSeries, str, None] = Field(None, alias="ownCapacityAllocationDown")

    def as_write(self) -> PriceAreaAFRRWrite:
        """Convert this read version of price area afrr to the writing version."""
        return PriceAreaAFRRWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            capacity_price_up=self.capacity_price_up,
            capacity_price_down=self.capacity_price_down,
            activation_price_up=self.activation_price_up,
            activation_price_down=self.activation_price_down,
            relative_activation=self.relative_activation,
            total_capacity_allocation_up=self.total_capacity_allocation_up,
            total_capacity_allocation_down=self.total_capacity_allocation_down,
            own_capacity_allocation_up=self.own_capacity_allocation_up,
            own_capacity_allocation_down=self.own_capacity_allocation_down,
        )

    def as_apply(self) -> PriceAreaAFRRWrite:
        """Convert this read version of price area afrr to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaAFRRWrite(PriceAreaWrite):
    """This represents the writing version of price area afrr.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area afrr.
        data_record: The data record of the price area afrr node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        capacity_price_up: The capacity price up field.
        capacity_price_down: The capacity price down field.
        activation_price_up: The mFRR activation price (TBC)
        activation_price_down: The mFRR activate price (TBC)
        relative_activation: Value between -1 (100 % activation down) and 1 (100 % activation down)
        total_capacity_allocation_up: The total capacity allocation up field.
        total_capacity_allocation_down: The total capacity allocation down field.
        own_capacity_allocation_up: The own capacity allocation up field.
        own_capacity_allocation_down: The own capacity allocation down field.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaAFRR", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    capacity_price_up: Union[TimeSeries, str, None] = Field(None, alias="capacityPriceUp")
    capacity_price_down: Union[TimeSeries, str, None] = Field(None, alias="capacityPriceDown")
    activation_price_up: Union[TimeSeries, str, None] = Field(None, alias="activationPriceUp")
    activation_price_down: Union[TimeSeries, str, None] = Field(None, alias="activationPriceDown")
    relative_activation: Union[TimeSeries, str, None] = Field(None, alias="relativeActivation")
    total_capacity_allocation_up: Union[TimeSeries, str, None] = Field(None, alias="totalCapacityAllocationUp")
    total_capacity_allocation_down: Union[TimeSeries, str, None] = Field(None, alias="totalCapacityAllocationDown")
    own_capacity_allocation_up: Union[TimeSeries, str, None] = Field(None, alias="ownCapacityAllocationUp")
    own_capacity_allocation_down: Union[TimeSeries, str, None] = Field(None, alias="ownCapacityAllocationDown")

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

        if self.display_name is not None or write_none:
            properties["displayName"] = self.display_name

        if self.ordering is not None or write_none:
            properties["ordering"] = self.ordering

        if self.asset_type is not None or write_none:
            properties["assetType"] = self.asset_type

        if self.capacity_price_up is not None or write_none:
            properties["capacityPriceUp"] = self.capacity_price_up if isinstance(self.capacity_price_up, str) or self.capacity_price_up is None else self.capacity_price_up.external_id

        if self.capacity_price_down is not None or write_none:
            properties["capacityPriceDown"] = self.capacity_price_down if isinstance(self.capacity_price_down, str) or self.capacity_price_down is None else self.capacity_price_down.external_id

        if self.activation_price_up is not None or write_none:
            properties["activationPriceUp"] = self.activation_price_up if isinstance(self.activation_price_up, str) or self.activation_price_up is None else self.activation_price_up.external_id

        if self.activation_price_down is not None or write_none:
            properties["activationPriceDown"] = self.activation_price_down if isinstance(self.activation_price_down, str) or self.activation_price_down is None else self.activation_price_down.external_id

        if self.relative_activation is not None or write_none:
            properties["relativeActivation"] = self.relative_activation if isinstance(self.relative_activation, str) or self.relative_activation is None else self.relative_activation.external_id

        if self.total_capacity_allocation_up is not None or write_none:
            properties["totalCapacityAllocationUp"] = self.total_capacity_allocation_up if isinstance(self.total_capacity_allocation_up, str) or self.total_capacity_allocation_up is None else self.total_capacity_allocation_up.external_id

        if self.total_capacity_allocation_down is not None or write_none:
            properties["totalCapacityAllocationDown"] = self.total_capacity_allocation_down if isinstance(self.total_capacity_allocation_down, str) or self.total_capacity_allocation_down is None else self.total_capacity_allocation_down.external_id

        if self.own_capacity_allocation_up is not None or write_none:
            properties["ownCapacityAllocationUp"] = self.own_capacity_allocation_up if isinstance(self.own_capacity_allocation_up, str) or self.own_capacity_allocation_up is None else self.own_capacity_allocation_up.external_id

        if self.own_capacity_allocation_down is not None or write_none:
            properties["ownCapacityAllocationDown"] = self.own_capacity_allocation_down if isinstance(self.own_capacity_allocation_down, str) or self.own_capacity_allocation_down is None else self.own_capacity_allocation_down.external_id


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



        if isinstance(self.capacity_price_up, CogniteTimeSeries):
            resources.time_series.append(self.capacity_price_up)

        if isinstance(self.capacity_price_down, CogniteTimeSeries):
            resources.time_series.append(self.capacity_price_down)

        if isinstance(self.activation_price_up, CogniteTimeSeries):
            resources.time_series.append(self.activation_price_up)

        if isinstance(self.activation_price_down, CogniteTimeSeries):
            resources.time_series.append(self.activation_price_down)

        if isinstance(self.relative_activation, CogniteTimeSeries):
            resources.time_series.append(self.relative_activation)

        if isinstance(self.total_capacity_allocation_up, CogniteTimeSeries):
            resources.time_series.append(self.total_capacity_allocation_up)

        if isinstance(self.total_capacity_allocation_down, CogniteTimeSeries):
            resources.time_series.append(self.total_capacity_allocation_down)

        if isinstance(self.own_capacity_allocation_up, CogniteTimeSeries):
            resources.time_series.append(self.own_capacity_allocation_up)

        if isinstance(self.own_capacity_allocation_down, CogniteTimeSeries):
            resources.time_series.append(self.own_capacity_allocation_down)

        return resources


class PriceAreaAFRRApply(PriceAreaAFRRWrite):
    def __new__(cls, *args, **kwargs) -> PriceAreaAFRRApply:
        warnings.warn(
            "PriceAreaAFRRApply is deprecated and will be removed in v1.0. Use PriceAreaAFRRWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PriceAreaAFRR.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PriceAreaAFRRList(DomainModelList[PriceAreaAFRR]):
    """List of price area afrrs in the read version."""

    _INSTANCE = PriceAreaAFRR

    def as_write(self) -> PriceAreaAFRRWriteList:
        """Convert these read versions of price area afrr to the writing versions."""
        return PriceAreaAFRRWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PriceAreaAFRRWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaAFRRWriteList(DomainModelWriteList[PriceAreaAFRRWrite]):
    """List of price area afrrs in the writing version."""

    _INSTANCE = PriceAreaAFRRWrite

class PriceAreaAFRRApplyList(PriceAreaAFRRWriteList): ...



def _create_price_area_afrr_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    display_name: str | list[str] | None = None,
    display_name_prefix: str | None = None,
    min_ordering: int | None = None,
    max_ordering: int | None = None,
    asset_type: str | list[str] | None = None,
    asset_type_prefix: str | None = None,
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
    if isinstance(display_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("displayName"), value=display_name))
    if display_name and isinstance(display_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("displayName"), values=display_name))
    if display_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("displayName"), value=display_name_prefix))
    if min_ordering is not None or max_ordering is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("ordering"), gte=min_ordering, lte=max_ordering))
    if isinstance(asset_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetType"), value=asset_type))
    if asset_type and isinstance(asset_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetType"), values=asset_type))
    if asset_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetType"), value=asset_type_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
