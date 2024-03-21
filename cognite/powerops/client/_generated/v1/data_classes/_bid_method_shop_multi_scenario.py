from __future__ import annotations

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
from ._bid_method_day_ahead import BidMethodDayAhead, BidMethodDayAheadWrite

if TYPE_CHECKING:
    from ._scenario import Scenario, ScenarioGraphQL, ScenarioWrite


__all__ = [
    "BidMethodSHOPMultiScenario",
    "BidMethodSHOPMultiScenarioWrite",
    "BidMethodSHOPMultiScenarioApply",
    "BidMethodSHOPMultiScenarioList",
    "BidMethodSHOPMultiScenarioWriteList",
    "BidMethodSHOPMultiScenarioApplyList",
    "BidMethodSHOPMultiScenarioFields",
    "BidMethodSHOPMultiScenarioTextFields",
]


BidMethodSHOPMultiScenarioTextFields = Literal[
    "name", "shop_start_specification", "shop_end_specification", "shop_bid_date_specification"
]
BidMethodSHOPMultiScenarioFields = Literal[
    "name", "shop_start_specification", "shop_end_specification", "shop_bid_date_specification"
]

_BIDMETHODSHOPMULTISCENARIO_PROPERTIES_BY_FIELD = {
    "name": "name",
    "shop_start_specification": "shopStartSpecification",
    "shop_end_specification": "shopEndSpecification",
    "shop_bid_date_specification": "shopBidDateSpecification",
}


class BidMethodSHOPMultiScenarioGraphQL(GraphQLCore):
    """This represents the reading version of bid method shop multi scenario, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid method shop multi scenario.
        data_record: The data record of the bid method shop multi scenario node.
        name: Name for the BidMethod
        shop_start_specification: The shop start specification
        shop_end_specification: The shop end specification
        shop_bid_date_specification: The shop bid date specification
        scenarios: The scenarios to run this bid method with (includes incremental mappings and base mappings)
    """

    view_id = dm.ViewId("sp_powerops_models", "BidMethodSHOPMultiScenario", "1")
    name: Optional[str] = None
    shop_start_specification: Optional[str] = Field(None, alias="shopStartSpecification")
    shop_end_specification: Optional[str] = Field(None, alias="shopEndSpecification")
    shop_bid_date_specification: Optional[str] = Field(None, alias="shopBidDateSpecification")
    scenarios: Optional[list[ScenarioGraphQL]] = Field(default=None, repr=False)

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

    @field_validator("scenarios", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> BidMethodSHOPMultiScenario:
        """Convert this GraphQL format of bid method shop multi scenario to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidMethodSHOPMultiScenario(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            shop_start_specification=self.shop_start_specification,
            shop_end_specification=self.shop_end_specification,
            shop_bid_date_specification=self.shop_bid_date_specification,
            scenarios=[
                scenario.as_read() if isinstance(scenario, GraphQLCore) else scenario
                for scenario in self.scenarios or []
            ],
        )

    def as_write(self) -> BidMethodSHOPMultiScenarioWrite:
        """Convert this GraphQL format of bid method shop multi scenario to the writing format."""
        return BidMethodSHOPMultiScenarioWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            shop_start_specification=self.shop_start_specification,
            shop_end_specification=self.shop_end_specification,
            shop_bid_date_specification=self.shop_bid_date_specification,
            scenarios=[
                scenario.as_write() if isinstance(scenario, DomainModel) else scenario
                for scenario in self.scenarios or []
            ],
        )


class BidMethodSHOPMultiScenario(BidMethodDayAhead):
    """This represents the reading version of bid method shop multi scenario.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid method shop multi scenario.
        data_record: The data record of the bid method shop multi scenario node.
        name: Name for the BidMethod
        shop_start_specification: The shop start specification
        shop_end_specification: The shop end specification
        shop_bid_date_specification: The shop bid date specification
        scenarios: The scenarios to run this bid method with (includes incremental mappings and base mappings)
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "BidMethodSHOPMultiScenario"
    )
    shop_start_specification: Optional[str] = Field(None, alias="shopStartSpecification")
    shop_end_specification: Optional[str] = Field(None, alias="shopEndSpecification")
    shop_bid_date_specification: Optional[str] = Field(None, alias="shopBidDateSpecification")
    scenarios: Union[list[Scenario], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)

    def as_write(self) -> BidMethodSHOPMultiScenarioWrite:
        """Convert this read version of bid method shop multi scenario to the writing version."""
        return BidMethodSHOPMultiScenarioWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            shop_start_specification=self.shop_start_specification,
            shop_end_specification=self.shop_end_specification,
            shop_bid_date_specification=self.shop_bid_date_specification,
            scenarios=[
                scenario.as_write() if isinstance(scenario, DomainModel) else scenario
                for scenario in self.scenarios or []
            ],
        )

    def as_apply(self) -> BidMethodSHOPMultiScenarioWrite:
        """Convert this read version of bid method shop multi scenario to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMethodSHOPMultiScenarioWrite(BidMethodDayAheadWrite):
    """This represents the writing version of bid method shop multi scenario.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid method shop multi scenario.
        data_record: The data record of the bid method shop multi scenario node.
        name: Name for the BidMethod
        shop_start_specification: The shop start specification
        shop_end_specification: The shop end specification
        shop_bid_date_specification: The shop bid date specification
        scenarios: The scenarios to run this bid method with (includes incremental mappings and base mappings)
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "BidMethodSHOPMultiScenario"
    )
    shop_start_specification: Optional[str] = Field(None, alias="shopStartSpecification")
    shop_end_specification: Optional[str] = Field(None, alias="shopEndSpecification")
    shop_bid_date_specification: Optional[str] = Field(None, alias="shopBidDateSpecification")
    scenarios: Union[list[ScenarioWrite], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)

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
            BidMethodSHOPMultiScenario, dm.ViewId("sp_powerops_models", "BidMethodSHOPMultiScenario", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.shop_start_specification is not None or write_none:
            properties["shopStartSpecification"] = self.shop_start_specification

        if self.shop_end_specification is not None or write_none:
            properties["shopEndSpecification"] = self.shop_end_specification

        if self.shop_bid_date_specification is not None or write_none:
            properties["shopBidDateSpecification"] = self.shop_bid_date_specification

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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "BidMethodDayahead.scenarios")
        for scenario in self.scenarios or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=scenario,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class BidMethodSHOPMultiScenarioApply(BidMethodSHOPMultiScenarioWrite):
    def __new__(cls, *args, **kwargs) -> BidMethodSHOPMultiScenarioApply:
        warnings.warn(
            "BidMethodSHOPMultiScenarioApply is deprecated and will be removed in v1.0. Use BidMethodSHOPMultiScenarioWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidMethodSHOPMultiScenario.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidMethodSHOPMultiScenarioList(DomainModelList[BidMethodSHOPMultiScenario]):
    """List of bid method shop multi scenarios in the read version."""

    _INSTANCE = BidMethodSHOPMultiScenario

    def as_write(self) -> BidMethodSHOPMultiScenarioWriteList:
        """Convert these read versions of bid method shop multi scenario to the writing versions."""
        return BidMethodSHOPMultiScenarioWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidMethodSHOPMultiScenarioWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMethodSHOPMultiScenarioWriteList(DomainModelWriteList[BidMethodSHOPMultiScenarioWrite]):
    """List of bid method shop multi scenarios in the writing version."""

    _INSTANCE = BidMethodSHOPMultiScenarioWrite


class BidMethodSHOPMultiScenarioApplyList(BidMethodSHOPMultiScenarioWriteList): ...


def _create_bid_method_shop_multi_scenario_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    shop_start_specification: str | list[str] | None = None,
    shop_start_specification_prefix: str | None = None,
    shop_end_specification: str | list[str] | None = None,
    shop_end_specification_prefix: str | None = None,
    shop_bid_date_specification: str | list[str] | None = None,
    shop_bid_date_specification_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(shop_start_specification, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("shopStartSpecification"), value=shop_start_specification)
        )
    if shop_start_specification and isinstance(shop_start_specification, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("shopStartSpecification"), values=shop_start_specification)
        )
    if shop_start_specification_prefix is not None:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("shopStartSpecification"), value=shop_start_specification_prefix)
        )
    if isinstance(shop_end_specification, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopEndSpecification"), value=shop_end_specification))
    if shop_end_specification and isinstance(shop_end_specification, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopEndSpecification"), values=shop_end_specification))
    if shop_end_specification_prefix is not None:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("shopEndSpecification"), value=shop_end_specification_prefix)
        )
    if isinstance(shop_bid_date_specification, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("shopBidDateSpecification"), value=shop_bid_date_specification)
        )
    if shop_bid_date_specification and isinstance(shop_bid_date_specification, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("shopBidDateSpecification"), values=shop_bid_date_specification)
        )
    if shop_bid_date_specification_prefix is not None:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("shopBidDateSpecification"), value=shop_bid_date_specification_prefix
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
