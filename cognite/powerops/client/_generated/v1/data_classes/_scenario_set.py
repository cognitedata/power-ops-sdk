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

if TYPE_CHECKING:
    from ._scenario import Scenario, ScenarioGraphQL, ScenarioWrite


__all__ = [
    "ScenarioSet",
    "ScenarioSetWrite",
    "ScenarioSetApply",
    "ScenarioSetList",
    "ScenarioSetWriteList",
    "ScenarioSetApplyList",
    "ScenarioSetFields",
    "ScenarioSetTextFields",
]


ScenarioSetTextFields = Literal[
    "name", "shop_start_specification", "shop_end_specification", "shop_bid_date_specification"
]
ScenarioSetFields = Literal["name", "shop_start_specification", "shop_end_specification", "shop_bid_date_specification"]

_SCENARIOSET_PROPERTIES_BY_FIELD = {
    "name": "name",
    "shop_start_specification": "shopStartSpecification",
    "shop_end_specification": "shopEndSpecification",
    "shop_bid_date_specification": "shopBidDateSpecification",
}


class ScenarioSetGraphQL(GraphQLCore):
    """This represents the reading version of scenario set, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario set.
        data_record: The data record of the scenario set node.
        name: The name of the scenario set to run
        shop_start_specification: TODO definition
        shop_end_specification: TODO definition
        shop_bid_date_specification: TODO definition
        shop_scenarios: Configuration of the partial bids that make up the total bid configuration
    """

    view_id = dm.ViewId("sp_powerops_models_temp", "ScenarioSet", "1")
    name: Optional[str] = None
    shop_start_specification: Optional[str] = Field(None, alias="shopStartSpecification")
    shop_end_specification: Optional[str] = Field(None, alias="shopEndSpecification")
    shop_bid_date_specification: Optional[str] = Field(None, alias="shopBidDateSpecification")
    shop_scenarios: Optional[list[ScenarioGraphQL]] = Field(default=None, repr=False, alias="shopScenarios")

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

    @field_validator("shop_scenarios", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ScenarioSet:
        """Convert this GraphQL format of scenario set to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ScenarioSet(
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
            shop_scenarios=[
                shop_scenario.as_read() if isinstance(shop_scenario, GraphQLCore) else shop_scenario
                for shop_scenario in self.shop_scenarios or []
            ],
        )

    def as_write(self) -> ScenarioSetWrite:
        """Convert this GraphQL format of scenario set to the writing format."""
        return ScenarioSetWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            shop_start_specification=self.shop_start_specification,
            shop_end_specification=self.shop_end_specification,
            shop_bid_date_specification=self.shop_bid_date_specification,
            shop_scenarios=[
                shop_scenario.as_write() if isinstance(shop_scenario, DomainModel) else shop_scenario
                for shop_scenario in self.shop_scenarios or []
            ],
        )


class ScenarioSet(DomainModel):
    """This represents the reading version of scenario set.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario set.
        data_record: The data record of the scenario set node.
        name: The name of the scenario set to run
        shop_start_specification: TODO definition
        shop_end_specification: TODO definition
        shop_bid_date_specification: TODO definition
        shop_scenarios: Configuration of the partial bids that make up the total bid configuration
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    shop_start_specification: str = Field(alias="shopStartSpecification")
    shop_end_specification: str = Field(alias="shopEndSpecification")
    shop_bid_date_specification: str = Field(alias="shopBidDateSpecification")
    shop_scenarios: Union[list[Scenario], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="shopScenarios"
    )

    def as_write(self) -> ScenarioSetWrite:
        """Convert this read version of scenario set to the writing version."""
        return ScenarioSetWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            shop_start_specification=self.shop_start_specification,
            shop_end_specification=self.shop_end_specification,
            shop_bid_date_specification=self.shop_bid_date_specification,
            shop_scenarios=[
                shop_scenario.as_write() if isinstance(shop_scenario, DomainModel) else shop_scenario
                for shop_scenario in self.shop_scenarios or []
            ],
        )

    def as_apply(self) -> ScenarioSetWrite:
        """Convert this read version of scenario set to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ScenarioSetWrite(DomainModelWrite):
    """This represents the writing version of scenario set.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario set.
        data_record: The data record of the scenario set node.
        name: The name of the scenario set to run
        shop_start_specification: TODO definition
        shop_end_specification: TODO definition
        shop_bid_date_specification: TODO definition
        shop_scenarios: Configuration of the partial bids that make up the total bid configuration
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    shop_start_specification: str = Field(alias="shopStartSpecification")
    shop_end_specification: str = Field(alias="shopEndSpecification")
    shop_bid_date_specification: str = Field(alias="shopBidDateSpecification")
    shop_scenarios: Union[list[ScenarioWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="shopScenarios"
    )

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
            ScenarioSet, dm.ViewId("sp_powerops_models_temp", "ScenarioSet", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.shop_start_specification is not None:
            properties["shopStartSpecification"] = self.shop_start_specification

        if self.shop_end_specification is not None:
            properties["shopEndSpecification"] = self.shop_end_specification

        if self.shop_bid_date_specification is not None:
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

        edge_type = dm.DirectRelationReference("sp_powerops_types_temp", "ScenarioSet.scenarios")
        for shop_scenario in self.shop_scenarios or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=shop_scenario,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class ScenarioSetApply(ScenarioSetWrite):
    def __new__(cls, *args, **kwargs) -> ScenarioSetApply:
        warnings.warn(
            "ScenarioSetApply is deprecated and will be removed in v1.0. Use ScenarioSetWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ScenarioSet.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ScenarioSetList(DomainModelList[ScenarioSet]):
    """List of scenario sets in the read version."""

    _INSTANCE = ScenarioSet

    def as_write(self) -> ScenarioSetWriteList:
        """Convert these read versions of scenario set to the writing versions."""
        return ScenarioSetWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ScenarioSetWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ScenarioSetWriteList(DomainModelWriteList[ScenarioSetWrite]):
    """List of scenario sets in the writing version."""

    _INSTANCE = ScenarioSetWrite


class ScenarioSetApplyList(ScenarioSetWriteList): ...


def _create_scenario_set_filter(
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
