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
    from ._scenario import Scenario, ScenarioGraphQL, ScenarioWrite


__all__ = [
    "Case",
    "CaseWrite",
    "CaseApply",
    "CaseList",
    "CaseWriteList",
    "CaseApplyList",
    "CaseFields",
    "CaseTextFields",
]


CaseTextFields = Literal["case_file", "reservoir_mapping", "cut_order_files", "extra_files"]
CaseFields = Literal[
    "case_file",
    "reservoir_mapping",
    "cut_order_files",
    "extra_files",
    "cog_shop_files_config",
    "start_time",
    "end_time",
]

_CASE_PROPERTIES_BY_FIELD = {
    "case_file": "caseFile",
    "reservoir_mapping": "reservoirMapping",
    "cut_order_files": "cutOrderFiles",
    "extra_files": "extraFiles",
    "cog_shop_files_config": "cogShopFilesConfig",
    "start_time": "startTime",
    "end_time": "endTime",
}


class CaseGraphQL(GraphQLCore):
    """This represents the reading version of case, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the case.
        data_record: The data record of the case node.
        scenario: The Shop scenario that was used to produce this result
        case_file: The case file used
        reservoir_mapping: The cut file reservoir mapping
        cut_order_files: Cut order files (Module series in PRODRISK)
        extra_files: The extra file field.
        cog_shop_files_config: Configuration for in what order to load the various files into pyshop
        start_time: The start time of the case
        end_time: The end time of the case
    """

    view_id = dm.ViewId("sp_powerops_models_temp", "Case", "1")
    scenario: Optional[ScenarioGraphQL] = Field(None, repr=False)
    case_file: Union[str, None] = Field(None, alias="caseFile")
    reservoir_mapping: Optional[list[str]] = Field(None, alias="reservoirMapping")
    cut_order_files: Optional[list[str]] = Field(None, alias="cutOrderFiles")
    extra_files: Optional[list[str]] = Field(None, alias="extraFiles")
    cog_shop_files_config: Optional[list[dict]] = Field(None, alias="cogShopFilesConfig")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")

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

    @field_validator("scenario", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Case:
        """Convert this GraphQL format of case to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Case(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            scenario=self.scenario.as_read() if isinstance(self.scenario, GraphQLCore) else self.scenario,
            case_file=self.case_file,
            reservoir_mapping=self.reservoir_mapping,
            cut_order_files=self.cut_order_files,
            extra_files=self.extra_files,
            cog_shop_files_config=self.cog_shop_files_config,
            start_time=self.start_time,
            end_time=self.end_time,
        )

    def as_write(self) -> CaseWrite:
        """Convert this GraphQL format of case to the writing format."""
        return CaseWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            scenario=self.scenario.as_write() if isinstance(self.scenario, DomainModel) else self.scenario,
            case_file=self.case_file,
            reservoir_mapping=self.reservoir_mapping,
            cut_order_files=self.cut_order_files,
            extra_files=self.extra_files,
            cog_shop_files_config=self.cog_shop_files_config,
            start_time=self.start_time,
            end_time=self.end_time,
        )


class Case(DomainModel):
    """This represents the reading version of case.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the case.
        data_record: The data record of the case node.
        scenario: The Shop scenario that was used to produce this result
        case_file: The case file used
        reservoir_mapping: The cut file reservoir mapping
        cut_order_files: Cut order files (Module series in PRODRISK)
        extra_files: The extra file field.
        cog_shop_files_config: Configuration for in what order to load the various files into pyshop
        start_time: The start time of the case
        end_time: The end time of the case
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types_temp", "Case")
    scenario: Union[Scenario, str, dm.NodeId, None] = Field(None, repr=False)
    case_file: Union[str, None] = Field(None, alias="caseFile")
    reservoir_mapping: Optional[list[str]] = Field(None, alias="reservoirMapping")
    cut_order_files: Optional[list[str]] = Field(None, alias="cutOrderFiles")
    extra_files: Optional[list[str]] = Field(None, alias="extraFiles")
    cog_shop_files_config: Optional[list[dict]] = Field(None, alias="cogShopFilesConfig")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")

    def as_write(self) -> CaseWrite:
        """Convert this read version of case to the writing version."""
        return CaseWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            scenario=self.scenario.as_write() if isinstance(self.scenario, DomainModel) else self.scenario,
            case_file=self.case_file,
            reservoir_mapping=self.reservoir_mapping,
            cut_order_files=self.cut_order_files,
            extra_files=self.extra_files,
            cog_shop_files_config=self.cog_shop_files_config,
            start_time=self.start_time,
            end_time=self.end_time,
        )

    def as_apply(self) -> CaseWrite:
        """Convert this read version of case to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CaseWrite(DomainModelWrite):
    """This represents the writing version of case.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the case.
        data_record: The data record of the case node.
        scenario: The Shop scenario that was used to produce this result
        case_file: The case file used
        reservoir_mapping: The cut file reservoir mapping
        cut_order_files: Cut order files (Module series in PRODRISK)
        extra_files: The extra file field.
        cog_shop_files_config: Configuration for in what order to load the various files into pyshop
        start_time: The start time of the case
        end_time: The end time of the case
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types_temp", "Case")
    scenario: Union[ScenarioWrite, str, dm.NodeId, None] = Field(None, repr=False)
    case_file: Union[str, None] = Field(None, alias="caseFile")
    reservoir_mapping: Optional[list[str]] = Field(None, alias="reservoirMapping")
    cut_order_files: Optional[list[str]] = Field(None, alias="cutOrderFiles")
    extra_files: Optional[list[str]] = Field(None, alias="extraFiles")
    cog_shop_files_config: Optional[list[dict]] = Field(None, alias="cogShopFilesConfig")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")

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

        write_view = (view_by_read_class or {}).get(Case, dm.ViewId("sp_powerops_models_temp", "Case", "1"))

        properties: dict[str, Any] = {}

        if self.scenario is not None:
            properties["scenario"] = {
                "space": self.space if isinstance(self.scenario, str) else self.scenario.space,
                "externalId": self.scenario if isinstance(self.scenario, str) else self.scenario.external_id,
            }

        if self.case_file is not None or write_none:
            properties["caseFile"] = self.case_file

        if self.reservoir_mapping is not None or write_none:
            properties["reservoirMapping"] = self.reservoir_mapping

        if self.cut_order_files is not None or write_none:
            properties["cutOrderFiles"] = self.cut_order_files

        if self.extra_files is not None or write_none:
            properties["extraFiles"] = self.extra_files

        if self.cog_shop_files_config is not None or write_none:
            properties["cogShopFilesConfig"] = self.cog_shop_files_config

        if self.start_time is not None or write_none:
            properties["startTime"] = self.start_time.isoformat(timespec="milliseconds") if self.start_time else None

        if self.end_time is not None or write_none:
            properties["endTime"] = self.end_time.isoformat(timespec="milliseconds") if self.end_time else None

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

        if isinstance(self.scenario, DomainModelWrite):
            other_resources = self.scenario._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class CaseApply(CaseWrite):
    def __new__(cls, *args, **kwargs) -> CaseApply:
        warnings.warn(
            "CaseApply is deprecated and will be removed in v1.0. Use CaseWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Case.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CaseList(DomainModelList[Case]):
    """List of cases in the read version."""

    _INSTANCE = Case

    def as_write(self) -> CaseWriteList:
        """Convert these read versions of case to the writing versions."""
        return CaseWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CaseWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CaseWriteList(DomainModelWriteList[CaseWrite]):
    """List of cases in the writing version."""

    _INSTANCE = CaseWrite


class CaseApplyList(CaseWriteList): ...


def _create_case_filter(
    view_id: dm.ViewId,
    scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if scenario and isinstance(scenario, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("scenario"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": scenario}
            )
        )
    if scenario and isinstance(scenario, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("scenario"), value={"space": scenario[0], "externalId": scenario[1]}
            )
        )
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("scenario"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in scenario],
            )
        )
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("scenario"),
                values=[{"space": item[0], "externalId": item[1]} for item in scenario],
            )
        )
    if min_start_time is not None or max_start_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startTime"),
                gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None,
                lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None,
            )
        )
    if min_end_time is not None or max_end_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("endTime"),
                gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None,
                lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None,
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
