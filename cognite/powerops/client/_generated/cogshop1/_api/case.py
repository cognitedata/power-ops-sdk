from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.cogshop1.data_classes import Case, CaseApply, CaseApplyList, CaseList

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class CaseProcessingLogAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Case.processingLog"},
        )
        if isinstance(external_id, str):
            is_case = f.Equals(
                ["edge", "startNode"],
                {"space": "cogShop", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_case))

        else:
            is_cases = f.In(
                ["edge", "startNode"],
                [{"space": "cogShop", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_cases))

    def list(self, case_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Case.processingLog"},
        )
        filters.append(is_edge_type)
        if case_id:
            case_ids = [case_id] if isinstance(case_id, str) else case_id
            is_cases = f.In(
                ["edge", "startNode"],
                [{"space": "cogShop", "externalId": ext_id} for ext_id in case_ids],
            )
            filters.append(is_cases)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class CaseAPI(TypeAPI[Case, CaseApply, CaseList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Case,
            class_apply_type=CaseApply,
            class_list=CaseList,
        )
        self.view_id = view_id
        self.processing_log = CaseProcessingLogAPI(client)

    def apply(self, case: CaseApply | Sequence[CaseApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(case, CaseApply):
            instances = case.to_instances_apply()
        else:
            instances = CaseApplyList(case).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CaseApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CaseApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Case:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CaseList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Case | CaseList:
        if isinstance(external_id, str):
            case = self._retrieve((self.sources.space, external_id))

            processing_log_edges = self.processing_log.retrieve(external_id)
            case.processing_log = [edge.end_node.external_id for edge in processing_log_edges]

            return case
        else:
            cases = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            processing_log_edges = self.processing_log.retrieve(external_id)
            self._set_processing_log(cases, processing_log_edges)

            return cases

    def list(
        self,
        start_time: str | list[str] | None = None,
        start_time_prefix: str | None = None,
        end_time: str | list[str] | None = None,
        end_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> CaseList:
        filter_ = _create_filter(
            self.view_id,
            start_time,
            start_time_prefix,
            end_time,
            end_time_prefix,
            external_id_prefix,
            filter,
        )

        cases = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            processing_log_edges = self.processing_log.list(cases.as_external_ids(), limit=-1)
            self._set_processing_log(cases, processing_log_edges)

        return cases

    @staticmethod
    def _set_processing_log(cases: Sequence[Case], processing_log_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in processing_log_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for case in cases:
            node_id = case.id_tuple()
            if node_id in edges_by_start_node:
                case.processing_log = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    start_time: str | list[str] | None = None,
    start_time_prefix: str | None = None,
    end_time: str | list[str] | None = None,
    end_time_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if start_time and isinstance(start_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("startTime"), value=start_time))
    if start_time and isinstance(start_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("startTime"), values=start_time))
    if start_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("startTime"), value=start_time_prefix))
    if end_time and isinstance(end_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("endTime"), value=end_time))
    if end_time and isinstance(end_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("endTime"), values=end_time))
    if end_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("endTime"), value=end_time_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
