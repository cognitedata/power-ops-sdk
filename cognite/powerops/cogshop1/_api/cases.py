from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.cogshop1._api._core import TypeAPI
from cognite.powerops.cogshop1.data_classes import Case, CaseApply, CaseList


class CaseProcessingLogsAPI:
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

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "Case.processingLog"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class CasesAPI(TypeAPI[Case, CaseApply, CaseList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cogShop", "Case", "ed83a1fc90224d"),
            class_type=Case,
            class_apply_type=CaseApply,
            class_list=CaseList,
        )
        self.processing_logs = CaseProcessingLogsAPI(client)

    def apply(self, case: CaseApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = case.to_instances_apply()
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

            processing_log_edges = self.processing_logs.retrieve(external_id)
            case.processing_log = [edge.end_node.external_id for edge in processing_log_edges]

            return case
        else:
            cases = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            processing_log_edges = self.processing_logs.retrieve(external_id)
            self._set_processing_log(cases, processing_log_edges)

            return cases

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> CaseList:
        cases = self._list(limit=limit)

        processing_log_edges = self.processing_logs.list(limit=-1)
        self._set_processing_log(cases, processing_log_edges)

        return cases

    @staticmethod
    def _set_processing_log(cases: Sequence[Case], processing_log_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in processing_log_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for case in cases:
            node_id = case.id_tuple()
            if node_id in edges_by_start_node:
                case.processing_log = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
