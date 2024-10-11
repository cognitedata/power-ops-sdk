from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class BenchmarkingShopCaseShopFilesAPI(EdgeAPI):
    def list(
            self,
            from_benchmarking_shop_case: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            from_benchmarking_shop_case_space: str = DEFAULT_INSTANCE_SPACE,
            to_shop_file: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
            to_shop_file_space: str = DEFAULT_INSTANCE_SPACE,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit=DEFAULT_LIMIT_READ,
    ) ->dm.EdgeList:
        """List shop file edges of a benchmarking shop case.

        Args:
            from_benchmarking_shop_case: ID of the source benchmarking shop case.
            from_benchmarking_shop_case_space: Location of the benchmarking shop cases.
            to_shop_file: ID of the target shop file.
            to_shop_file_space: Location of the shop files.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop file edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested shop file edges.

        Examples:

            List 5 shop file edges connected to "my_benchmarking_shop_case":

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_shop_case = client.benchmarking_shop_case.shop_files_edge.list("my_benchmarking_shop_case", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopCase.shopFiles"),

            from_benchmarking_shop_case,
            from_benchmarking_shop_case_space,
            to_shop_file,
            to_shop_file_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
