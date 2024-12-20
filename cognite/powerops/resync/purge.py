from __future__ import annotations

import logging
from pathlib import Path

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import EdgeId, NodeId, ViewId
from cognite.client.data_classes.filters import In

from cognite.powerops.utils.serialization import load_yaml

logger = logging.getLogger("resync")


class ResyncPurge:
    """Class for purging resync data from CDF."""

    toolkit_modules: list[Path]
    models_space: str
    data_model_version: str
    client: CogniteClient
    exclude_edges: list[str]
    exclude_nodes: list[str]
    dry_run: bool = False
    verbose: bool = False

    def __init__(
        self,
        toolkit_directory: list[Path],
        dry_run: bool,
        verbose: bool,
        client: CogniteClient,
        models_space: str,
        type_space: str,
        data_model_version: str,
        exclude_edges: list[str],
        exclude_nodes: list[str],
    ):
        self.toolkit_directory = toolkit_directory
        self.dry_run = dry_run
        self.client = client
        self.data_model_version = data_model_version
        self.models_space = models_space
        self.type_space = type_space
        self.verbose = verbose
        self.exclude_edges = exclude_edges
        self.exclude_nodes = exclude_nodes

    @classmethod
    def from_yaml(
        cls,
        configuration_path: Path,
        cdf_client: CogniteClient,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> ResyncPurge:
        """Creates a ResyncImporter object from a resync configuration file.

        Args:
            configuration_path: Path to the resync configuration file.
            data_model_classes: A dictionary of all data model classes to be used for the resync configuration.

        Returns:
            A ResyncImporter object.
        """
        configuration = load_yaml(configuration_path, expected_return_type="dict")

        temp_toolkit_directory = configuration.get("toolkit_modules")
        if temp_toolkit_directory:
            if isinstance(temp_toolkit_directory, str):
                toolkit_directory = [Path(temp_toolkit_directory)]
            elif isinstance(temp_toolkit_directory, list):
                toolkit_directory = [Path(directory) for directory in temp_toolkit_directory]
            else:
                raise ValueError("toolkit_directory must be a string or a list of strings")
        else:
            raise ValueError("toolkit_directory is required in the configuration file")

        return cls(
            toolkit_directory=toolkit_directory,
            dry_run=dry_run,
            verbose=verbose,
            client=cdf_client,
            data_model_version=configuration["data_model_version"],
            models_space=configuration["models_space"],
            type_space=configuration["type_space"],
            exclude_nodes=configuration.get("exclude_nodes", []),
            exclude_edges=configuration.get("exclude_edges", []),
        )

    def purge(self) -> None:
        """Purges resync data from CDF."""

        node_ids, edge_ids = self.get_toolkit_external_ids()
        views_with_nodes_to_delete = self.get_nodes_to_delete(node_ids)

        logger.info("NODES to be deleted:")
        all_nodes_to_delete: list[NodeId] = []
        for view, nodes in views_with_nodes_to_delete.items():
            logger.info(f"- {view}: {len(nodes)}")
            if self.verbose:
                logger.info(f"   * External ids: {nodes}")
            all_nodes_to_delete.extend(nodes)

        logger.info("EDGES to be deleted:")
        all_edges_to_delete = []
        edges_to_delete = self.get_edges_to_delete(edge_ids)
        for edge_type, edges in edges_to_delete.items():
            logger.info(f"- {edge_type}: {len(edges)}")
            if self.verbose:
                logger.info(f"   * External ids of edge type {edge_type}: {edges}")
            all_edges_to_delete.extend(edges)

        logger.info("Purge Summary:")
        logger.info(f"- Found {len(all_nodes_to_delete)} nodes in total to delete.")
        logger.info(f"- Found {len(all_edges_to_delete)} edges in total to delete.")

        if self.dry_run:
            logger.info("Dry run mode enabled. Exiting without deleting any data.")
            return

        self.client.data_modeling.instances.delete(nodes=all_nodes_to_delete, edges=all_edges_to_delete)
        logger.info(f"Deleted {len(all_nodes_to_delete)} nodes and {len(all_edges_to_delete)} edges.")

    def get_toolkit_external_ids(self) -> tuple[dict[str, list[tuple[str, str]]], dict[str, list[tuple[str, str]]]]:
        """Get all node and external IDs from the toolkit files."""

        node_external_ids: dict[str, list[tuple[str, str]]] = {}
        edge_external_ids: dict[str, list[tuple[str, str]]] = {}
        for toolkit_directory in self.toolkit_directory:
            yaml_files = list(toolkit_directory.rglob("*.yaml")) + list(toolkit_directory.rglob("*.yml"))
            for file in yaml_files:
                instances = load_yaml(file, expected_return_type="list")
                for instance in instances:
                    if instance_type := instance.get("instanceType"):
                        external_id: tuple[str, str] = (instance["space"], instance["externalId"])
                        if instance_type == "node":
                            node_type = instance.get("type")
                            if node_type:
                                node_type_xid = node_type["externalId"]
                                if node_type_xid in node_external_ids:
                                    temp_list = node_external_ids[node_type_xid]
                                    temp_list.append(external_id)
                                    node_external_ids[node_type_xid] = temp_list
                                else:
                                    node_external_ids[node_type_xid] = [external_id]
                        elif instance_type == "edge":
                            edge_type = instance.get("type")
                            if edge_type:
                                edge_type_xid = edge_type["externalId"]
                                if edge_type_xid in edge_external_ids:
                                    temp_list = edge_external_ids[edge_type_xid]
                                    temp_list.append(external_id)
                                    edge_external_ids[edge_type_xid] = temp_list
                                else:
                                    edge_external_ids[edge_type_xid] = [external_id]

        return node_external_ids, edge_external_ids

    def get_nodes_to_delete(self, node_ids: dict[str, list[tuple[str, str]]]) -> dict[str, list[NodeId]]:
        """Gets all the nodes that exist that are not in the toolkit files."""

        views_with_nodes_to_delete: dict[str, list[NodeId]] = {}
        for view_xid in node_ids.keys():
            if view_xid not in self.exclude_nodes:
                view_id = ViewId(self.models_space, view_xid, self.data_model_version)
                existing_nodes = self.client.data_modeling.instances.list(
                    instance_type="node", limit=None, sources=view_id
                )
                toolkit_nodes = node_ids.get(view_xid, [])
                for node in existing_nodes:
                    temp_node = (node.space, node.external_id)
                    if temp_node not in toolkit_nodes:
                        if view_xid in views_with_nodes_to_delete:
                            temp_list = views_with_nodes_to_delete[view_xid]
                            temp_list.append(node.as_id())
                            views_with_nodes_to_delete[view_xid] = temp_list
                        else:
                            views_with_nodes_to_delete[view_xid] = [node.as_id()]

        return views_with_nodes_to_delete

    def get_edges_to_delete(self, edge_ids: dict[str, list[tuple[str, str]]]) -> dict[str, list[EdgeId]]:
        """Gets all the edges that exist that are not in the toolkit files."""

        for edge_type in self.exclude_edges:
            edge_ids.pop(edge_type, None)

        spaces = set()
        for edges in edge_ids.values():
            for tk_edge in edges:
                spaces.add(tk_edge[0])

        types = [{"space": self.type_space, "externalId": xid} for xid in edge_ids.keys()]

        type_filter = In(("edge", "type"), types)
        existing_edges = self.client.data_modeling.instances.list(  # type: ignore[call-overload]
            instance_type="edge", limit=None, space=spaces, include_typing=True, filter=type_filter
        )

        types_with_nodes_to_delete: dict[str, list[EdgeId]] = {}
        for edge in existing_edges:
            type_xid = edge.type.external_id
            xids_for_type = edge_ids[type_xid]
            if (edge.space, edge.external_id) not in xids_for_type:
                if type_xid in types_with_nodes_to_delete.keys():
                    temp_list = types_with_nodes_to_delete[type_xid]
                    temp_list.append(edge.as_id())
                    types_with_nodes_to_delete[type_xid] = temp_list
                else:
                    types_with_nodes_to_delete[type_xid] = [edge.as_id()]

        return types_with_nodes_to_delete
