from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from cognite.pygen.utils import MockGenerator
from cognite.client import data_modeling as dm
from cognite.client.data_classes import filters
import time

from cognite.powerops import PowerOpsClient

INSTANCE_SPACE = "power_ops_instances"
MODEL_SPACE = "power_ops_core"
TYPE_SPACE = "power_ops_types"

SPACES = [MODEL_SPACE, INSTANCE_SPACE, TYPE_SPACE]
# TODO: consider adding a separate space for mock data


def main(client: CogniteClient):
    print(f"Connected to {client.config.project}")

    data_model_ids = [dm.DataModelId(MODEL_SPACE, "all_PowerOps", "1")]
    node_count = 5
    for data_model_id in data_model_ids:
        data_models = client.data_modeling.data_models.retrieve(data_model_id, inline_views=True)
        if not data_models:
            print(f"Data model {data_model_id} not found.")
            continue
        data_model = data_models.latest_version()
        leaf_views_by_parent = MockGenerator._to_leaf_children_by_parent(data_model.views)

        mock_generator = MockGenerator(data_model.views, instance_space=INSTANCE_SPACE, seed=42, skip_interfaces=False)
        mock_data = mock_generator.generate_mock_data(node_count=node_count)
        print(f"Generated {len(mock_data.nodes)} nodes for {len(data_model.views)} views.")

        # Custom fix of the Scenario/BidMatrix data as it filters on a property which the PygenMockGenerator does not set.
        for view_data in mock_data:
            if view_data.view_id in {
                dm.ViewId(MODEL_SPACE, "BidMatrixRaw", "1"),
                dm.ViewId(MODEL_SPACE, "MultiScenarioMatrix", "1"),
                dm.ViewId(MODEL_SPACE, "MultiScenarioMatrixRaw", "1"),
                dm.ViewId(MODEL_SPACE, "CustomBidMatrix", "1"),
                dm.ViewId(MODEL_SPACE, "BasicBidMatrixRaw", "1"),
                dm.ViewId(MODEL_SPACE, "BasicBidMatrix", "1"),
            }:
                is_processed = False if "Raw" in view_data.view_id.external_id else True
                for node in view_data.node:
                    node.sources[0].properties["isProcessed"] = is_processed

        # Write to all views
        mock_data.deploy(client)
        print(f"Deployed {len(mock_data.nodes)} nodes to {len(data_model.views)} views.")

        # Check Read.
        correct_count = 0
        for view in data_model.views:
            view_id = view.as_id()
            try:
                nodes = client.data_modeling.instances.list("node", sources=[view_id], limit=-1)
            except CogniteAPIError as e:
                print(f"Failed to read nodes for {view_id}: {e}")
                continue

            # PriceArea have three different versions, that all picks up the same nodes.
            if view_id in leaf_views_by_parent:
                leaf_views = leaf_views_by_parent[view_id]
                expected_node_count = len(leaf_views) * node_count
                expected_nodes = [
                    n
                    for n in mock_data.nodes
                    if any(n.external_id.startswith(f"{v.external_id.lower()}_") for v in leaf_views)
                ]
            elif view_id.external_id.startswith("PriceArea"):
                expected_nodes = [n for n in mock_data.nodes if n.external_id.startswith("pricearea")]
                expected_node_count = len(expected_nodes)
            else:
                expected_nodes = [n for n in mock_data.nodes if n.external_id.startswith(view_id.external_id.lower())]
                expected_node_count = node_count

            if view_id in {
                dm.ViewId(MODEL_SPACE, "FunctionInput", "1"),
                dm.ViewId(MODEL_SPACE, "FunctionOutput", "1"),
            }:
                # This is an exception, the input and output stores data in the same container and has identical filtering.
                # So Input will retrieve input + output and output will retrieve input + output.
                # These interfaces that are not used anywhere in the data model, thus this is considered not to be a problem.
                expected_node_count *= 2

            if len(nodes) != expected_node_count:
                print(f"Print unexpected number of nodes for {view_id}: {len(nodes)} instead of {expected_node_count}.")
                print(f"Expected {expected_node_count} nodes: {expected_nodes}")
            else:
                correct_count += 1
                # print(f"Read {len(nodes)} nodes for {view_id} as expected.")

        print(f"Read {correct_count} views out of {len(data_model.views)} views as expected.")
        # mock_data.clean(client)


def clean_instances(client: CogniteClient, dry_run: bool):
    t0 = time.perf_counter()

    print(f"Connected to {client.config.project}")

    for space in SPACES:

        for edges in client.data_modeling.instances(instance_type="edge", space=space, limit=-1, chunk_size=100):
            if not dry_run:
                deleted = client.data_modeling.instances.delete(edges.as_ids())
                print(f"Deleted {len(deleted.edges)} edges. Elapsed time {time.perf_counter() - t0:.2f} seconds")
        else:
            print(f"Done deleting edges in space {space}")

        for nodes in client.data_modeling.instances(instance_type="node", space=space, limit=-1, chunk_size=500):
            if not dry_run:
                deleted = client.data_modeling.instances.delete(nodes.as_ids())
                print(f"Deleted {len(deleted.nodes)} nodes. Elapsed time {time.perf_counter() - t0:.2f} seconds")
        else:
            print(f"Done deleting nodes in space {space}")

        is_mock_generator = filters.Equals(["metadata", "source"], "PygenMockGenerator")

    timeseries = client.time_series.filter(limit=-1, filter=is_mock_generator)
    if timeseries:
        if not dry_run:
            client.time_series.delete(id=[ts.id for ts in timeseries])
        print(f"Deleted {len(timeseries)} timeseries. Elapsed time {time.perf_counter() - t0:.2f} seconds")

    sequences = client.sequences.filter(limit=-1, filter=is_mock_generator)
    if sequences:
        if not dry_run:
            client.sequences.delete(id=[seq.id for seq in sequences])
        print(f"Deleted {len(sequences)} sequences. Elapsed time {time.perf_counter() - t0:.2f} seconds")

    files = client.files.list(limit=-1, metadata={"source": "PygenMockGenerator"})
    if files:
        if not dry_run:
            client.files.delete(id=[file.id for file in files])
        print(f"Deleted {len(files)} files. Elapsed time {time.perf_counter() - t0:.2f} seconds")


def clean_containers_views_data_models(client: CogniteClient, dry_run: bool):
    t0 = time.perf_counter()

    print(f"Connected to {client.config.project}")

    for space in SPACES:
        views = client.data_modeling.views.list(space=space, limit=-1, all_versions=True).as_ids()

        if views:
            print(f"Deleting {len(views)} views in {space}")
            if not dry_run:
                client.data_modeling.views.delete(views)
        else:
            print(f"No views found in {space}")

        data_models = client.data_modeling.data_models.list(space=space, limit=-1, all_versions=True).as_ids()
        if data_models:
            print(f"Deleting {len(data_models)} data models in {space}")
            if not dry_run:
                client.data_modeling.data_models.delete(data_models)
        else:
            print(f"No data models found in {space}")

        containers = client.data_modeling.containers.list(space=space, limit=-1).as_ids()
        if containers:
            print(f"Deleting {len(containers)} containers in {space}")
            if not dry_run:
                client.data_modeling.containers.delete(containers)
        else:
            print(f"No containers found in {space}")


def clean_spaces(client: CogniteClient, dry_run: bool):
    t0 = time.perf_counter()

    print(f"Connected to {client.config.project}")

    existing_spaces = client.data_modeling.spaces.retrieve(spaces=SPACES)
    existing_space_names = [s.space for s in existing_spaces]
    for space in SPACES:
        if space in existing_space_names:
            print(f"Deleting space {space}")
            if not dry_run:
                client.data_modeling.spaces.delete(space)
        else:
            print(f"Space {space} not found")


if __name__ == "__main__":
    dry_run = True

    client = PowerOpsClient.from_config("power_ops_config.yaml").cdf # TODO: remove from yaml configuration datasets

    clean_instances(client, dry_run)
    clean_containers_views_data_models(client, dry_run)
    clean_spaces(client, dry_run)
    # main(client)
