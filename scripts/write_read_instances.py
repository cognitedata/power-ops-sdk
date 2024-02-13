from cognite.pygen.utils import MockGenerator
from cognite.client import data_modeling as dm
import os
from pathlib import Path
from cognite.powerops.utils.cdf import get_cognite_client


from cognite.powerops.utils.serialization import chdir

REPO_ROOT = Path(__file__).parent.parent


def main():
    with chdir(REPO_ROOT):
        os.environ["SETTINGS_FILES"] = "settings.toml;.secrets.toml"
        client = get_cognite_client()

    print(f"Connected to {client.config.project}")

    data_model_ids = [dm.DataModelId("sp_powerops_models", "all_PowerOps", "1")]
    instance_space = "sp_powerops_instance"
    node_count = 5
    for data_model_id in data_model_ids:
        data_models = client.data_modeling.data_models.retrieve(data_model_id, inline_views=True)
        if not data_models:
            print(f"Data model {data_model_id} not found.")
            continue
        data_model = data_models.latest_version()
        leaf_views_by_parent = MockGenerator._to_leaf_children_by_parent(data_model.views)

        mock_generator = MockGenerator(data_model.views, instance_space=instance_space, seed=42, skip_interfaces=True)
        mock_data = mock_generator.generate_mock_data(node_count=node_count)
        print(f"Generated {len(mock_data.nodes)} nodes for {len(data_model.views)} views.")

        # Write to all views
        for view_data in mock_data:
            try:
                view_data.deploy(client, verbose=True)
            except Exception as e:
                print(f"Failed to deploy {len(view_data.node)} nodes to {view_data.view_id}: {e}")
                continue
        print(f"Deployed {len(mock_data.nodes)} nodes to {len(data_model.views)} views.")

        # Check Read.
        for view in data_model.views:
            view_id = view.as_id()
            nodes = client.data_modeling.instances.list("node", sources=[view_id], limit=-1)
            if view_id in leaf_views_by_parent:
                expected_nodes = len(leaf_views_by_parent[view_id]) * node_count
            else:
                expected_nodes = node_count
            if len(nodes) != expected_nodes:
                print(f"Print unexpected number of nodes for {view_id}: {len(nodes)} instead of {expected_nodes}.")
            else:
                print(f"Read {len(nodes)} nodes for {view_id} as expected.")

        mock_data.clean(client)


if __name__ == "__main__":
    main()
