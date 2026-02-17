from __future__ import annotations

import importlib
from pathlib import Path
from unittest.mock import patch

import pytest

# Import the module object directly to avoid name collision with the `purge` function
# exported from cognite.powerops.resync.__init__
purge_module = importlib.import_module("cognite.powerops.resync.purge")
ResyncPurge = purge_module.ResyncPurge


def _rglob_factory(yaml_files: list[Path], yml_files: list[Path] | None = None):
    """Helper that returns an rglob side_effect returning yaml_files for *.yaml and yml_files for *.yml."""
    if yml_files is None:
        yml_files = []

    def _rglob(pattern):
        if pattern == "*.yaml":
            return yaml_files
        return yml_files

    return _rglob


@pytest.fixture
def purge_instance(cognite_client_mock):
    """Create a ResyncPurge instance with default test configuration."""
    return ResyncPurge(
        toolkit_directory=[Path("/fake/toolkit")],
        dry_run=False,
        verbose=False,
        client=cognite_client_mock,
        models_space="test_space",
        type_space="test_type_space",
        data_model_version="1",
        exclude_edges=[],
        exclude_nodes=[],
    )


# -- Tests for get_toolkit_external_ids --


class TestGetToolkitExternalIds:
    """Tests for the get_toolkit_external_ids method, focusing on instance type resolution."""

    @patch.object(purge_module, "load_yaml")
    def test_node_from_instance_type_field(self, mock_load_yaml, purge_instance):
        """When instanceType is present in the YAML, it should be used regardless of filename."""
        mock_load_yaml.return_value = [
            {
                "instanceType": "node",
                "space": "sp",
                "externalId": "node_1",
                "type": {"space": "sp", "externalId": "MyNodeType"},
            }
        ]

        with patch.object(Path, "rglob", side_effect=_rglob_factory([Path("/fake/toolkit/data.yaml")])):
            nodes, edges = purge_instance.get_toolkit_external_ids()

        assert nodes == {"MyNodeType": [("sp", "node_1")]}
        assert edges == {}

    @patch.object(purge_module, "load_yaml")
    def test_edge_from_instance_type_field(self, mock_load_yaml, purge_instance):
        """When instanceType is present in the YAML, it should be used regardless of filename."""
        mock_load_yaml.return_value = [
            {
                "instanceType": "edge",
                "space": "sp",
                "externalId": "edge_1",
                "type": {"space": "sp", "externalId": "MyEdgeType"},
            }
        ]

        with patch.object(Path, "rglob", side_effect=_rglob_factory([Path("/fake/toolkit/data.yaml")])):
            nodes, edges = purge_instance.get_toolkit_external_ids()

        assert nodes == {}
        assert edges == {"MyEdgeType": [("sp", "edge_1")]}

    @patch.object(purge_module, "load_yaml")
    def test_node_inferred_from_filename(self, mock_load_yaml, purge_instance):
        """When instanceType is missing, instance type should be inferred from .node in filename."""
        mock_load_yaml.return_value = [
            {
                "space": "sp",
                "externalId": "node_1",
                "type": {"space": "sp", "externalId": "Generator"},
            }
        ]

        with patch.object(Path, "rglob", side_effect=_rglob_factory([Path("/fake/toolkit/Generator.node.yaml")])):
            nodes, edges = purge_instance.get_toolkit_external_ids()

        assert nodes == {"Generator": [("sp", "node_1")]}
        assert edges == {}

    @patch.object(purge_module, "load_yaml")
    def test_edge_inferred_from_filename(self, mock_load_yaml, purge_instance):
        """When instanceType is missing, instance type should be inferred from .edge in filename."""
        mock_load_yaml.return_value = [
            {
                "space": "sp",
                "externalId": "edge_1",
                "type": {"space": "sp", "externalId": "BelongsTo"},
            }
        ]

        with patch.object(Path, "rglob", side_effect=_rglob_factory([Path("/fake/toolkit/BelongsTo.edge.yaml")])):
            nodes, edges = purge_instance.get_toolkit_external_ids()

        assert nodes == {}
        assert edges == {"BelongsTo": [("sp", "edge_1")]}

    @patch.object(purge_module, "load_yaml")
    def test_instance_type_field_takes_precedence_over_filename(self, mock_load_yaml, purge_instance):
        """instanceType in YAML should take precedence over file-name inference."""
        mock_load_yaml.return_value = [
            {
                "instanceType": "node",
                "space": "sp",
                "externalId": "node_1",
                "type": {"space": "sp", "externalId": "SomeType"},
            }
        ]

        # Filename says edge, but instanceType says node — instanceType should win
        with patch.object(Path, "rglob", side_effect=_rglob_factory([Path("/fake/toolkit/SomeType.edge.yaml")])):
            nodes, edges = purge_instance.get_toolkit_external_ids()

        assert nodes == {"SomeType": [("sp", "node_1")]}
        assert edges == {}

    @patch.object(purge_module, "load_yaml")
    def test_instance_without_type_or_filename_hint_is_skipped(self, mock_load_yaml, purge_instance):
        """Instances with no instanceType and no filename hint should be skipped."""
        mock_load_yaml.return_value = [
            {
                "space": "sp",
                "externalId": "orphan_1",
                "type": {"space": "sp", "externalId": "Orphan"},
            }
        ]

        with patch.object(Path, "rglob", side_effect=_rglob_factory([Path("/fake/toolkit/data.yaml")])):
            nodes, edges = purge_instance.get_toolkit_external_ids()

        assert nodes == {}
        assert edges == {}

    @patch.object(purge_module, "load_yaml")
    def test_node_without_type_field_is_skipped(self, mock_load_yaml, purge_instance):
        """A node instance missing the 'type' key should not be added to results."""
        mock_load_yaml.return_value = [
            {
                "instanceType": "node",
                "space": "sp",
                "externalId": "node_no_type",
            }
        ]

        with patch.object(Path, "rglob", side_effect=_rglob_factory([Path("/fake/toolkit/data.yaml")])):
            nodes, edges = purge_instance.get_toolkit_external_ids()

        assert nodes == {}
        assert edges == {}

    @patch.object(purge_module, "load_yaml")
    def test_edge_without_type_field_is_skipped(self, mock_load_yaml, purge_instance):
        """An edge instance missing the 'type' key should not be added to results."""
        mock_load_yaml.return_value = [
            {
                "instanceType": "edge",
                "space": "sp",
                "externalId": "edge_no_type",
            }
        ]

        with patch.object(Path, "rglob", side_effect=_rglob_factory([Path("/fake/toolkit/data.yaml")])):
            nodes, edges = purge_instance.get_toolkit_external_ids()

        assert nodes == {}
        assert edges == {}

    @patch.object(purge_module, "load_yaml")
    def test_multiple_instances_grouped_by_type(self, mock_load_yaml, purge_instance):
        """Multiple instances of the same type should be grouped together."""
        mock_load_yaml.return_value = [
            {
                "instanceType": "node",
                "space": "sp",
                "externalId": "node_1",
                "type": {"space": "sp", "externalId": "Generator"},
            },
            {
                "instanceType": "node",
                "space": "sp",
                "externalId": "node_2",
                "type": {"space": "sp", "externalId": "Generator"},
            },
            {
                "instanceType": "edge",
                "space": "sp",
                "externalId": "edge_1",
                "type": {"space": "sp", "externalId": "BelongsTo"},
            },
        ]

        with patch.object(Path, "rglob", side_effect=_rglob_factory([Path("/fake/toolkit/data.yaml")])):
            nodes, edges = purge_instance.get_toolkit_external_ids()

        assert nodes == {"Generator": [("sp", "node_1"), ("sp", "node_2")]}
        assert edges == {"BelongsTo": [("sp", "edge_1")]}

    @patch.object(purge_module, "load_yaml")
    def test_multiple_directories(self, mock_load_yaml, purge_instance):
        """Instances from multiple toolkit directories should be merged."""
        purge_instance.toolkit_directory = [Path("/fake/toolkit_a"), Path("/fake/toolkit_b")]

        call_count = 0

        def yaml_side_effect(file, expected_return_type="list"):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [
                    {
                        "instanceType": "node",
                        "space": "sp",
                        "externalId": "node_a",
                        "type": {"space": "sp", "externalId": "Plant"},
                    }
                ]
            return [
                {
                    "instanceType": "node",
                    "space": "sp",
                    "externalId": "node_b",
                    "type": {"space": "sp", "externalId": "Plant"},
                }
            ]

        mock_load_yaml.side_effect = yaml_side_effect

        with patch.object(Path, "rglob", side_effect=_rglob_factory([Path("/fake/file.yaml")])):
            nodes, edges = purge_instance.get_toolkit_external_ids()

        assert nodes == {"Plant": [("sp", "node_a"), ("sp", "node_b")]}

    @patch.object(purge_module, "load_yaml")
    def test_yml_extension_is_also_loaded(self, mock_load_yaml, purge_instance):
        """Both .yaml and .yml files should be discovered and loaded."""
        mock_load_yaml.return_value = [
            {
                "instanceType": "node",
                "space": "sp",
                "externalId": "n1",
                "type": {"space": "sp", "externalId": "T"},
            }
        ]

        with patch.object(
            Path,
            "rglob",
            side_effect=_rglob_factory([Path("/fake/toolkit/a.yaml")], [Path("/fake/toolkit/b.yml")]),
        ):
            nodes, _ = purge_instance.get_toolkit_external_ids()

        assert mock_load_yaml.call_count == 2
        assert nodes == {"T": [("sp", "n1"), ("sp", "n1")]}
