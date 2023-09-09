"""
This module contains the main functions for the resync tool.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from cognite.client.data_classes.data_modeling import SpaceApply, SpaceList
from cognite.client.data_classes.data_modeling.instances import InstanceCore
from yaml import safe_dump

from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.resync import models
from cognite.powerops.resync.config import ReSyncConfig
from cognite.powerops.resync.diff import FieldDifference, ModelDifference, ModelDifferences, model_difference
from cognite.powerops.resync.models.base import DataModel, Model

from . import Echo
from .cdf import get_cognite_api
from .transform import transform
from .validation import _clean_relationships

MODELS_BY_NAME = {m.__name__: m for m in models.V1_MODELS}


def _default_echo(message: str, is_warning: bool = False) -> None:
    print(message)


def init(client: PowerOpsClient | None, echo: Echo | None = None, model_names: str | list[str] | None = None) -> None:
    """
    This function will create the data models in CDF that are required for resync to work. It will not overwrite
    existing models.

    Args:
        client: The PowerOpsClient to use. If not provided, a new client will be created.
        echo: Function to use for printing. Defaults to print.
        model_names: The models to deploy. If not provided, all models will be deployed.

    """
    client = client or PowerOpsClient.from_settings()
    cdf = client.cdf
    echo = echo or _default_echo
    model_classes = _to_models(model_names)
    data_models = [model.graph_ql for model in model_classes if issubclass(model, DataModel)]

    spaces = set(d.id_.space for d in data_models)
    existing_spaces = set((cdf.data_modeling.spaces.retrieve(list(spaces)) or SpaceList([])).as_ids())

    if new_spaces := spaces - existing_spaces:
        echo(f"Creating {len(new_spaces)} new spaces: {new_spaces}")
        cdf.data_modeling.spaces.apply(
            [SpaceApply(space, description="PowerOps Configuration Space", name=space.title()) for space in new_spaces]
        )
        echo(f"Spaces {new_spaces} created")

    existing = set(cdf.data_modeling.data_models.retrieve([d.id_ for d in data_models]).as_ids())
    for model in data_models:
        if model.id_ in existing:
            echo(f"Skipping {model.name} data model with {model.id_}, is already exists", is_warning=True)
            continue
        echo(f"Deploying {model.name} data model with {model.id_}")
        result = cdf.data_modeling.graphql.apply_dml(model.id_, model.graphql, model.name, model.description)
        echo(f"Deployed {model.name} model ({result.space}, {result.external_id}, {result.version})")


def validate(config_dir: str | Path, echo: Echo | None = None) -> None:
    """
    Validates the local configuration files.

    Args:
        config_dir: Local path to the configuration files. Needs to follow a specific structure. See below.
        echo: Function to use for printing. Defaults to print.

    Configuration file structure:
    ```
    📦config_dir
     ┣ 📂cogshop - The CogSHOP configuration
     ┣ 📂market - The Market configuration for DayAhead, RKOM, and benchmarking.
     ┣ 📂production - The physical assets configuration, Watercourse, PriceArea, Genertor, Plant  (SHOP centered)
     ┗ 📜settings.yaml - Settings for resync.
    ```
    """
    echo = echo or _default_echo
    echo(f"Validating configuration in {config_dir}")
    _ = ReSyncConfig.from_yamls(Path(config_dir), "dummy")
    echo("Validation successful")


def plan(
    config_dir: Path,
    market: str,
    client: PowerOpsClient | None,
    echo: Echo | None = None,
    model_names: str | list[str] | None = None,
    dump_folder: Optional[Path] = None,
) -> ModelDifferences:
    """
    Loads the local configuration files, transform them into Resync models, and compares them to the downloaded
    CDF Resync models.

    Args:
        config_dir: Local path to the configuration files. Needs to follow a specific structure. See below.
        market: The market to load the configuration for.
        client: The PowerOpsClient to use. If not provided, a new client will be created.
        echo: Function to use for printing. Defaults to print.
        model_names: The models to run the plan.
        dump_folder: If present, the local and CDF changes will be dumped to this directory. This is done so that
                    you can use local tools (for example, PyCharm or VS Code) to compare the detailed changes
                    between the local and CDF configuration.


    Returns:
        A ModelDifferences object containing the differences between the local and CDF configuration.

    Configuration file structure:
    ```
    📦config_dir
     ┣ 📂cogshop - The CogSHOP configuration
     ┣ 📂market - The Market configuration for DayAhead, RKOM, and benchmarking.
     ┣ 📂production - The physical assets configuration, Watercourse, PriceArea, Genertor, Plant  (SHOP centered)
     ┗ 📜settings.yaml - Settings for resync.
    ```
    """
    echo = echo or _default_echo
    client = client or PowerOpsClient.from_settings()

    loaded_models = _load_transform(market, config_dir, client.cdf.config.project, echo, model_names)

    echo(f"Load transform completed, models {', '.join([type(m).__name__ for m in loaded_models])} loaded")
    if client.datasets.read_dataset is None:
        raise ValueError("No read_dataset configured in settings")
    data_set_external_id = client.datasets.read_dataset
    all_differences = []
    for new_model in loaded_models:
        echo(f"Retrieving {new_model.model_name} from CDF")
        cdf_model = type(new_model).from_cdf(client, data_set_external_id=data_set_external_id)

        differences = model_difference(cdf_model, new_model)
        _clean_relationships(client.cdf, differences, new_model, echo)

        if dump_folder:
            dump_folder.mkdir(parents=True, exist_ok=True)
            # Standardize models for easy comparison
            new_model.standardize()
            cdf_model.standardize()

            (dump_folder / f"{new_model.model_name}_local.yaml").write_text(safe_dump(new_model.dump_as_cdf_resource()))
            (dump_folder / f"{new_model.model_name}_cdf.yaml").write_text(safe_dump(cdf_model.dump_as_cdf_resource()))

        all_differences.append(differences)
    return ModelDifferences(all_differences)


def apply(
    config_dir: Path,
    market: str,
    client: PowerOpsClient | None,
    echo: Echo | None = None,
    model_names: str | list[str] | None = None,
    auto_yes: bool = False,
) -> ModelDifferences:
    """
    Loads the local configuration files, transform them into Resync models, and uploads it to CDF. Any deviations
    of the existing CDF configuration will be overwritten.

    Args:
        config_dir: Local path to the configuration files. Needs to follow a specific structure. See below.
        market: The market to load the configuration for.
        client: The PowerOpsClient to use. If not provided, a new client will be created.
        echo: Function to use for printing. Defaults to print.
        model_names: The models to run the plan.
        auto_yes: If true, all prompts will be auto confirmed.

    Returns:
        A ModelDifferences object containing the differences between the local and CDF configuration which have been
        written to CDF.

    Configuration file structure:
    ```
    📦config_dir
     ┣ 📂cogshop - The CogSHOP configuration
     ┣ 📂market - The Market configuration for DayAhead, RKOM, and benchmarking.
     ┣ 📂production - The physical assets configuration, Watercourse, PriceArea, Genertor, Plant  (SHOP centered)
     ┗ 📜settings.yaml - Settings for resync.
    ```
    """
    echo = echo or _default_echo
    client = client or PowerOpsClient.from_settings()
    loaded_models = _load_transform(market, config_dir, client.cdf.config.project, echo, model_names)

    write_dataset = client.datasets.write_dataset_id

    written_changes = ModelDifferences([])
    for new_model in loaded_models:
        cdf_model = type(new_model).from_cdf(client, data_set_external_id=client.datasets.read_dataset)

        new_sequences_by_id = {s.external_id: s for s in new_model.sequences()}
        new_files_by_id = {f.external_id: f for f in new_model.files()}

        differences = model_difference(cdf_model, new_model)
        _clean_relationships(client.cdf, differences, new_model, echo)
        written_model_changes = ModelDifference(new_model.model_name)
        changed = []
        for diff in differences:
            if diff.group == "Domain":
                continue
            if len(diff.unchanged) == diff.total:
                echo(f"No changes detected for {diff.name} in {new_model.model_name}")
                continue
            elif diff.name == "timeseries":
                echo("Found timeseries changes, skipping. These are not updated by resync")
                continue
            changed.append(diff)

        for diff in sorted(changed, key=_edges_before_nodes):
            if not diff.removed:
                continue
            echo(f"Removals detected for {diff.name} in {new_model.model_name}")
            echo(f"Remove count: {diff.as_summary().removed}")
            diff_ids = diff.as_ids(5)
            echo(f"Sample removed for {diff_ids.name}: {diff_ids.removed}")
            ans = "y" if auto_yes else input("Continue? (y/n)")
            if ans.lower() != "y":
                echo("Aborting")
                continue

            api = get_cognite_api(client.cdf, diff.name, new_sequences_by_id, new_files_by_id)
            api.delete(
                external_id=[
                    r.as_id() if isinstance(r, InstanceCore) else r.external_id for r in diff.removed if r.external_id
                ]
            )
            echo(f"Deleted {len(diff.removed)} of {diff.name}")
            written_model_changes.changes[diff.name] = FieldDifference(
                group=diff.group,
                name=diff.name,
                added=[],
                removed=diff.removed.copy(),
                changed=[],
                unchanged=diff.unchanged,
            )

        for diff in sorted(changed, key=_nodes_before_edges):
            if not diff.added and not diff.changed:
                continue
            echo(f"Changes/Additions detected for {diff.name} in {new_model.model_name}")
            summary_count = diff.as_summary()
            echo(f"Change count: {summary_count.changed}")
            echo(f"Addition count: {summary_count.added}")
            diff_ids = diff.as_ids(5)
            if diff_ids.changed:
                echo(f"Sample changed for {diff_ids.name}:")
                for change in diff.changed[:3]:
                    echo(change.changed_fields)
            if diff_ids.added:
                echo(f"Sample added for {diff_ids.name}: {diff_ids.added}")
            ans = "y" if auto_yes else input("Continue? (y/n)")
            if ans.lower() != "y":
                echo("Aborting")
                continue

            diff.set_set_dataset(write_dataset)
            api = get_cognite_api(client.cdf, diff.name, new_sequences_by_id, new_files_by_id)

            if diff.added:
                api.create(diff.added)
                echo(f"Created {len(diff.added)} of {diff.name}")
                if diff.name in written_model_changes:
                    written_model_changes[diff.name].added.extend(diff.added)
                else:
                    written_model_changes[diff.name] = FieldDifference(
                        group=diff.group, name=diff.name, added=diff.added, removed=[], changed=[], unchanged=[]
                    )

            if diff.changed:
                updates = [c.new for c in diff.changed if not c.is_changed_content]
                if updates:
                    api.upsert(updates, mode="replace")
                    echo(f"Updated {len(updates)} of {diff.name}")
                content_updates = [c.new for c in diff.changed if c.is_changed_content]
                if content_updates:
                    api.delete([c.external_id for c in content_updates if c.external_id])
                    api.create(content_updates)
                    echo(f"Updated {len(content_updates)} of {diff.name} with content")
                if diff.name in written_model_changes:
                    written_model_changes[diff.name].changed.extend(diff.changed)
                else:
                    written_model_changes[diff.name] = FieldDifference(
                        group=diff.group, name=diff.name, added=[], removed=[], changed=diff.changed, unchanged=[]
                    )
        written_changes.models.append(written_model_changes)
    return written_changes


def destroy(
    client: PowerOpsClient | None,
    echo: Echo | None = None,
    model_names: str | list[str] | None = None,
    auto_yes: bool = False,
) -> ModelDifferences:
    """
    Destroys all resync models in CDF. This will also delete all data in the models.

    Args:
        client: The PowerOpsClient to use. If not provided, a new client will be created.
        echo: Function to use for printing. Defaults to print.
        model_names: The models to destroy.
        auto_yes: If true, all prompts will be auto confirmed.

    Returns:
        A ModelDifferences object containing the resources that has been destroyed.
    """
    raise NotImplementedError()


def _edges_before_nodes(diff: FieldDifference) -> int:
    if diff.group == "Domain":
        return 0
    elif diff.group == "CDF" and diff.name not in {"edges", "nodes"}:
        return 1
    elif diff.group == "CDF" and diff.name == "edges":
        return 2
    elif diff.group == "Local" and diff.name == "nodes":
        return 3
    else:
        return 4


def _nodes_before_edges(diff: FieldDifference) -> int:
    if diff.group == "Domain":
        return 0
    elif diff.group == "CDF" and diff.name not in {"edges", "nodes"}:
        return 1
    elif diff.group == "CDF" and diff.name == "nodes":
        return 2
    elif diff.group == "Local" and diff.name == "edges":
        return 3
    else:
        return 4


def _load_transform(
    market: str, path: Path, cdf_project: str, echo: Echo, model_names: str | list[str] | None
) -> list[Model]:
    model_types = _to_models(model_names)
    echo(f"Loading and transforming {', '.join([m.__name__ for m in model_types])}")

    config = ReSyncConfig.from_yamls(path, cdf_project)

    return transform(config, market, set(model_types))


def _to_models(model_names: str | list[str] | None) -> list[type[Model]]:
    if isinstance(model_names, str):
        model_names = [model_names]
    elif model_names is None:
        model_names = list(MODELS_BY_NAME)
    elif isinstance(model_names, list) and model_names and isinstance(model_names[0], str):
        model_names = model_names
    else:
        raise ValueError(f"Invalid model_names type: {type(model_names)}")

    if invalid := set(model_names) - set(MODELS_BY_NAME):
        raise ValueError(f"Invalid model names: {invalid}. Available models: {list(MODELS_BY_NAME)}")

    return [MODELS_BY_NAME[model_name] for model_name in model_names]
