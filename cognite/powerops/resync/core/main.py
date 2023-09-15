"""
This module contains the main functions for the resync tool.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import DataModelId, MappedProperty, SpaceApply, SpaceList, ViewList
from cognite.client.exceptions import CogniteAPIError
from yaml import safe_dump

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.resync import diff, models
from cognite.powerops.resync.config import ReSyncConfig
from cognite.powerops.resync.diff import FieldDifference, ModelDifference, ModelDifferences
from cognite.powerops.resync.models.base import AssetModel, CDFFile, CDFSequence, DataModel, Model, SpaceId

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


def validate(config_dir: str | Path, market: str, echo: Echo | None = None) -> None:
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
    _ = _load_transform(market, Path(config_dir), "dummy", echo, list(MODELS_BY_NAME))
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

        if isinstance(new_model, AssetModel):
            static_resources = new_model.static_resources_from_cdf(client)
        else:
            static_resources = {}

        differences = diff.model_difference(cdf_model, new_model, static_resources)
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
    client: PowerOpsClient | None = None,
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

    written_changes = ModelDifferences([])
    for new_model in loaded_models:
        cdf_model = type(new_model).from_cdf(client, data_set_external_id=client.datasets.read_dataset)
        if isinstance(new_model, AssetModel):
            static_resources = new_model.static_resources_from_cdf(client)
        else:
            static_resources = {}

        differences = diff.model_difference(cdf_model, new_model, static_resources)

        # Do not create relationships to time series that does not exist.
        _clean_relationships(client.cdf, differences, new_model, echo)
        # Remove the domain model as this is not CDF resources
        # TimeSeries are not updated by resync.
        differences.filter_out(group="Domain", field_names={"timeseries"})

        new_sequences_by_id = {s.external_id: s for s in new_model.sequences()}
        new_files_by_id = {f.external_id: f for f in new_model.files()}

        removed = _remove_resources(differences, echo, client.cdf, auto_yes)
        added_updated = _add_update_resources(differences, echo, client, auto_yes, new_sequences_by_id, new_files_by_id)

        written_changes.append(removed + added_updated)

    return written_changes


def destroy(
    client: PowerOpsClient | None,
    echo: Echo | None = None,
    model_names: str | list[str] | None = None,
    auto_yes: bool = False,
    dry_run: bool = False,
) -> ModelDifferences:
    """
    Destroys all resync models in CDF. This will also delete all data in the models.

    Args:
        client: The PowerOpsClient to use. If not provided, a new client will be created.
        echo: Function to use for printing. Defaults to print.
        model_names: The models to destroy.
        auto_yes: If true, all prompts will be auto confirmed.
        dry_run: If true, the models will not be deleted, but the changes will be printed.

    Returns:
        A ModelDifferences object containing the resources that has been destroyed.
    """
    echo = echo or _default_echo
    client = client or PowerOpsClient.from_settings()

    model_types = _to_models(model_names)
    destroyed = ModelDifferences([])
    for model_type in model_types:
        if issubclass(model_type, DataModel):
            remove_data_model = _get_data_model_view_containers(
                client.cdf, model_type.graph_ql.id_, model_type.__name__
            )
            if not remove_data_model.changes:
                echo(f"Skipping {model_type.__name__}, no data model found", is_warning=True)
                continue
            static_resources = {}
        elif issubclass(model_type, AssetModel):
            remove_data_model = ModelDifference(model_type.__name__, {})
            static_resources = model_type.static_resources_from_cdf(client)
        else:
            raise ValueError(f"Unknown model type {model_type}")

        cdf_model = model_type.from_cdf(client, data_set_external_id=client.datasets.read_dataset)

        remove_data = diff.remove_only(cdf_model, static_resources)
        remove_data.filter_out(group="Domain", field_names={"timeseries"})

        if dry_run:
            destroyed.append(remove_data + remove_data_model)
        else:
            removed = _remove_resources(remove_data + remove_data_model, echo, client.cdf, auto_yes)
            destroyed.append(removed)

    # Spaces are deleted last, as they might contain other resources.
    spaces = set(d.graph_ql.id_.space for d in model_types if issubclass(d, DataModel))
    if spaces and not dry_run:
        deleted_space: list[SpaceId] = []
        # One at a time, in case there are other resources in the space that will prevent deletion.
        for space in spaces:
            echo(f"Deleting space {space}..")
            try:
                client.cdf.data_modeling.spaces.delete(list(spaces))
            except CogniteAPIError as e:
                echo(f"Failed to delete space {space} with error {e}", is_warning=True)
            else:
                echo(f"... deleted space {space}")
                deleted_space.append(SpaceId(space))
        if deleted_space:
            destroyed.append(
                ModelDifference(
                    model_name="All Models",
                    changes={
                        "spaces": FieldDifference(
                            group="CDF",
                            field_name="spaces",
                            removed=list(deleted_space),
                            added=[],
                            changed=[],
                            unchanged=[],
                        )
                    },
                )
            )

    labels = AssetLabel.as_label_definitions() + RelationshipLabel.as_label_definitions()
    client.cdf.labels.delete([label.external_id for label in labels if label.external_id])

    return destroyed


def _remove_resources(differences: ModelDifference, echo: Echo, cdf: CogniteClient, auto_yes: bool) -> ModelDifference:
    removed = ModelDifference(model_name=differences.model_name, changes={})
    for difference in sorted(differences, key=_remove_order):
        if not difference.removed:
            continue
        echo(f"Removals detected for {difference.field_name} in {differences.model_name}")
        echo(f"Remove count: {difference.as_summary().removed}")
        diff_ids = difference.as_ids(5)
        echo(f"Sample removed for {diff_ids.field_name}: {diff_ids.removed}")
        ans = "y" if auto_yes else input("Continue? (y/n)")
        if ans.lower() != "y":
            echo("Aborting")
            continue

        api = get_cognite_api(cdf, difference.field_name)

        api.delete(external_id=difference.removed_ids)

        echo(f"Deleted {len(difference.removed)} of {difference.field_name}")
        removed.changes[difference.field_name] = FieldDifference(
            group=difference.group,
            field_name=difference.field_name,
            added=[],
            removed=difference.removed,
            changed=[],
            unchanged=difference.unchanged,
        )
    return removed


def _add_update_resources(
    differences: ModelDifference,
    echo: Echo,
    client: PowerOpsClient,
    auto_yes: bool,
    sequences_by_id: dict[str, CDFSequence],
    files_by_id: dict[str, CDFFile],
) -> ModelDifference:
    added_updated = ModelDifference(model_name=differences.model_name, changes={})
    for difference in sorted(differences, key=_adding_order):
        if not difference.added and not difference.changed:
            continue
        echo(f"Changes/Additions detected for {difference.field_name} in {differences.model_name}")
        summary_count = difference.as_summary()
        echo(f"Change count: {summary_count.changed}")
        echo(f"Addition count: {summary_count.added}")
        diff_ids = difference.as_ids(5)
        if diff_ids.changed:
            echo(f"Sample changed for {diff_ids.field_name}:")
            for change in difference.changed[:3]:
                echo(change.changed_fields)
        if diff_ids.added:
            echo(f"Sample added for {diff_ids.field_name}: {diff_ids.added}")
        ans = "y" if auto_yes else input("Continue? (y/n)")
        if ans.lower() != "y":
            echo("Aborting")
            continue

        difference.set_set_dataset(client.datasets.write_dataset_id)
        api = get_cognite_api(client.cdf, difference.field_name, sequences_by_id, files_by_id)

        if difference.added:
            api.create(difference.added)
            echo(f"Created {len(difference.added)} of {difference.field_name}")
            if difference.field_name in added_updated:
                added_updated[difference.field_name].added.extend(difference.added)
            else:
                added_updated[difference.field_name] = FieldDifference(
                    group=difference.group,
                    field_name=difference.field_name,
                    added=difference.added,
                    removed=[],
                    changed=[],
                    unchanged=[],
                )

        if difference.changed:
            updates = [c.new for c in difference.changed if not c.is_changed_content]
            if updates:
                api.upsert(updates, mode="replace")
                echo(f"Updated {len(updates)} of {difference.field_name}")
            content_updates = [c.new for c in difference.changed if c.is_changed_content]
            if content_updates:
                api.delete([c.external_id for c in content_updates if c.external_id])
                api.create(content_updates)
                echo(f"Updated {len(content_updates)} of {difference.field_name} with content")
            if difference.field_name in added_updated:
                added_updated[difference.field_name].changed.extend(difference.changed)
            else:
                added_updated[difference.field_name] = FieldDifference(
                    group=difference.group,
                    field_name=difference.field_name,
                    added=[],
                    removed=[],
                    changed=difference.changed,
                    unchanged=[],
                )
    return added_updated


def _remove_order(diff: FieldDifference) -> int:
    if diff.group == "Domain":
        return 0
    elif diff.group == "CDF" and diff.field_name not in {
        "edges",
        "nodes",
        "containers",
        "views",
        "data_models",
        "spaces",
        "parent_assets",
        "labels",
    }:
        return 1
    elif diff.group == "CDF" and diff.field_name == "edges":
        return 2
    elif diff.group == "CDF" and diff.field_name == "nodes":
        return 3
    elif diff.group == "CDF" and diff.field_name == "data_models":
        return 4
    elif diff.group == "CDF" and diff.field_name == "views":
        return 5
    elif diff.group == "CDF" and diff.field_name == "containers":
        return 6
    elif diff.group == "CDF" and diff.field_name == "spaces":
        return 7
    elif diff.group == "CDF" and diff.field_name == "parent_assets":
        return 8
    elif diff.group == "CDF" and diff.field_name == "labels":
        return 9
    else:
        return 8


def _adding_order(diff: FieldDifference) -> int:
    if diff.group == "Domain":
        return 0
    elif diff.group == "CDF" and diff.field_name in {"parent_assets", "labels"}:
        return 1
    elif diff.group == "CDF" and diff.field_name not in {"edges", "nodes"}:
        return 2
    elif diff.group == "CDF" and diff.field_name == "nodes":
        return 3
    elif diff.group == "CDF" and diff.field_name == "edges":
        return 4
    else:
        return 5


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


def _get_data_model_view_containers(cdf: CogniteClient, data_model_id: DataModelId, model_name: str) -> ModelDifference:
    data_model = cdf.data_modeling.data_models.retrieve(ids=data_model_id, inline_views=True)
    if not data_model:
        return ModelDifference(model_name=model_name, changes={})
    data_model = data_model.latest_version()
    views = data_model.views

    changes = {
        "views": FieldDifference(group="CDF", field_name="views", removed=list(ViewList(views).as_ids())),
        "containers": FieldDifference(
            group="CDF",
            field_name="containers",
            removed=list(
                {
                    prop.container
                    for view in views
                    for prop in view.properties.values()
                    if isinstance(prop, MappedProperty)
                }
            ),
        ),
        "data_models": FieldDifference(group="CDF", field_name="data_models", removed=[data_model.as_id()]),
    }

    return ModelDifference(model_name=model_name, changes=changes)
