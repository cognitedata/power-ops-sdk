"""
This module contains the main functions for the resync tool.
"""

from __future__ import annotations

import itertools
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Optional, cast

from click.core import Command
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import DataModelId, MappedProperty, ViewList
from cognite.client.exceptions import CogniteAPIError
from cognite_toolkit._cdf_tk.loaders import DataModelLoader as ToolkitDataModelLoader  # type: ignore[import-untyped]
from cognite_toolkit._cdf_tk.loaders import ViewLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, calculate_directory_hash  # type: ignore[import-untyped]
from rich import print
from rich.panel import Panel
from typer import Context
from yaml import safe_dump

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.resync import diff, models
from cognite.powerops.resync.config import ReSyncConfig
from cognite.powerops.resync.diff import FieldDifference, ModelDifference, ModelDifferences
from cognite.powerops.resync.models.base import AssetModel, CDFFile, CDFSequence, DataModel, Model, SpaceId
from cognite.powerops.resync.models.v2.powerops_models import DataModelLoader
from cognite.powerops.utils.serialization import environment_variables

from .cdf import get_cognite_api
from .transform import transform
from .validation import _clean_relationships

os.environ["SENTRY_ENABLED"] = "false"
# ruff: noqa: E402
from cognite_toolkit._cdf import Common, build, deploy  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

MODELS_BY_NAME = {m.__name__: m for m in itertools.chain(models.V1_MODELS, models.V2_MODELS)}

DATAMODEL_ID_TO_RESYNC_NAME: dict[DataModelId, str] = {
    m.data_model_ids()[0]: m.__name__
    for m in itertools.chain(models.V1_MODELS, models.V2_MODELS)
    if issubclass(m, DataModel) and m.data_model_ids()
}


def init(client: PowerOpsClient | None, is_dev: bool = False, dry_run: bool = False, verbose: bool = False) -> None:
    """
    This function will create the data models in CDF that are required for resync to work. It will not overwrite
    existing models.

    Args:
        client: The PowerOpsClient to use. If not provided, a new client will be created.
        is_dev: Whether the deployment is for development environment. If true, the models views and data models
                will be deleted and recreated.
        dry_run: Whether to run the command as a dry run, meaning no resources will be created.
    """
    client = client or PowerOpsClient.from_settings()
    cdf = client.cdf
    result = re.findall(r"^https://([\w-]+).cognitedata.com", cdf.config.base_url)
    if result:
        cluster = result[0]
    else:
        raise ValueError(f"Invalid base_url {cdf.config.base_url}")

    with environment_variables(
        {
            "CDF_CLUSTER": cluster,
            "CDF_URL": cdf.config.base_url,
            "IDP_CLIENT_ID": cdf.config.credentials.client_id,  # type: ignore[attr-defined]
            "IDP_CLIENT_SECRET": "dummy",
            "IDP_TOKEN_URL": (
                cdf.config.credentials.authority_url
                if hasattr(cdf.config.credentials, "authority_url")
                else cdf.config.credentials.token_url  # type: ignore[attr-defined]
            ),
            "CDF_PROJECT": cdf.config.project,
        }
    ):
        tool_config = CDFToolConfig()
        tool_config._client = cdf

    powerops_folder = Path(__file__).parent.parent.parent
    with environment_variables({"SENTRY_ENABLED": "false"}):
        ctx = Context(Command("build"))
        ctx.obj = Common(
            override_env=False,
            verbose=verbose,
            cluster=cluster,
            project=cdf.config.project,
            mockToolGlobals=tool_config,
        )
        build(ctx=ctx, source_dir=str(powerops_folder), build_dir="build", build_env="dev", clean=True)

    build_folder = Path.cwd() / "build"

    loader = DataModelLoader(build_folder)
    schema = loader.load()
    logger.info("Loaded all powerops data models")
    # TODO: fix validation
    DataModelLoader.validate(schema)
    logger.info("Validated all powerops data models")

    # Nodes should not be deployed, so we remove them from the build folder
    for node_file in build_folder.glob("data_models/*.powerops_nodes.yaml"):
        node_file.unlink()

    build_hash = calculate_directory_hash(build_folder)
    filepath = Path(tempfile.gettempdir()) / "powerops_init_command.txt"
    if dry_run:
        filepath.write_text(build_hash)
    elif filepath.exists() and filepath.read_text() == build_hash:
        ...
    else:
        print(
            Panel(
                "[bold red]Error: [/] `powerops init` has not been run with --dry-run before running without it."
                "Please run `powerops init` with --dry-run first to verify the changes.",
                title="No dry-run",
            )
        )
        exit(1)

    if is_dev:
        changed = loader.changed_views(cdf, schema, verbose)
        if changed:
            print(Panel(f"Detected {len(changed)} changed views"))
            if verbose:
                for view in changed:
                    logger.info(f"Changed view: {view}")
            dependencies = {view_id for dependencies in changed.values() for view_id in dependencies}
            print(f"Detected {len(dependencies)} dependent views")
            prefix = "Would delete" if dry_run else "Deleting"
            to_delete = list(dependencies | set(changed))
            print(f"{prefix} changed and dependent {len(to_delete)} views")

            if not dry_run:
                deleted = cdf.data_modeling.views.delete(to_delete)
                print(f"Deleted {len(deleted)} views")

            data_model_ids_to_delete = set(loader.dependent_data_models(schema, set(to_delete)))
        else:
            data_model_ids_to_delete = set()
            print(Panel("No changes detected in any views"))

        existing = cdf.data_modeling.data_models.retrieve(schema.data_models.as_ids())
        existing_by_id = {model.as_id(): model for model in existing}
        loader = ToolkitDataModelLoader.create_loader(tool_config)
        for local_data_model in schema.data_models:
            if local_data_model.as_id() not in existing_by_id:
                continue
            existing_data_model = existing_by_id[local_data_model.as_id()]

            if not loader._is_equal_custom(local_data_model, existing_data_model):  # type: ignore[attr-defined]
                data_model_ids_to_delete.add(local_data_model.as_id())

        print(Panel(f"Detected {len(data_model_ids_to_delete)} dependent and changed data models"))
        prefix = "Would delete" if dry_run else "Deleting"
        print(f"{prefix} dependent {len(data_model_ids_to_delete)} data models")
        if not dry_run:
            deleted_models = cdf.data_modeling.data_models.delete(list(data_model_ids_to_delete))
            print(f"Deleted {len(deleted_models)} data models")

    with environment_variables({"SENTRY_ENABLED": "false"}):
        ctx = Context(Command("deploy"))
        ctx.obj = Common(
            override_env=False,
            verbose=verbose,
            cluster=cluster,
            project=cdf.config.project,
            mockToolGlobals=tool_config,
        )
        try:
            deploy(
                ctx,
                build_dir="build",
                build_env="dev",
                interactive=False,
                drop=False,
                drop_data=False,
                dry_run=dry_run,
                include=None,
            )
        except SystemExit as e:
            if e.code != 0 and not dry_run:
                # Toolkit currently calls apply once for each view, which typically fails due to dependencies.
                # We try to deploy the views first, and then retry the deploy.
                print(Panel("Trying deploying views first", title="Deploy failed"))
                view_loader = ViewLoader.create_loader(tool_config)
                view_files = view_loader.find_files(build_folder / "data_models")
                views = [
                    view_loader.load_resource(view_file, tool_config, skip_validation=False) for view_file in view_files
                ]
                created = cdf.data_modeling.views.apply(views)
                print(f"Created {len(created)} views. Retrying deploy...")
                deploy(
                    ctx,
                    build_dir="build",
                    build_env="dev",
                    interactive=False,
                    drop=False,
                    drop_data=False,
                    dry_run=dry_run,
                    include=None,
                )
            else:
                raise


def validate(config_dir: str | Path, market: str) -> Any:
    raise NotImplementedError("validate is not implemented")


# def validate(config_dir: str | Path, market: str) -> ValidationResults:
#     """
#     Validates the local configuration files.
#
#     Args:
#         config_dir: Local path to the configuration files. Needs to follow a specific structure. See below.
#         market: The market to load the configuration for.
#
#     Configuration file structure:
#     ```
#     📦config_dir
#      ┣ 📂cogshop - The CogSHOP configuration
#      ┣ 📂market - The Market configuration for DayAhead, RKOM, and benchmarking.
#      ┣ 📂production - The physical assets configuration, Watercourse, PriceArea, Generator, Plant  (SHOP centered)
#      ┗ 📜settings.yaml - Settings for resync.
#     ```
#     """
#


def plan(
    config_dir: Path,
    market: str,
    client: PowerOpsClient | None,
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
     ┣ 📂production - The physical assets configuration, Watercourse, PriceArea, Generator, Plant  (SHOP centered)
     ┗ 📜settings.yaml - Settings for resync.
    ```
    """
    client = client or PowerOpsClient.from_settings()

    loaded_models = _load_transform(market, config_dir, client.cdf.config.project, model_names)

    logger.info(f"Load transform completed, models {', '.join([type(m).__name__ for m in loaded_models])} loaded")
    if client.datasets.read_dataset is None:
        raise ValueError("No read_dataset configured in settings")
    data_set_external_id = client.datasets.read_dataset
    all_differences = []
    for new_model in loaded_models:
        logger.info(f"Retrieving {new_model.model_name} from CDF")
        cdf_model = type(new_model).from_cdf(client, data_set_external_id=data_set_external_id)

        if isinstance(new_model, AssetModel):
            static_resources = new_model.static_resources_from_cdf(client)
        else:
            static_resources = {}

        differences = diff.model_difference(cdf_model, new_model, static_resources)
        _clean_relationships(client.cdf, differences, new_model)

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
     ┣ 📂production - The physical assets configuration, Watercourse, PriceArea, Generator, Plant  (SHOP centered)
     ┗ 📜settings.yaml - Settings for resync.
    ```
    """
    client = client or PowerOpsClient.from_settings()
    loaded_models = _load_transform(market, config_dir, client.cdf.config.project, model_names)

    written_changes = ModelDifferences([])
    for new_model in loaded_models:
        cdf_model = type(new_model).from_cdf(client, data_set_external_id=client.datasets.read_dataset)
        if isinstance(new_model, AssetModel):
            static_resources = new_model.static_resources_from_cdf(client)
        else:
            static_resources = {}

        differences = diff.model_difference(cdf_model, new_model, static_resources)

        # Do not create relationships to time series that does not exist.
        _clean_relationships(client.cdf, differences, new_model)
        # Remove the domain model as this is not CDF resources
        # TimeSeries are not updated by resync.
        differences.filter_out(group="Domain", field_names={"timeseries"})

        new_sequences_by_id = {s.external_id: s for s in new_model.sequences()}
        new_files_by_id = {f.external_id: f for f in new_model.files()}

        removed = _remove_resources(differences, client.cdf, auto_yes)
        added_updated = _add_update_resources(differences, client, auto_yes, new_sequences_by_id, new_files_by_id)

        written_changes.append(removed + added_updated)

    return written_changes


def destroy(
    client: PowerOpsClient | None,
    model_names: str | list[str] | None = None,
    auto_yes: bool = False,
    dry_run: bool = False,
) -> ModelDifferences:
    """
    Destroys all resync models in CDF. This will also delete all data in the models.

    Args:
        client: The PowerOpsClient to use. If not provided, a new client will be created.
        model_names: The models to destroy.
        auto_yes: If true, all prompts will be auto confirmed.
        dry_run: If true, the models will not be deleted, but the changes will be printed.

    Returns:
        A ModelDifferences object containing the resources that has been destroyed.
    """
    client = client or PowerOpsClient.from_settings()

    model_types = _to_models(model_names)
    destroyed = ModelDifferences([])
    for model_type in model_types:
        if issubclass(model_type, DataModel) and model_type.graph_ql:
            remove_data_model = _get_data_model_view_containers(
                client.cdf, model_type.graph_ql.id_, model_type.__name__
            )
            if not remove_data_model.changes:
                logger.warning(f"Skipping {model_type.__name__}, no data model found")
                continue
            static_resources = {}
        elif issubclass(model_type, DataModel) and model_type.dms_model:
            raise NotImplementedError()
        elif issubclass(model_type, DataModel) and model_type.source_model:
            raise NotImplementedError()
        elif issubclass(model_type, AssetModel):
            remove_data_model = ModelDifference(model_type.__name__, {})
            if issubclass(model_type, models.MarketModel):
                # We only need the root asset to be set.
                production_external_id = cast(str, models.ProductionModel.root_asset.external_id)
                model_type.set_root_asset("", "", "", production_external_id)
            static_resources = model_type.static_resources_from_cdf(client)
        else:
            raise ValueError(f"Unknown model type {model_type}")

        cdf_model = model_type.from_cdf(client, data_set_external_id=client.datasets.read_dataset)

        remove_data = diff.remove_only(cdf_model, static_resources)
        remove_data.filter_out(group="Domain", field_names={"timeseries"})

        if dry_run:
            destroyed.append(remove_data + remove_data_model)
        else:
            removed = _remove_resources(remove_data + remove_data_model, client.cdf, auto_yes)
            destroyed.append(removed)

    loader = DataModelLoader(Path.cwd() / "build")
    schema = loader.load()
    loader.destroy(client.cdf, schema, dry_run)

    # Spaces are deleted last, as they might contain other resources.
    spaces = set(space for d in model_types if issubclass(d, DataModel) for space in d.spaces())
    if spaces and not dry_run:
        deleted_space: list[SpaceId] = []
        # One at a time, in case there are other resources in the space that will prevent deletion.
        for space in spaces:
            logger.info(f"Deleting space {space}..")
            try:
                client.cdf.data_modeling.spaces.delete(list(spaces))
            except CogniteAPIError as e:
                logger.warning(f"Failed to delete space {space} with error {e}")
            else:
                logger.info(f"... deleted space {space}")
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
    if not dry_run:
        labels = AssetLabel.as_label_definitions() + RelationshipLabel.as_label_definitions()
        client.cdf.labels.delete([label.external_id for label in labels if label.external_id])

    return destroyed


def _remove_resources(differences: ModelDifference, cdf: CogniteClient, auto_yes: bool) -> ModelDifference:
    removed = ModelDifference(model_name=differences.model_name, changes={})
    for difference in sorted(differences, key=_remove_order):
        if not difference.removed:
            continue
        logger.info(f"Removals detected for {difference.field_name} in {differences.model_name}")
        logger.info(f"Remove count: {difference.as_summary().removed}")
        diff_ids = difference.as_ids(5)
        logger.info(f"Sample removed for {diff_ids.field_name}: {diff_ids.removed}")
        answer = "y" if auto_yes else input("Continue? (y/n)")
        if answer.lower() != "y":
            logger.info("Aborting")
            continue

        api = get_cognite_api(cdf, difference.field_name)

        api.delete(external_id=difference.removed_ids)

        logger.info(f"Deleted {len(difference.removed)} of {difference.field_name}")
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
    client: PowerOpsClient,
    auto_yes: bool,
    sequences_by_id: dict[str, CDFSequence],
    files_by_id: dict[str, CDFFile],
) -> ModelDifference:
    added_updated = ModelDifference(model_name=differences.model_name, changes={})
    for difference in sorted(differences, key=_adding_order):
        if not difference.added and not difference.changed:
            continue
        logger.info(f"Changes/Additions detected for {difference.field_name} in {differences.model_name}")
        summary_count = difference.as_summary()
        logger.info(f"Change count: {summary_count.changed}")
        logger.info(f"Addition count: {summary_count.added}")
        diff_ids = difference.as_ids(5)
        if diff_ids.changed:
            logger.info(f"Sample changed for {diff_ids.field_name}:")
            for change in difference.changed[:3]:
                logger.info(change.changed_fields)
        if diff_ids.added:
            logger.info(f"Sample added for {diff_ids.field_name}: {diff_ids.added}")
        answer = "y" if auto_yes else input("Continue? (y/n)")
        if answer.lower() != "y":
            logger.info("Aborting")
            continue

        difference.set_set_dataset(client.datasets.write_dataset_id)
        api = get_cognite_api(client.cdf, difference.field_name, sequences_by_id, files_by_id)

        if difference.added:
            api.create(difference.added)
            logger.info(f"Created {len(difference.added)} of {difference.field_name}")
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
                logger.info(f"Updated {len(updates)} of {difference.field_name}")
            content_updates = [c.new for c in difference.changed if c.is_changed_content]
            if content_updates:
                api.delete([c.external_id for c in content_updates if c.external_id])
                api.create(content_updates)
                logger.info(f"Updated {len(content_updates)} of {difference.field_name} with content")
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


def _load_transform(market: str, path: Path, cdf_project: str, model_names: str | list[str] | None) -> list[Model]:
    model_types = _to_models(model_names)
    logger.info(f"Loading and transforming {', '.join([m.__name__ for m in model_types])}")

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
