from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Callable, Any, Type
from uuid import uuid4
from cognite.client.data_classes import Event
from cognite.client.data_classes.data_modeling.instances import InstanceCore
from yaml import safe_dump

from cognite.powerops.client.powerops_client import get_powerops_client, PowerOpsClient
from cognite.powerops.resync._validation import _clean_relationships
from cognite.powerops.resync.config._main import ReSyncConfig
from cognite.powerops.resync.models.base import Model  # type: ignore[attr-defined]
from cognite.powerops.resync import models
from cognite.powerops.resync._changes import FieldDifference, ModelDifferences, ModelDifference
from cognite.powerops.resync.to_models.transform import transform
from cognite.powerops.resync.utils.cdf import get_cognite_api
from cognite.powerops.utils.navigation import all_concrete_subclasses
from cognite.powerops.utils.cdf import Settings

MODEL_BY_NAME: dict[str, Type[Model]] = {
    model.__name__: model for model in all_concrete_subclasses(Model)  # type: ignore[type-abstract]
}
AVAILABLE_MODELS: frozenset[str] = frozenset(MODEL_BY_NAME)
DEFAULT_MODELS: frozenset[str] = frozenset(
    [m.__name__ for m in [models.ProductionModel, models.MarketModel, models.CogShop1Asset]]
)


def plan(
    path: Path,
    market: str,
    echo: Optional[Callable[[str], None]] = None,
    model_names: Optional[str | list[str]] = None,
    dump_folder: Optional[Path] = None,
    client: PowerOpsClient | None = None,
) -> ModelDifferences:
    echo = echo or print
    settings = Settings()

    client = client or get_powerops_client()

    loaded_models = _load_transform(market, path, client.cdf.config.project, echo, model_names)

    echo(f"Load transform completed, models {', '.join([type(m).__name__ for m in loaded_models])} loaded")
    if settings.powerops.read_dataset is None:
        raise ValueError("No read_dataset configured in settings")
    data_set_external_id = settings.powerops.read_dataset
    all_differences = []
    for new_model in loaded_models:
        echo(f"Retrieving {new_model.model_name} from CDF")
        cdf_model = type(new_model).from_cdf(client, data_set_external_id=data_set_external_id)

        differences = cdf_model.difference(new_model)
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
    path: Path,
    market: str,
    model_names: list[str] | str | None = None,
    echo: Optional[Callable[[Any], None]] = None,
    auto_yes: bool = False,
    verbose: bool = False,
) -> ModelDifferences:
    echo = echo or print
    client = get_powerops_client()

    loaded_models = _load_transform(market, path, client.cdf.config.project, echo, model_names or list(DEFAULT_MODELS))

    settings = Settings()
    if settings.powerops.read_dataset is None or settings.powerops.write_dataset is None:
        raise ValueError("No read_dataset or write_dataset configured in settings")
    read_dataset = settings.powerops.read_dataset
    retrieved = client.cdf.data_sets.retrieve(external_id=settings.powerops.write_dataset)
    if retrieved is None or retrieved.id is None:
        raise ValueError(f"Could not find write_dataset {settings.powerops.write_dataset}")

    write_dataset = retrieved.id

    written_changes = ModelDifferences([])
    for new_model in loaded_models:
        cdf_model = type(new_model).from_cdf(client, data_set_external_id=read_dataset)
        new_sequences_by_id = {s.external_id: s for s in new_model.sequences()}
        new_files_by_id = {f.external_id: f for f in new_model.files()}

        differences = cdf_model.difference(new_model)
        _clean_relationships(client.cdf, differences, new_model, echo)
        written_model_changes = ModelDifference(new_model.model_name)
        changed = []
        for diff in differences:
            if diff.group == "Domain":
                continue
            if len(diff.unchanged) == diff.total:
                if verbose:
                    echo(f"No changes detected for {diff.name} in {new_model.model_name}")
                continue
            elif diff.name == "timeseries":
                if verbose:
                    echo("Found timeseries changes, skipping. These are not updated by resync")
                continue
            changed.append(diff)

        for diff in sorted(changed, key=_edges_before_nodes):
            if not diff.removed:
                continue
            if verbose:
                echo(f"Removals detected for {diff.name} in {new_model.model_name}")
                echo(f"Remove count: {diff.as_summary().removed}")
                diff_ids = diff.as_ids(5)
                echo(f"Sample removed for {diff_ids.name}: {diff_ids.removed}")
            ans = "y" if auto_yes else input("Continue? (y/n)")
            if ans.lower() != "y":
                if verbose:
                    echo("Aborting")
                continue

            api = get_cognite_api(client.cdf, diff.name, new_sequences_by_id, new_files_by_id)
            api.delete(
                external_id=[
                    r.as_id() if isinstance(r, InstanceCore) else r.external_id for r in diff.removed if r.external_id
                ]
            )
            if verbose:
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
            if verbose:
                echo(f"Changes/Additions detected for {diff.name} in {new_model.model_name}")
                summary_count = diff.as_summary()
                echo(f"Change count: {summary_count.changed}")
                echo(f"Addition count: {summary_count.added}")
            diff_ids = diff.as_ids(5)
            if diff_ids.changed and verbose:
                echo(f"Sample changed for {diff_ids.name}:")
                for change in diff.changed[:3]:
                    echo(change.changed_fields)
            if diff_ids.added and verbose:
                echo(f"Sample added for {diff_ids.name}: {diff_ids.added}")
            ans = "y" if auto_yes else input("Continue? (y/n)")
            if ans.lower() != "y":
                if verbose:
                    echo("Aborting")
                continue

            diff.set_set_dataset(write_dataset)
            api = get_cognite_api(client.cdf, diff.name, new_sequences_by_id, new_files_by_id)

            if diff.added:
                api.create(diff.added)
                if verbose:
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
                    if verbose:
                        echo(f"Updated {len(updates)} of {diff.name}")
                content_updates = [c.new for c in diff.changed if c.is_changed_content]
                if content_updates:
                    api.delete([c.external_id for c in content_updates if c.external_id])
                    api.create(content_updates)
                    if verbose:
                        echo(f"Updated {len(content_updates)} of {diff.name} with content")
                if diff.name in written_model_changes:
                    written_model_changes[diff.name].changed.extend(diff.changed)
                else:
                    written_model_changes[diff.name] = FieldDifference(
                        group=diff.group, name=diff.name, added=[], removed=[], changed=diff.changed, unchanged=[]
                    )
        written_changes.models.append(written_model_changes)
    return written_changes


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
    market: str, path: Path, cdf_project: str, echo: Callable[[str], None], model_names: str | list[str] | None
) -> list[Model]:
    if isinstance(model_names, str):
        model_names = [model_names]
    elif model_names is None:
        model_names = list(DEFAULT_MODELS)
    elif isinstance(model_names, list) and model_names and isinstance(model_names[0], str):
        model_names = model_names
    else:
        raise ValueError(f"Invalid model_names type: {type(model_names)}")

    if invalid := set(model_names) - AVAILABLE_MODELS:
        raise ValueError(f"Invalid model names: {invalid}. Available models: {AVAILABLE_MODELS}")

    echo(f"Loading and transforming {', '.join(model_names)}")

    config = ReSyncConfig.from_yamls(path, cdf_project)

    return transform(config, market, {MODEL_BY_NAME[model_name] for model_name in model_names})


def _create_bootstrap_finished_event(echo: Callable[[str], None]) -> Event:
    """Creating a POWEROPS_BOOTSTRAP_FINISHED Event in CDF to signal that bootstrap scripts have been ran"""
    current_time = int(datetime.now(timezone.utc).timestamp() * 1000)  # in milliseconds
    event = Event(
        start_time=current_time,
        end_time=current_time,
        external_id=f"POWEROPS_BOOTSTRAP_FINISHED_{str(uuid4())}",
        type="POWEROPS_BOOTSTRAP_FINISHED",
        source="PowerOps bootstrap",
        description="Manual run of bootstrap scripts finished",
    )
    echo(f"Created status event '{event.external_id}'")

    return event


if __name__ == "__main__":
    demo_data = Path(__file__).parent.parent.parent.parent / "tests" / "test_unit" / "test_bootstrap" / "data" / "demo"

    apply(demo_data, "DayAhead", echo=print)
