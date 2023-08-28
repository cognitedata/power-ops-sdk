from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Callable, Any, Type
from uuid import uuid4

from cognite.client.data_classes import Event
from yaml import safe_dump

from cognite.powerops.clients.powerops_client import get_powerops_client, PowerOpsClient
from cognite.powerops.resync._logger import configure_debug_logging
from cognite.powerops.resync.config.resync_config import ReSyncConfig
from cognite.powerops.resync.models.base import Model
from cognite.powerops.resync import models
from cognite.powerops.resync.models.base.model import FieldDifference
from cognite.powerops.resync.to_models.transform import transform
from cognite.powerops.resync.utils.common import all_concrete_subclasses
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
    echo_pretty: Optional[Callable[[Any], None]] = None,
    client: PowerOpsClient | None = None,
) -> dict[str, list[FieldDifference]]:
    echo = echo or print
    echo_pretty: Callable[[Any], None] = echo_pretty or echo
    settings = Settings()

    client = client or get_powerops_client()

    loaded_models = _load_transform(market, path, client.cdf.config.project, echo, model_names)

    echo(f"Load transform completed, models {', '.join([type(m).__name__ for m in loaded_models])} loaded")
    if settings.powerops.read_dataset is None:
        raise ValueError("No read_dataset configured in settings")
    data_set_external_id = settings.powerops.read_dataset
    model_diff_by_name = {}
    for new_model in loaded_models:
        echo(f"Retrieving {new_model.model_name} from CDF")
        cdf_model = type(new_model).from_cdf(client, data_set_external_id=data_set_external_id)

        differences = cdf_model.difference(new_model)
        echo(f"Summary diff for {new_model.model_name}")
        echo_pretty([diff.as_summary() for diff in differences])

        if dump_folder:
            dump_folder.mkdir(parents=True, exist_ok=True)

            (dump_folder / f"{new_model.model_name}_local.yaml").write_text(safe_dump(new_model.dump_as_cdf_resource()))
            (dump_folder / f"{new_model.model_name}_cdf.yaml").write_text(safe_dump(cdf_model.dump_as_cdf_resource()))

        echo(f"External ids diff for {new_model.model_name}")
        echo_pretty([diff.as_ids(limit=10) for diff in differences])
        model_diff_by_name[new_model.model_name] = differences
    return model_diff_by_name


def apply(
    path: Path,
    market: str,
    model_names: list[str] | str | None = None,
    echo: Optional[Callable[[Any], None]] = None,
    auto_yes: bool = False,
    echo_pretty: Optional[Callable[[Any], None]] = None,
) -> None:
    echo = echo or print
    echo_pretty = echo_pretty or echo
    client = get_powerops_client()

    loaded_models = _load_transform(market, path, client.cdf.config.project, echo, model_names or list(DEFAULT_MODELS))

    settings = Settings()
    if settings.powerops.read_dataset is None or settings.powerops.write_dataset is None:
        raise ValueError("No read_dataset or write_dataset configured in settings")
    read_dataset = settings.powerops.read_dataset

    for new_model in loaded_models:
        cdf_model = type(new_model).from_cdf(client, data_set_external_id=read_dataset)
        differences = cdf_model.difference(new_model)
        for diff in differences:
            if diff.unchanged == diff.total:
                echo(f"No changes detected for {diff.name} in {new_model.model_name}")
                continue
            echo(f"Changes detected for {diff.name} in {new_model.model_name}")
            echo_pretty(diff.as_summary())
            ans = "y" if auto_yes else input("Continue? (y/n)")
            if ans.lower() != "y":
                echo("Aborting")
                continue
            # api = get_api(diff.name)
            #
            # if diff.added:
            #     api.create(client, diff.added, data_set_external_id=write_dataset)
            # elif diff.removed:
            #     api.delete(client, diff.removed.as_ids())
            # elif diff.changed:
            #     api.upsert(client, diff.changed, data_set_external_id=write_dataset)

        # _remove_non_existing_relationship_time_series_targets(client.cdf, models, echo)

        # summaries = {}
        # for model in models:
        #     summaries.update(model.summary())

        # echo("Models About to be uploaded")
        # echo_pretty(summaries)
        # ans = "y" if auto_yes else input("Continue? (y/n)")
        # if ans.lower() == "y":
        #     # Step 3 - write bootstrap resources from diffs to CDF
        #     collection.write_to_cdf(
        #         client,
        #         config.settings.data_set_external_id,
        #         config.settings.overwrite_data,
        #     )
        #     echo("Resync written to CDF")
        # else:
        #     echo("Aborting")
        #
        # return models[0] if len(models) == 1 else models


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
    configure_debug_logging(config.settings.debug_level)

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


# def _remove_non_existing_relationship_time_series_targets(
#     client: CogniteClient, models: list[Model],echo: Callable[[str], None]
# ) -> None:
#     """Validates that all relationships in the collection have targets that exist in CDF"""
#     to_delete = set()
#     for model in models:
#         if not isinstance(model, AssetModel):
#             continue
#         time_series = model.timeseries()
#         # retrieve_multiple fails if no time series are provided
#         if not time_series:
#             continue
#
#         existing_time_series = client.time_series.retrieve_multiple(
#             external_ids=list({t.external_id for t in time_series if t.external_id}), ignore_unknown_ids=True
#         )
#         existing_timeseries_ids = {ts.external_id: ts for ts in existing_time_series}
#         missing_timeseries = {t.external_id for t in time_series if t.external_id not in existing_timeseries_ids}
#
#         relationships = model.relationships()
#         to_delete = {
#             r.external_id
#             for r in relationships
#             if r.target_type and r.target_type.casefold() == "timeseries"
#             and r.target_external_id in missing_timeseries
#         }
#         if to_delete:
#             echo(
#                 f"WARNING: There are {len(to_delete)} relationships in {model.model_name} that have targets "
#                 "that do not exist in CDF. These relationships will not be created."
#             )
#
#         for external_id in to_delete:
#             if external_id:
#                 collection.relationships.pop(external_id, None)


if __name__ == "__main__":
    demo_data = Path(__file__).parent.parent.parent.parent / "tests" / "test_unit" / "test_bootstrap" / "data" / "demo"

    apply(demo_data, "DayAhead", echo=print)
