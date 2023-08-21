from __future__ import annotations


import logging


from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Callable, overload, Any
from uuid import uuid4

from cognite.client import CogniteClient
from cognite.client.data_classes import Event

from cognite.powerops.clients.powerops_client import get_powerops_client
from cognite.powerops.resync._logger import configure_debug_logging
from cognite.powerops.resync.config.resource_collection import ResourceCollection
from cognite.powerops.resync.config.resync_config import ReSyncConfig
from cognite.powerops.resync.models.base import Model, AssetModel
from cognite.powerops.resync.to_models.transform import transform
from cognite.powerops.resync.validation import prepare_validation, perform_validation
from yaml import safe_dump

logger = logging.getLogger(__name__)

AVAILABLE_MODELS = [
    "ProductionAsset",
    "MarketAsset",
    "CogShop1Asset",
    "ProductionDataModel",
    "CogShopDataModel",
    "BenchmarkMarketDataModel",
    "DayAheadMarketDataModel",
    "RKOMMarketDataModel",
]


def plan(
    path: Path,
    market: str,
    echo: Optional[Callable[[str], None]] = None,
    model_names: Optional[str | list[str]] = None,
    dump_folder: Optional[Path] = None,
    echo_pretty: Optional[Callable[[Any], None]] = None,
) -> None:
    echo = echo or print
    echo_pretty: Callable[[Any], None] = echo_pretty or echo
    model_names = _cli_names_to_resync_names(model_names)
    client = get_powerops_client()
    bootstrap_resources, config, models = _load_transform(market, path, client.cdf.config.project, echo, model_names)
    _remove_non_existing_relationship_time_series_targets(client.cdf, models, bootstrap_resources, echo)

    for model in models:
        cdf_model = type(model).from_cdf(client, fetch_metadata=True, fetch_content=False)

        summary_diff = model.summary_diff(cdf_model)
        echo(f"Summary diff for {model.model_name}")
        echo_pretty(summary_diff)

        if dump_folder:
            dump_folder.mkdir(parents=True, exist_ok=True)

            (dump_folder / f"{model.model_name}_local.yaml").write_text(safe_dump(model.dump()))
            (dump_folder / f"{model.model_name}_cdf.yaml").write_text(safe_dump(cdf_model.dump()))
        else:
            cdf_model.difference(model, print_string=True)


@overload
def apply(
    path: Path,
    market: str,
    model_names: str,
    echo: Optional[Callable[[Any], None]] = None,
    auto_yes: bool = False,
    echo_pretty: Optional[Callable[[Any], None]] = None,
) -> Model:
    ...


@overload
def apply(
    path: Path,
    market: str,
    model_names: list[str] | None = None,
    echo: Optional[Callable[[Any], None]] = None,
    auto_yes: bool = False,
    echo_pretty: Optional[Callable[[Any], None]] = None,
) -> list[Model]:
    ...


def apply(
    path: Path,
    market: str,
    model_names: list[str] | str | None = None,
    echo: Optional[Callable[[Any], None]] = None,
    auto_yes: bool = False,
    echo_pretty: Optional[Callable[[Any], None]] = None,
) -> Model | list[Model]:
    echo = echo or print
    echo_pretty = echo_pretty or echo
    model_names = _cli_names_to_resync_names(model_names)
    client = get_powerops_client()
    collection, config, models = _load_transform(market, path, client.cdf.config.project, echo, model_names)

    collection.add(_create_bootstrap_finished_event(echo))

    _remove_non_existing_relationship_time_series_targets(client.cdf, models, collection, echo)

    summaries = {}
    for model in models:
        summaries.update(model.summary())

    echo("Models About to be uploaded")
    echo_pretty(summaries)
    ans = "y" if auto_yes else input("Continue? (y/n)")
    if ans.lower() == "y":
        # Step 3 - write bootstrap resources from diffs to CDF
        collection.write_to_cdf(
            client,
            config.settings.data_set_external_id,
            config.settings.overwrite_data,
        )
        echo("Resync written to CDF")
    else:
        echo("Aborting")

    return models[0] if len(model_names) == 1 else models


# TODO make validate an option for plan ("--validate")?
def validate(
    path: Path,
    market: str,
    model_names: list[str] | str | None = None,
    echo: Optional[Callable[[str], None]] = None,
) -> None:
    market = market.lower()
    echo = echo or print
    model_names = _cli_names_to_resync_names(model_names)
    po_client = get_powerops_client()

    # config = _load_config(path, po_client.cdf.config.project, echo)

    bootstrap_resources, config, models = _load_transform(market, path, po_client.cdf.config.project, echo, model_names)
    models = [model for model in models if type(model).__name__ in model_names and hasattr(model, "processes")]
    echo(f"Validating {len(models)} models: {', '.join([str(type(model).__name__) for model in models])}.")

    ts_validations, validation_ranges = prepare_validation(models)
    perform_validation(po_client, ts_validations, validation_ranges)


def _load_transform(
    market: str, path: Path, cdf_project: str, echo: Callable[[str], None], model_names: list[str]
) -> tuple[ResourceCollection, ReSyncConfig, list[Model]]:
    echo(f"Loading and transforming {', '.join(model_names)}")
    # Step 1 - configure and validate config
    config = ReSyncConfig.from_yamls(path, cdf_project)
    configure_debug_logging(config.settings.debug_level)
    # Step 2 - transform from config to CDF resources and preview diffs
    echo(
        f"Running resync for data set {config.settings.data_set_external_id} "
        f"in CDF project {config.settings.cdf_project}"
    )
    bootstrap_resources, models = transform(config, market, set(model_names))
    return bootstrap_resources, config, models


def _create_bootstrap_finished_event(echo: Callable[[str], None]) -> Event:
    """Creating a POWEROPS_BOOTSTRAP_FINISHED Event in CDF to signal that bootstrap scripts have been run"""
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


def _remove_non_existing_relationship_time_series_targets(
    client: CogniteClient, models: list[Model], collection: ResourceCollection, echo: Callable[[str], None]
) -> None:
    """Validates that all relationships in the collection have targets that exist in CDF"""
    to_delete = set()
    for model in models:
        if not isinstance(model, AssetModel):
            continue
        time_series = model.time_series()
        # retrieve_multiple fails if no time series are provided
        if not time_series:
            continue

        existing_time_series = client.time_series.retrieve_multiple(
            external_ids=list({t.external_id for t in time_series if t.external_id}), ignore_unknown_ids=True
        )
        existing_timeseries_ids = {ts.external_id: ts for ts in existing_time_series}
        missing_timeseries = {t.external_id for t in time_series if t.external_id not in existing_timeseries_ids}

        relationships = model.relationships()
        to_delete = {
            r.external_id
            for r in relationships
            if r.target_type and r.target_type.casefold() == "timeseries" and r.target_external_id in missing_timeseries
        }
        if to_delete:
            echo(
                f"WARNING: There are {len(to_delete)} relationships in {model.model_name} that have targets "
                "that do not exist in CDF. These relationships will not be created."
            )

        for external_id in to_delete:
            if external_id:
                collection.relationships.pop(external_id, None)


# Only needed while we support both asset models and data models
def _cli_names_to_resync_names(model_names: Optional[str | list[str]]) -> list[str]:
    """Map model names as accepted by cli to available models in resync"""
    if not model_names:
        return AVAILABLE_MODELS
    cli_names = {model_names} if isinstance(model_names, str) else set(model_names)

    res: list[str] = []
    for m in AVAILABLE_MODELS:
        res.extend(m for c in cli_names if c.casefold() in m.casefold())

    # If any of the market models are present, add the MarketAsset
    if {"dayahead", "rkom", "benchmark"}.intersection(cli_names):
        res.append("MarketAsset")
    return res


if __name__ == "__main__":
    demo_data = Path(__file__).parent.parent.parent.parent / "tests" / "test_unit" / "test_bootstrap" / "data" / "demo"

    # apply(demo_data, "DayAhead", echo=print)
    validate(demo_data, "DayAhead", echo=print)
