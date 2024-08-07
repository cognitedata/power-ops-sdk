from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.logging import Console, RichHandler

from cognite import powerops
from cognite.powerops.utils.cdf.extraction_pipelines import ExtractionPipelineCreate, RunStatus

for third_party in ["cognite-sdk", "requests", "urllib3", "msal", "requests_oauthlib"]:
    third_party_logger = logging.getLogger(third_party)
    third_party_logger.setLevel(logging.WARNING)
    third_party_logger.propagate = False

MODELS_BY_NAME_SORTED = sorted(powerops.resync.MODELS_BY_NAME)
MODELS_BY_NAME_STR = ", ".join(MODELS_BY_NAME_SORTED)

FORMAT = "%(message)s"
logging.basicConfig(
    level=logging.INFO, format=FORMAT, datefmt="[%X]", handlers=[RichHandler(console=Console(stderr=True))]
)

log = logging.getLogger("rich")

app = typer.Typer(pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False)


def _version_callback(value: bool):
    if value:
        typer.echo(powerops.__version__)
        raise typer.Exit()


@app.callback()
def common(ctx: typer.Context, version: bool = typer.Option(None, "--version", callback=_version_callback)): ...


@app.command("init", help="Setup necessary data models in CDF for ReSync to run")
def init(
    dev: bool = typer.Option(
        False,
        "--dev",
        "-d",
        help="Whether the deployment is for development environment. If true,"
        "the models views and data models will be deleted and recreated.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Whether to run the command as a dry run, meaning no resources will be created."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Whether to print verbose output"),
):
    client = powerops.PowerOpsClient.from_settings()

    powerops.resync.init(client, is_dev=dev, dry_run=dry_run, verbose=verbose)


@app.command("validate", help="Validate the configuration files and timeseries")
def validate(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Include valid results in output (specify --format), and enable INFO logs"
    ),
    format: str = typer.Option(default=None, help="The format of the output. Available formats: markdown, json"),
    error_code: bool = typer.Option(False, "--error-code", "-e", help="Exit with error code if validation fails"),
):
    echo = _setup_echo(verbose, typer.echo)
    log.info(f"Running validate on configuration files located in {path}")
    validation_results = powerops.resync.validate(path, market)

    if format == "markdown":
        echo(validation_results.as_markdown(include_valid=verbose))
    elif format == "json":
        echo(validation_results.as_json(include_valid=verbose))

    if error_code and not all(result.valid for result in validation_results.results):
        raise typer.Exit(code=1)


@app.command(
    "plan",
    help="Preview the changes from the configuration files that `powerops apply` would make to the data model in CDF",
)
def plan(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
    models: list[str] = typer.Option(  # noqa: B008
        default=MODELS_BY_NAME_SORTED,
        help=f"The models to run the plan. Available models: {MODELS_BY_NAME_STR}",
    ),
    dump_folder: Optional[Path] = typer.Option(  # noqa: B008
        default=None, help="If present, the local and cdf changes will be dumped to this directory."
    ),
    format: str = typer.Option(default=None, help="The format of the output. Available formats: markdown"),
    as_extraction_pipeline_run: bool = typer.Option(
        default=False,
        help="If true, the command will be registered as an extraction pipeline run. With the configuration"
        "fetched from the settings.toml [powerops] section.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Whether to print verbose output"),
):
    if dump_folder and not dump_folder.is_dir():
        raise typer.BadParameter(f"{dump_folder} is not a directory")

    echo = _setup_echo(verbose, typer.echo)
    log.info(f"Running plan on configuration files located in {path}")
    power = powerops.PowerOpsClient.from_settings()

    changes = powerops.resync.plan(path, market, model_names=models, dump_folder=dump_folder, client=power)

    if format == "markdown":
        echo(changes.as_github_markdown())

    if as_extraction_pipeline_run is True:
        client = power.cdf

        pipeline = ExtractionPipelineCreate(
            external_id="resync/plan",
            data_set_external_id=power.datasets.monitor_dataset,
            dump_truncated_to_file=True,
            message_keys_skip=["error"],
            truncate_keys_first=["error"],
            log_file_prefix="powerops_function_loss",
            description="The resync/plan function checks that the configuration files are matching "
            "the expected resources in CDF. If there are any differences, the run will report as failed",
        ).get_or_create(client)

        with pipeline.create_pipeline_run(client) as run:
            if changes.has_changes("CDF", exclude={"timeseries", "parent_assets", "labels"}):
                run.update_data(
                    RunStatus.FAILURE,
                    summary=changes.as_markdown_summary(no_headers=True),
                    error=changes.as_markdown_detailed(),
                )
            else:
                run.update_data(RunStatus.SUCCESS)
        echo(f"Extraction pipeline run executed with status: {run.status}")


@app.command("apply", help="Apply the changes from the configuration files to the data model in CDF")
def apply(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
    models: list[str] = typer.Option(  # noqa: B008
        default=MODELS_BY_NAME_SORTED,
        help=f"The models to run apply. Available models: {MODELS_BY_NAME_STR}",
    ),
    auto_yes: bool = typer.Option(False, "--yes", "-y", help="Auto confirm all prompts"),
    format: str = typer.Option(default=None, help="The format of the output. Available formats: markdown"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Whether to print verbose output"),
):
    echo = _setup_echo(verbose, typer.echo)
    client = powerops.PowerOpsClient.from_settings()

    logging.info(f"Running apply on configuration files located in {path}")
    changed = powerops.resync.apply(path, market, client, model_names=models, auto_yes=auto_yes)
    if format == "markdown":
        echo(changed.as_github_markdown())


@app.command("destroy", help="Destroy all the data models created by resync and remove all the data.")
def destroy(
    models: list[str] = typer.Option(  # noqa: B008
        default=MODELS_BY_NAME_SORTED,
        help=f"The models to destroy. Available models: {MODELS_BY_NAME_STR}",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Whether to run the command as a dry run, meaning no resources will be destroyed."
    ),
    auto_yes: bool = typer.Option(False, "--yes", "-y", help="Auto confirm all prompts"),
    format: str = typer.Option(default=None, help="The format of the output. Available formats: markdown"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Whether to print verbose output"),
):
    echo = _setup_echo(verbose, typer.echo)
    client = powerops.PowerOpsClient.from_settings()

    destroyed = powerops.resync.destroy(client, model_names=models, auto_yes=auto_yes, dry_run=dry_run)
    if format == "markdown":
        echo(destroyed.as_github_markdown())


@app.command("plan_v1", help="Plan the changes from the configuration files to the data model in CDF")
def plan_v1(path: Annotated[Path, typer.Argument(help="Path to configuration files")]):
    logging.info(f"Running plan on configuration files located in {path}")
    powerops.resync.plan_v1(path)


@app.command("apply_v1", help="Apply the changes from the configuration files to the data model in CDF")
def apply_v1(path: Annotated[Path, typer.Argument(help="Path to configuration files")]):
    logging.info(f"Running apply on configuration files located in {path}")
    powerops.resync.apply_v1(path)


def main():
    app()


def _setup_echo(verbose: bool, echo_func: Optional[powerops.resync.Echo] = None) -> powerops.resync.Echo:
    if not verbose:
        logging.disable(logging.INFO)  # disable logs for INFO and below

    return echo_func or print


if __name__ == "__main__":
    main()
