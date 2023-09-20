from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.logging import RichHandler

import cognite.powerops.resync.core.echo
from cognite import powerops
from cognite.powerops import resync
from cognite.powerops.client import PowerOpsClient
from cognite.powerops.utils.cdf.extraction_pipelines import ExtractionPipelineCreate, RunStatus

for third_party in ["cognite-sdk", "requests", "urllib3", "msal", "requests_oauthlib"]:
    third_party_logger = logging.getLogger(third_party)
    third_party_logger.setLevel(logging.WARNING)
    third_party_logger.propagate = False

FORMAT = "%(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt="[%X]", handlers=[RichHandler()])

log = logging.getLogger("rich")

app = typer.Typer(pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False)


def _version_callback(value: bool):
    if value:
        typer.echo(powerops.__version__)
        raise typer.Exit()


@app.callback()
def common(ctx: typer.Context, version: bool = typer.Option(None, "--version", callback=_version_callback)):
    ...


@app.command("init", help="Setup necessary data models in CDF for ReSync to run")
def init(
    models: list[str] = typer.Option(
        default=sorted(resync.MODELS_BY_NAME),
        help=f"The models to initialize. Available models: {', '.join(resync.MODELS_BY_NAME)}",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Whether to print verbose output"),
):
    client = PowerOpsClient.from_settings()
    resync.init(client, echo=_to_echo(verbose), model_names=models)


@app.command("validate", help="Validate the configuration files")
def validate(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Whether to print verbose output"),
):
    resync.validate(path, market, echo=_to_echo(verbose))


@app.command(
    "plan",
    help="Preview the changes from the configuration files that `powerops apply` would make to the data model in CDF",
)
def plan(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
    models: list[str] = typer.Option(
        default=sorted(resync.MODELS_BY_NAME),
        help=f"The models to run the plan. Available models: {', '.join(resync.MODELS_BY_NAME)}",
    ),
    dump_folder: Optional[Path] = typer.Option(
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

    echo = _to_echo(verbose)
    echo(f"Running plan on configuration files located in {path}")
    power = PowerOpsClient.from_settings()

    changes = resync.plan(path, market, echo=echo, model_names=models, dump_folder=dump_folder, client=power)

    if format == "markdown":
        typer.echo(changes.as_github_markdown())

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
        typer.echo(f"Extraction pipeline run executed with status: {run.status}")


@app.command("apply", help="Apply the changes from the configuration files to the data model in CDF")
def apply(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
    models: list[str] = typer.Option(
        default=sorted(resync.MODELS_BY_NAME),
        help=f"The models to run apply. Available models: {', '.join(resync.MODELS_BY_NAME)}",
    ),
    auto_yes: bool = typer.Option(False, "--yes", "-y", help="Auto confirm all prompts"),
    format: str = typer.Option(default=None, help="The format of the output. Available formats: markdown"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Whether to print verbose output"),
):
    echo = _to_echo(verbose)
    client = PowerOpsClient.from_settings()

    echo(f"Running apply on configuration files located in {path}")
    changed = resync.apply(path, market, client, model_names=models, echo=_to_echo(verbose), auto_yes=auto_yes)
    if format == "markdown":
        typer.echo(changed.as_github_markdown())


@app.command("destroy", help="Destroy all the data models created by resync and remove all the data.")
def destroy(
    models: list[str] = typer.Option(
        default=sorted(resync.MODELS_BY_NAME),
        help=f"The models to destroy. Available models: {', '.join(resync.MODELS_BY_NAME)}",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Whether to run the command as a dry run, meaning no resources will be destroyed."
    ),
    auto_yes: bool = typer.Option(False, "--yes", "-y", help="Auto confirm all prompts"),
    format: str = typer.Option(default=None, help="The format of the output. Available formats: markdown"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Whether to print verbose output"),
):
    client = PowerOpsClient.from_settings()

    destroyed = resync.destroy(client, echo=_to_echo(verbose), model_names=models, auto_yes=auto_yes, dry_run=dry_run)
    if format == "markdown":
        typer.echo(destroyed.as_github_markdown())


def main():
    app()


def _to_echo(verbose: bool) -> cognite.powerops.resync.core.echo.Echo:
    if verbose:

        def echo(message: str, is_warning: bool = False) -> None:
            if is_warning:
                log.warning(message)
            else:
                log.info(message)

    else:

        def echo(message: str, is_warning: bool = False) -> None:
            ...

    return echo
