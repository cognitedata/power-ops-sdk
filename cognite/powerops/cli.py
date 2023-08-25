from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.pretty import pprint

from cognite import powerops
from cognite.powerops import resync
from cognite.powerops._models import MODEL_BY_NAME
from cognite.powerops.clients.powerops_client import get_powerops_client

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("msal").setLevel(logging.WARNING)
logging.getLogger("cognite-sdk").setLevel(logging.WARNING)

FORMAT = "%(message)s"
logging.basicConfig(level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()])

log = logging.getLogger("rich")

app = typer.Typer(pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False)


def _version_callback(value: bool):
    if value:
        typer.echo(powerops.__version__)
        raise typer.Exit()


@app.callback()
def common(
    ctx: typer.Context,
    version: bool = typer.Option(None, "--version", callback=_version_callback),
):
    ...


@app.command(
    "plan",
    help="Preview the changes from the configuration files that `powerops apply` would make to the data model in CDF",
)
def plan(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
    models: list[str] = typer.Option(
        default=sorted(resync.DEFAULT_MODELS),
        help=f"The models to run the plan. Available models: {', '.join(resync.MODEL_BY_NAME)}",
    ),
    dump_folder: Optional[Path] = typer.Option(
        default=None, help="If present, the local and cdf changes will be dumped to this directory."
    ),
):
    if dump_folder and not dump_folder.is_dir():
        raise typer.BadParameter(f"{dump_folder} is not a directory")

    log.info(f"Running plan on configuration files located in {path}")
    if len(models) == 1 and models[0].lower() == "all":
        models = list(MODEL_BY_NAME.keys())

    resync.plan(path, market, echo=log.info, model_names=models, dump_folder=dump_folder, echo_pretty=pprint)


@app.command("apply", help="Apply the changes from the configuration files to the data model in CDF")
def apply(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
    models: list[str] = typer.Option(
        default=sorted(resync.DEFAULT_MODELS),
        help=f"The models to run apply. Available models: {', '.join(resync.MODEL_BY_NAME)}",
    ),
    auto_yes: bool = typer.Option(False, "--yes", "-y", help="Auto confirm all prompts"),
):
    log.info(f"Running apply on configuration files located in {path}")

    resync.apply(path, market, model_names=models, echo=log.info, auto_yes=auto_yes, echo_pretty=pprint)


@app.command(
    "deploy",
    help=f"Deploy the data model in CDF. Available models: {list(MODEL_BY_NAME.keys())}. "
    f"Use 'all' to deploy all models.",
)
def deploy(
    models: Annotated[list[str], typer.Argument(help="The models to deploy")],
):
    if len(models) == 1 and models[0].lower() == "all":
        models = list(MODEL_BY_NAME.keys())

    client = get_powerops_client()
    for model_name in models:
        if model_name not in MODEL_BY_NAME:
            log.warning(f"Model {model_name} not found, skipping. Available models: {list(MODEL_BY_NAME.keys())}")
            continue
        log.info(f"Deploying {model_name} model...")

        model = MODEL_BY_NAME[model_name]
        result = client.cdf.data_modeling.graphql.apply_dml(
            model.id_,
            model.graphql,
            model.name,
            model.description,
        )
        log.info(f"Deployed {model_name} model ({result.space}, {result.external_id}, {result.version})")


@app.command("show", help=f"Show the graphql schema of Power Ops model. Available models: {list(MODEL_BY_NAME.keys())}")
def show(
    model: Annotated[str, typer.Argument(help="The models to deploy")],
    remove_newlines: bool = typer.Option(
        False,
        "--remove-newlines",
        help="Remove newlines from the graphql schema. This is done when deploying the schema.",
    ),
):
    if model not in MODEL_BY_NAME:
        log.warning(f"Model {model} not found. Available models: {list(MODEL_BY_NAME.keys())}")
        typer.Exit()

    console = Console()
    graphql = MODEL_BY_NAME[model].graphql
    if remove_newlines:
        graphql = graphql.replace("\n", "")
        console.print(f"{graphql!r}")
    else:
        console.print(graphql)


def main():
    app()


if __name__ == "__main__":
    deploy(["dayahead"])
