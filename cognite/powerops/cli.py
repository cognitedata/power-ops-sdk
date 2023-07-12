from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import cognite.client
import typer
from rich.logging import RichHandler

from cognite import powerops
from cognite.powerops.clients import get_powerops_client

from . import resync
from ._models import MODEL_BY_NAME

FORMAT = "%(message)s"
logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler(tracebacks_suppress=[cognite.client])]
)

log = logging.getLogger("rich")

app = typer.Typer()


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
):
    log.info(f"Running plan on configuration files located in {path}")
    resync.plan(path, market)


@app.command("apply", help="Apply the changes from the configuration files to the data model in CDF")
def apply(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
):
    log.info(f"Running apply on configuration files located in {path}")

    resync.apply(path, market)


@app.command("deploy", help=f"Deploy the data model in CDF. Available models: {list(MODEL_BY_NAME.keys())}")
def deploy(
    models: Annotated[list[str], typer.Argument(help="The models to deploy")],
):
    client = get_powerops_client()
    for model_name in models:
        if model_name not in MODEL_BY_NAME:
            log.warning(f"Model {model_name} not found, skipping. Available models: {list(MODEL_BY_NAME.keys())}")
            continue
        log.info(f"Deploying {model_name} model...")

        model = MODEL_BY_NAME[model_name]
        result = client.cdf.data_modeling.graphql.apply_dml(
            model.id_, model.graphql_file.read_text().replace("\n", " "), model.name, model.description
        )
        log.info(f"Deployed {model_name} model ({result.space}, {result.external_id}, {result.version})")


@app.command("show", help=f"Show the graphql schema of Power Ops model. Available models: {list(MODEL_BY_NAME.keys())}")
def show(
    model: Annotated[str, typer.Argument(help="The models to deploy")],
):
    if model not in MODEL_BY_NAME:
        log.warning(f"Model {model} not found. Available models: {list(MODEL_BY_NAME.keys())}")

    print(f"{MODEL_BY_NAME[model].graphql_file.read_text()!r}")


def main():
    app()


if __name__ == "__main__":
    main()
