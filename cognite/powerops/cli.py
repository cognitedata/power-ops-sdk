from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.logging import RichHandler

from cognite.powerops.clients import get_powerops_client

from . import resync
from ._models import MODEL_BY_NAME

FORMAT = "%(message)s"
logging.basicConfig(level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()])

log = logging.getLogger("rich")

app = typer.Typer()


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


@app.command("deploy", help="Deploy the data model in CDF")
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
            model.id_, model.graphql_file.read_text(), model.name, model.description
        )
        log.info(f"Deployed {model_name} model ({result.space}, {result.external_id}, {result.version})")


def main():
    app()


if __name__ == "__main__":
    main()
