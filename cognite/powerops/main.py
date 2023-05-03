from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer()


@app.command(
    "plan",
    help="Preview the changes from the configuration files that `powerops apply` would make to the data model in CDF",
)
def plan(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
):
    typer.echo(f"Running plan on configuration files located in {path}")


@app.command("apply", help="Apply the changes from the configuration files to the data model in CDF")
def apply(path: Annotated[Path, typer.Argument(help="Path to configuration files")]):
    typer.echo(f"Running apply on configuration files located in {path}")


def main():
    app()
