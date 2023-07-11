from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from . import resync

app = typer.Typer()


@app.command(
    "plan",
    help="Preview the changes from the configuration files that `powerops apply` would make to the data model in CDF",
)
def plan(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
):
    typer.echo(f"Running plan on configuration files located in {path}")
    resync.plan(path, market)


@app.command("apply", help="Apply the changes from the configuration files to the data model in CDF")
def apply(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
):
    typer.echo(f"Running apply on configuration files located in {path}")

    resync.apply(path, market)


def main():
    app()


if __name__ == "__main__":
    main()
