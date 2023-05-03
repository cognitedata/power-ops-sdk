from typing import Annotated

import typer

from pathlib import Path

app = typer.Typer()


@app.command("plan", help="Compare CDF resource creation between desired state (configuration files)"
                          " and remote state (resources in CDF) and print differences")
def plan(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
):
    typer.echo(
        f"Running plan on configuration files located at {path}"
    )



@app.command("apply", help="Apply differences between desired and actual state. Writes desired state to CDF")
def apply(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")]
):
    typer.echo(
        f"Running apply on configuration files located at {path}"
    )


def main():
    app()
