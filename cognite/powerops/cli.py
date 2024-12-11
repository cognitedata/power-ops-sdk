from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.logging import Console, RichHandler

from cognite import powerops

for third_party in ["cognite-sdk", "requests", "urllib3", "msal", "requests_oauthlib"]:
    third_party_logger = logging.getLogger(third_party)
    third_party_logger.setLevel(logging.WARNING)
    third_party_logger.propagate = False

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


@app.command("pre-build", help="Create toolkit configuration files from a resync configuration file")
def pre_build(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    configuration: Annotated[Path, typer.Argument(help="Path to resync configuration file")],
    silent_mode: bool = typer.Option(False, "--silent", help="Silent mode for running in pre-commit hook"),
):
    if silent_mode:  # Turn off logging if silent mode is enabled
        log.setLevel(logging.CRITICAL)
    log.info(f"Running pre_build on configuration files located in {path}")
    powerops.resync.pre_build(path, configuration)


def main():
    app()


if __name__ == "__main__":
    main()
