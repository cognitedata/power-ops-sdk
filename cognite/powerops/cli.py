from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer

from cognite import powerops

for third_party in ["cognite-sdk", "requests", "urllib3", "msal", "requests_oauthlib"]:
    third_party_logger = logging.getLogger(third_party)
    third_party_logger.setLevel(logging.WARNING)
    third_party_logger.propagate = False

app = typer.Typer(pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False)


def setup_logger(silent: bool, file_path: Path | None):
    logger = logging.getLogger("resync")
    level = logging.CRITICAL if silent else logging.INFO
    logger.setLevel(level)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # File handler
    if file_path:
        file_handler = logging.FileHandler(file_path)
        file_handler.setLevel(level)

    # Formatter
    formatter = logging.Formatter("%(message)s")
    console_handler.setFormatter(formatter)
    if file_path:
        file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    if file_path:
        logger.addHandler(file_handler)

    return logger


def _version_callback(value: bool):
    if value:
        typer.echo(powerops.__version__)
        raise typer.Exit()


@app.callback()
def common(ctx: typer.Context, version: bool = typer.Option(None, "--version", callback=_version_callback)): ...


@app.command("pre-build", help="Create toolkit configuration files from a resync configuration file")
def pre_build(
    path: Annotated[Path, typer.Argument(help="Path to CDF configuration file")],
    configuration: Annotated[Path, typer.Argument(help="Path to resync configuration file")],
    silent_mode: bool = typer.Option(False, "--silent", help="Silent mode for running in pre-commit hook"),
    logs: Path | None = typer.Option(None, "--logs", help="Path to file where logs should be printed out"),  # noqa: B008
):
    logger = setup_logger(silent=silent_mode, file_path=logs)
    logger.info(f"Running pre_build on configuration files located in {path}")
    powerops.resync.pre_build(path, configuration)


@app.command("purge", help="Deletes any data in CDF that is not included in the resync pre-build output")
def purge(
    path: Annotated[Path, typer.Argument(help="Path to CDF configuration file")],
    configuration: Annotated[Path, typer.Argument(help="Path to resync configuration file")],
    silent_mode: bool = typer.Option(False, "--silent", help="Silent mode for running in pre-commit hook"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Dry run mode for running in pre-commit hook"),
    verbose: bool = typer.Option(False, "--verbose", help="Verbose mode for listing all nodes to be deleted"),
    logs: Path | None = typer.Option(None, "--logs", help="Path to file where logs should be printed out"),  # noqa: B008
):
    logger = setup_logger(silent=silent_mode, file_path=logs)
    logger.info(f"Resync Purge {'' if not dry_run else 'dry run'}")
    powerops.resync.purge(path, configuration, dry_run, verbose)


def main():
    app()


if __name__ == "__main__":
    main()
