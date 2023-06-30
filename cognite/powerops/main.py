from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from cognite.powerops.bootstrap import (
    _create_cdf_resources,
    _load_config,
    _preview_resources_diff,
    _transform,
    validate_config,
)
from cognite.powerops.logger import configure_debug_logging

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

    # Step 1 - configure and validate config
    config = _load_config(path)
    config = validate_config(config)
    configure_debug_logging(config.constants.debug_level)

    # Step 2 - transform from config to CDF resources and preview diffs
    # 2.a - transform
    bootstrap_resources = _transform(
        config,
        path,
        market,
    )

    # 2.b - preview diff
    _preview_resources_diff(
        bootstrap_resources,
        config.constants.data_set_external_id,
    )


@app.command("apply", help="Apply the changes from the configuration files to the data model in CDF")
def apply(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Selected power market")],
):
    typer.echo(f"Running apply on configuration files located in {path}")

    # Step 1 - configure and validate config
    config = _load_config(path)
    config = validate_config(config)
    configure_debug_logging(config.constants.debug_level)

    # Step 2 - transform from config to CDF resources and preview diffs
    bootstrap_resources = _transform(
        config,
        path,
        market,
    )

    # Step 3 - write bootstrap resources from diffs to CDF
    _create_cdf_resources(
        bootstrap_resources,
        config.constants.data_set_external_id,
        config.constants.overwrite_data,
        config.constants.skip_dm,
    )


def main():
    app()


if __name__ == "__main__":
    main()
