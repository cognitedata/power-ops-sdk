from pathlib import Path
from typing import Annotated

import typer

import cognite.powerops.utils.mapping.heco_mapping as heco_mapping
import cognite.powerops.utils.mapping.lyse_mapping as lyse_mapping

# from cognite.powerops._run import _run
from cognite.powerops.config import DATA, BootstrapConfig

# from cognite.powerops.logger import configure_debug_logging

app = typer.Typer()


@app.command(
    "plan",
    help="Preview the changes from the configuration files that `powerops apply` would make to the data model in CDF",
)
def plan(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[str, typer.Argument(help="Path to configuration files")],
):
    typer.echo(f"Running plan on configuration files located in {path} for market {market}")
    case = str(path).split("/")[-1]
    config = BootstrapConfig.from_yamls(DATA / case)

    if case in ["lyse", "demo"]:
        time_series_mappings = lyse_mapping.create_time_series_mapping(case, config)
    elif case == "heco":
        time_series_mappings = heco_mapping.create_time_series_mapping(case, config)

    print(time_series_mappings)  # dummy to surpass pre-commit

    # _run(config, case, time_series_mappings) #TODO: Split into parts that print diffs and writes diffs


@app.command("apply", help="Apply the changes from the configuration files to the data model in CDF")
def apply(
    path: Annotated[Path, typer.Argument(help="Path to configuration files")],
    market: Annotated[Path, typer.Argument(help="Path to configuration files")] = "Dayahead",
):
    typer.echo(f"Running apply on configuration files located in {path} for market {market}")


def main():
    app()
