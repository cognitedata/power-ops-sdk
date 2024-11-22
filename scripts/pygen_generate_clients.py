"""
This script is used to generate the Power Ops client. It is not used in the normal workflow.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from cognite.pygen import generate_sdk
from rich import print
from rich.panel import Panel

from cognite.powerops import PowerOpsClient
from cognite.client import data_modeling as dm
from zmq import REP

REPO_ROOT = Path(__file__).parent.parent


def main():
    top_level = "cognite.powerops.client._generated"
    output_dir = REPO_ROOT / "cognite" / "powerops" / "client" / "_generated" / "v1"

    client = PowerOpsClient.from_config("power_ops_config.yaml").cdf

    space = "power_ops_core"
    v1_models = [
        "compute_ShopBasedDayAhead",
        "compute_TotalBidMatrixCalculation",
        "compute_WaterValueBasedDayAheadBid",
        "config_DayAheadConfiguration",
        "frontend_AFRRBid",
        "frontend_Asset",
        "frontend_DayAheadBid",
        "compute_BenchmarkingDayAhead",
    ]
    print(
        Panel(
            f"Generating DM v1 Client for all {len(v1_models)} models",
            title="Generating DM v1 client",
            style="bold blue",
        )
    )
    generate_sdk(
        [dm.DataModelId(space, external_id, "1") for external_id in v1_models],
        client,
        top_level_package=f"{top_level}.v1",
        default_instance_space="power_ops_instances",
        client_name="PowerOpsModelsV1Client",
        output_dir=output_dir,
        logger=print,
        overwrite=True,
        format_code=True,
    )
    print(Panel("Done generating v1 client", title="Done", style="bold green"))


if __name__ == "__main__":
    main()
