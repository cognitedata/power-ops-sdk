"""
This script is used to generate the Power Ops client. It is not used in the normal workflow.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from cognite.pygen import generate_sdk
from rich import print
from rich.panel import Panel

from cognite.powerops.utils.cdf import get_cognite_client
from cognite.powerops.utils.serialization import chdir
from cognite.client import data_modeling as dm

REPO_ROOT = Path(__file__).parent.parent


@dataclass
class Model:
    model_id: dm.DataModelId
    directory: str
    client_name: str


def main():
    top_level = "cognite.powerops.client._generated"

    # Ensure we are in the root of the repo
    with chdir(REPO_ROOT):
        os.environ["SETTINGS_FILES"] = "settings.toml;.secrets.toml"
        client = get_cognite_client()
        # These are commented out as we plan to move away from these
        # models, and thus we don't want to maintain them (keep them up to date with
        # the newest pygen).
        # generate_sdk(
        #     model_ids,
        #     client,
        #     top_level_package=top_level,
        #     client_name="GeneratedPowerOpsClient",
        #     output_dir=REPO_ROOT,
        #     logger=print,
        #     pydantic_version="v2",
        #     overwrite=True,
        #     format_code=True,
        # )
        #
        # # The cogshop1 model must be in a different package as it has overlapping names with the CogShop model in
        # # the power-ops space.
        # generate_sdk(
        #     v1["cogshop1"].id_,
        #     client,
        #     top_level_package=f"{top_level}.cogshop1",
        #     client_name="CogShop1Client",
        #     output_dir=REPO_ROOT,
        #     logger=print,
        #     pydantic_version="v2",
        #     overwrite=True,
        #     format_code=True,
        # )
        models = [
            Model(
                model_id=dm.DataModelId("power-ops-afrr-bid", "AFRRBid", "1"),
                directory="afrr_bid",
                client_name="AFRRBidAPI",
            ),
            Model(
                model_id=dm.DataModelId("power-ops-day-ahead-bid", "DayAheadBid", "1"),
                directory="day_ahead_bid",
                client_name="DayAheadBidAPI",
            ),
            Model(
                model_id=dm.DataModelId("power-ops-assets", "PowerAsset", "1"),
                directory="assets",
                client_name="PowerAssetAPI",
            ),
        ]
        print(
            Panel(
                f"Generating DM v0 Clients for all {len(models)} models",
                title="Generating DM v0 clients",
                style="bold blue",
            )
        )
        for model in models:
            generate_sdk(
                model.model_id,
                client,
                top_level_package=f"{top_level}.{model.directory}",
                default_instance_space="power-ops" if model.model_id.space == "power-ops" else "power-ops-instance",
                client_name=model.client_name,
                output_dir=REPO_ROOT,
                logger=print,
                pydantic_version="v2",
                overwrite=True,
                format_code=True,
            )
        print(Panel("Done generating v0 clients", title="Done", style="bold green"))

        space = "sp_powerops_models"
        v1_models = [
            "compute_SHOPBasedDayAhead",
            "compute_TotalBidCalculation",
            "compute_WaterValueBasedDayAheadBid",
            "config_DayAheadConfiguration",
            "frontend_AFRRBid",
            "frontend_Asset",
            "frontend_DayAheadBid",
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
            default_instance_space="sp_powerops_instance",
            client_name="PowerOpsModelsV1Client",
            output_dir=REPO_ROOT,
            logger=print,
            pydantic_version="v2",
            overwrite=True,
            format_code=True,
        )
        print(Panel("Done generating v1 client", title="Done", style="bold green"))


if __name__ == "__main__":
    main()
