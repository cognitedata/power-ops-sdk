"""
This script is used to generate the Power Ops client. It is not used in the normal workflow.
"""

from pathlib import Path
from cognite.pygen import generate_sdk

from cognite.powerops import PowerOpsClient
from cognite.client import data_modeling as dm

REPO_ROOT = Path(__file__).parent.parent


def main():
    client = PowerOpsClient.from_config("power_ops_config.yaml").cdf

    top_level = "cognite.powerops.client._generated"
    output_dir = REPO_ROOT / "cognite" / "powerops" / "client" / "_generated"

    space = "power_ops_core"
    version = "1"
    instance_space = "power_ops_instances"
    client_name = "PowerOpsModelsClient"
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

    print(f"Generating DM v1 Client for all {len(v1_models)} models")
    generate_sdk(
        [dm.DataModelId(space, external_id, version) for external_id in v1_models],
        client,
        top_level_package=top_level,
        default_instance_space=instance_space,
        client_name=client_name,
        output_dir=output_dir,
        logger=print,
        overwrite=True,
        format_code=True,
    )
    print("Done generating v1 client")


if __name__ == "__main__":
    main()
