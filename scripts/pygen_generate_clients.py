"""
This script is used to generate the Power Ops client. It is not used in the normal workflow.
"""

from pathlib import Path

from cognite.pygen import generate_sdk

from cognite.powerops._models import MODEL_BY_NAME
from cognite.powerops.utils.cdf import get_cognite_client

REPO_ROOT = Path(__file__).parent.parent


def main():
    client = get_cognite_client()
    top_level = "cognite.powerops.client._generated"
    model_ids = [model.id_ for model in MODEL_BY_NAME.values() if model.id_.space == "power-ops"]

    generate_sdk(
        client,
        model_ids,
        top_level_package=top_level,
        client_name="GeneratedPowerOpsClient",
        output_dir=REPO_ROOT,
        logger=print,
        pydantic_version="v2",
        overwrite=True,
        format_code=True,
    )

    # The cogshop1 model must be in a different package as it has overlapping names with the CogShop model in
    # the power-ops space.
    top_level = "cognite.powerops._generated.cogshop1"
    model_id = ("cogShop", "CogShop", "2")

    generate_sdk(
        client,
        model_id,
        top_level_package=top_level,
        client_name="CogShop1Client",
        output_dir=REPO_ROOT,
        logger=print,
        pydantic_version="v2",
        overwrite=True,
        format_code=True,
    )


if __name__ == "__main__":
    main()
