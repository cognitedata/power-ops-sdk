"""
This script is used to generate the Power Ops client. It is not used in the normal workflow.
"""

from pathlib import Path

from cognite.pygen import generate_sdk

from cognite.powerops.resync.models import v1, v2
from cognite.powerops.utils.cdf import get_cognite_client
from cognite.powerops.utils.io_file import chdir

REPO_ROOT = Path(__file__).parent.parent


def main():
    client = get_cognite_client()
    top_level = "cognite.powerops.client._generated"
    model_ids = [model.id_ for model in v2.GRAPHQL_MODELS.values()]

    # Ensure we are in the root of the repo
    with chdir(REPO_ROOT):
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
        generate_sdk(
            client,
            v1.GRAPHQL_MODELS["cogshop1"].id_,
            top_level_package=f"{top_level}.cogshop1",
            client_name="CogShop1Client",
            output_dir=REPO_ROOT,
            logger=print,
            pydantic_version="v2",
            overwrite=True,
            format_code=True,
        )


if __name__ == "__main__":
    main()
