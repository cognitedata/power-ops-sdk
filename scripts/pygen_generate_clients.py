"""
This script is used to generate the Power Ops client. It is not used in the normal workflow.
"""
import os
from pathlib import Path

from cognite.pygen import generate_sdk

from cognite.powerops.resync.models.v1.graphql_schemas import GRAPHQL_MODELS as v1
from cognite.powerops.resync.models.v2.graphql_schemas import GRAPHQL_MODELS as v2
from cognite.powerops.resync.models.v2.dms import CapacityModel
from cognite.powerops.utils.cdf import get_cognite_client
from cognite.powerops.utils.serialization import chdir

REPO_ROOT = Path(__file__).parent.parent


def main():
    top_level = "cognite.powerops.client._generated"
    model_ids = [model.id_ for model in v2.values()] + [CapacityModel.id_]

    # Ensure we are in the root of the repo
    with chdir(REPO_ROOT):
        os.environ["SETTINGS_FILES"] = "settings.toml"
        client = get_cognite_client()
        generate_sdk(
            model_ids,
            client,
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
            v1["cogshop1"].id_,
            client,
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
