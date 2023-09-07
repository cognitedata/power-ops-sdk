from pathlib import Path

from cognite.pygen import generate_sdk

from cognite.powerops._models import MODEL_BY_NAME
from cognite.powerops.utils.cdf import get_cognite_client

REPO_ROOT = Path(__file__).parent.parent


def main():
    client = get_cognite_client()
    top_level = "cognite.powerops.clients"
    model_ids = [model.id_ for model in MODEL_BY_NAME.values() if model.id_.space == "power-ops"]

    generate_sdk(
        client,
        model_ids,
        top_level_package=top_level,
        client_name="PowerOpsClient",
        output_dir=REPO_ROOT,
        logger=print,
        pydantic_version="v2",
        overwrite=True,
        format_code=True,
    )


if __name__ == "__main__":
    main()
