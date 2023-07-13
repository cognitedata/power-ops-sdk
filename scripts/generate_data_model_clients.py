from pathlib import Path

from cognite.pygen import generate_sdk

from cognite.powerops._models import MODEL_BY_NAME
from cognite.powerops.utils.cdf import get_cognite_client

REPO_ROOT = Path(__file__).parent.parent


def main():
    client = get_cognite_client()
    top_level_base = "cognite.powerops.clients"
    for model in MODEL_BY_NAME.values():
        generate_sdk(
            client,
            model.id_,
            top_level_package=f"{top_level_base}.{model.id_.external_id}",
            client_name=model.client_name,
            output_dir=REPO_ROOT,
            logger=print,
        )


if __name__ == "__main__":
    main()
