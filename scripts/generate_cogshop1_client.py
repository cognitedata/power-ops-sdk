from pathlib import Path

from cognite.pygen import generate_sdk

from cognite.powerops.utils.cdf import get_cognite_client

REPO_ROOT = Path(__file__).parent.parent


def main():
    client = get_cognite_client()
    top_level = "cognite.powerops.cogshop1"
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
