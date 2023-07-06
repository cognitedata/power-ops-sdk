from pathlib import Path

from cognite.pygen import generate_sdk

from cognite.powerops.utils.cdf import get_cognite_client

REPO_ROOT = Path(__file__).parent.parent


def main():
    client = get_cognite_client()
    top_level_base = "cognite.powerops.clients"
    generate_sdk(
        client,
        ("cogShop", "CogShop", "1"),
        top_level_package=f"{top_level_base}.cogshop",
        client_name="CogShopClient",
        output_dir=REPO_ROOT,
        logger=print,
    )

    generate_sdk(
        client,
        ("power-ops", "Core", "1"),
        top_level_package=f"{top_level_base}.core",
        client_name="CoreClient",
        output_dir=REPO_ROOT,
        logger=print,
    )

    generate_sdk(
        client,
        ("power-ops", "MarketConfiguration", "1"),
        top_level_package=f"{top_level_base}.market_configuration",
        client_name="MarketConfigClient",
        output_dir=REPO_ROOT,
        logger=print,
    )


if __name__ == "__main__":
    main()
