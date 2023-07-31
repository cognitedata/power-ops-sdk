from cognite.powerops.clients.powerops_client import get_powerops_client
from cognite.powerops.resync.models.production import ProductionModel
from tests.constants import REPO_ROOT

from pprint import pprint

DEMO_DATA = REPO_ROOT / "tests" / "test_unit" / "test_bootstrap" / "data" / "demo"


def main():
    client = get_powerops_client().cdf

    # config = ProductionConfig.load_yamls(DEMO_DATA / "production", instantiate=True)
    # local_model = to_production_model(config)
    # pprint(local_model.generators[0].model_dump())

    # print("-------------------")
    cdf_model = ProductionModel.from_cdf(
        client,
        fetch_metadata=True,
        fetch_content=False,
    )

    pprint(cdf_model.model_dump())
    print("-------------------")

    # pprint(cdf_model.generators[0].model_dump())

    # local_model.difference(cdf_model, True)


if __name__ == "__main__":
    main()
    # client = get_powerops_client().cdf
