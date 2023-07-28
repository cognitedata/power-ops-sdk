from cognite.powerops.clients.powerops_client import get_powerops_client
from cognite.powerops.resync.config.resync_config import ProductionConfig
from cognite.powerops.resync.models.production import ProductionModel
from cognite.powerops.resync.to_models.to_production_model import to_production_model
from tests.constants import REPO_ROOT

from pprint import pprint

DEMO_DATA = REPO_ROOT / "tests" / "test_unit" / "test_bootstrap" / "data" / "demo"


def main():
    client = get_powerops_client().cdf

    config = ProductionConfig.load_yamls(DEMO_DATA / "production", instantiate=True)
    local_model = to_production_model(config)
    # pprint(sorted(local_model.reservoirs, key=lambda x: x.name))
    pprint(local_model.generators[0])
    # pprint(sorted(local_model.generators, key=lambda x: x.name))

    print("-------------------")
    cdf_model = ProductionModel.from_cdf(client)

    pprint(cdf_model.generators[0])
    # local_model.difference(cdf_model)


if __name__ == "__main__":
    main()
