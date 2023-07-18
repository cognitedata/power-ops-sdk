from cognite.powerops.clients.powerops_client import get_powerops_client
from cognite.powerops.resync.config.resync_config import ProductionConfig
from cognite.powerops.resync.models.production import ProductionModel
from cognite.powerops.resync.to_models.to_production_model import to_production_model
from tests.constants import REPO_ROOT

DEMO_DATA = REPO_ROOT / "tests" / "test_unit" / "test_bootstrap" / "data" / "demo"


def main():
    client = get_powerops_client().cdf

    config = ProductionConfig.load_yamls(DEMO_DATA / "production", instantiate=True)
    local_model = to_production_model(config)
    print("local_model")
    # print(type(local_model))
    print(1)
    print(local_model.watercourses[0].production_obligation_time_series)
    # print(2)
    print("-----------------------")

    cdf_model = ProductionModel.from_cdf(client)
    print("cdf_model")
    print(type(cdf_model))
    print(3)
    print(cdf_model.watercourses[0].production_obligation_time_series)
    print(4)
    print("-----------------------")

    print(local_model.difference(cdf_model))


if __name__ == "__main__":
    main()
