from pprint import pprint
from cognite.powerops.clients.powerops_client import get_powerops_client
from cognite.powerops.resync.config.resync_config import ProductionConfig
from cognite.powerops.resync.models.base import _DiffFormatter
from cognite.powerops.resync.models.helpers import format_change_unary
from cognite.powerops.resync.to_models.to_production_model import to_production_model
from cognite.powerops.resync.models.production import ProductionModel
from tests.constants import REPO_ROOT

from deepdiff import DeepDiff

DEMO_DATA = REPO_ROOT / "tests" / "test_unit" / "test_bootstrap" / "data" / "demo"


def main():
    client = get_powerops_client().cdf

    cdf_model = ProductionModel.from_cdf(
        client,
        fetch_metadata=True,
        fetch_content=True,
    )

    config = ProductionConfig.load_yamls(DEMO_DATA / "production", instantiate=True)
    local_model = to_production_model(config)
    _ = local_model.difference(cdf_model)
    # pprint(diff)


if __name__ == "__main__":
    main()
