import tempfile
from pprint import pprint

import yaml

from cognite.powerops.clients.powerops_client import PowerOpsClient, get_powerops_client

from cognite.powerops.resync._logger import configure_debug_logging


def main():
    # todo: move examples to docs/notebooks/
    configure_debug_logging("DEBUG")

    p: PowerOpsClient = get_powerops_client()

    run = p.shop.runs.retrieve(external_id="POWEROPS_SHOP_RUN_1662548743204")

    print(run.in_progress)
    print(run.status)

    res = run.get_results()

    tmp_dir = tempfile.mkdtemp(prefix="power-ops-sdk-usage")
    print(res.post_run.save_to_disk(tmp_dir))

    print(res.post_run.data["model"]["objective"]["average_objective"]["solver_status"])
    pprint(res.post_run.data["model"]["objective"]["average_objective"])
    print(yaml.dump(res.post_run.data["model"]["objective"]["average_objective"]))


if __name__ == "__main__":
    main()
