import json
from pathlib import Path

from cognite.client.data_classes import AssetUpdate

from cognite.powerops.utils.cdf import get_cognite_client

BACKUP_FILE = Path("backup.json")


def main():
    client = get_cognite_client()
    backup = client.assets.retrieve(external_id="POWEROPS_bid_process_configuration_multi_scenario_10_NO2")
    BACKUP_FILE.write_text(json.dumps(backup.dump(), indent=4))

    update = AssetUpdate(external_id="POWEROPS_bid_process_configuration_multi_scenario_10_NO2").metadata.add(
        {"bid:is_default_config_for_price_area": "true"}
    )

    client.assets.update(update)


if __name__ == "__main__":
    main()
