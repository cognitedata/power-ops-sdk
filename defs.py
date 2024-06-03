import os

from cognite.powerops import PowerOpsClient
from cognite.powerops.client.shop.shop_case import SHOPCase

os.environ["SETTINGS_FILES"] = "settings.toml; secrets.toml"

powerops = PowerOpsClient.from_settings()


def shop_run_func(case: SHOPCase):
    print("submitting case")
    powerops.shop.trigger_single_casefile(case)
    print("case submitted")
