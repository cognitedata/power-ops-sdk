"""
Depends on cognite-neat, needs to be installed manually 'pip install cognite-neat'.
It cannot be set in the pyproject.toml because it requires Python 3.10, pyproject toml
seems to ignore 'cognite-neat = {version = "^0.30.0", python = ">3.9"}.
"""
from cognite.neat.rules import importer
from cognite.client.data_classes.data_modeling import ContainerApplyList, ViewApplyList
from cognite.neat.rules.exporter.rules2dms import DataModel
from cognite.powerops.resync.models.v2.dms import CapacitySourceModel, CapacityModel
import yaml
from pathlib import Path

ROOT = Path(__file__).parent.parent
CAPACITY_MARKET_JSON = ROOT / "customers" / "data" / "970_0ca0c919-046a-4940-b191-f5cc4d0e6513.json"


def main():
    rules = importer.JSONImporter(CAPACITY_MARKET_JSON).to_rules()

    dm = DataModel.from_rules(rules)

    CapacitySourceModel.container_file.write_text(
        yaml.dump(ContainerApplyList(dm.containers.values()).dump(camel_case=True), sort_keys=False)
    )
    CapacityModel.view_file.write_text(
        yaml.dump(ViewApplyList(dm.views.values()).dump(camel_case=True), sort_keys=False)
    )


if __name__ == "__main__":
    main()
