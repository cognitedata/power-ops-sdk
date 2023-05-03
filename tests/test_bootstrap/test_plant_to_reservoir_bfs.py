from pathlib import Path

from bootstrap.data_classes.plant import plant_to_inlet_reservoir_breadth_first_search
from bootstrap.utils.serializer import load_yaml
from tests.test_bootstrap.data.test_config import watercourse_configs


PLANT_TO_RESERVOIR_MAP = {
    "Holen": "Nielsen",
    "Lund": "Danielsen",
    "Lien_krv": "Ranemsletta_1183",
    "Landet": "Livincovs",
    "Dalby": "Lensvik",
    "Rull1": "Dalbysvatn",
    "Rull2": "Sirefelt",
    "Scott": "Lundvann",
    "Strand_krv": "Hagen",
}


def test_plant_to_reservoir_bfs() -> None:
    """Test that the plant to reservoir breadth first search works as expected."""
    # Setup
    config = watercourse_configs[0]
    shop_case = load_yaml(Path(config.yaml_raw_path))

    connections = shop_case["connections"]
    reservoirs = shop_case["model"]["reservoir"]
    connected_reservoirs = dict.fromkeys(PLANT_TO_RESERVOIR_MAP.keys(), None)
    plants = shop_case["model"]["plant"]
    # Run
    for plant_name in plants:
        if plant_name in PLANT_TO_RESERVOIR_MAP:
            connected_reservoirs[plant_name] = plant_to_inlet_reservoir_breadth_first_search(
                plant_name, connections, reservoirs
            )

    # Assert
    assert connected_reservoirs == PLANT_TO_RESERVOIR_MAP
