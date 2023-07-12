from dataclasses import dataclass
from pathlib import Path

from cognite.client.data_classes.data_modeling import DataModelId

_POWEROPS_ROOT = Path(__file__).parent
GRAPHQL_SCHEMAS = _POWEROPS_ROOT / "data_models"


@dataclass
class PowerOpsModel:
    name: str
    description: str
    graphql_file: Path
    id_: DataModelId


_SPACE = "power-ops"


MODEL_BY_NAME: dict[str, PowerOpsModel] = {
    "production": PowerOpsModel(
        name="Production",
        description="The production model descripbes the physical assets such as watercourses, "
        "plants, and generators located in a price area.",
        graphql_file=GRAPHQL_SCHEMAS / "production.graphql",
        id_=DataModelId(_SPACE, "production", "1"),
    ),
    "market": PowerOpsModel(
        name="Market",
        description="The market model describes the different markets in an price area and their "
        "financial assets such as "
        "bids and processes. In addition, to benchmarking.",
        graphql_file=GRAPHQL_SCHEMAS / "market.graphql",
        id_=DataModelId(_SPACE, "market", "1"),
    ),
    "cogshop": PowerOpsModel(
        name="CogShop",
        description="The CogShop model describes the interaction between Cognite Data Fusion (CDF) and "
        "Sintef's SHOP algorithm. The scenario is used to determine which SHOP runs are"
        "executed daily and configuration of those SHOP runs.",
        graphql_file=GRAPHQL_SCHEMAS / "cogshop.graphql",
        id_=DataModelId(_SPACE, "cogshop", "1"),
    ),
}
