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
    extra_types: list[Path] = None

    @property
    def graphql(self) -> str:
        graphql = self.graphql_file.read_text()
        if self.extra_types:
            graphql = "\n\n".join([extra.read_text() for extra in self.extra_types] + [graphql])

        return graphql


_SPACE = "power-ops"


class _ExtraTypes:
    input_timeseries_mapping = GRAPHQL_SCHEMAS / "input_timeseries_mapping.graphql"
    value_transformation = GRAPHQL_SCHEMAS / "value_transformation.graphql"
    date_transformation = GRAPHQL_SCHEMAS / "date_transformation.graphql"
    market = GRAPHQL_SCHEMAS / "market.graphql"
    scenario_mapping = GRAPHQL_SCHEMAS / "scenario_mapping.graphql"
    shop_transformation = GRAPHQL_SCHEMAS / "shop_transformation.graphql"


MODEL_BY_NAME: dict[str, PowerOpsModel] = {
    "production": PowerOpsModel(
        name="Production",
        description="The production model descripbes the physical assets such as watercourses, "
        "plants, and generators located in a price area.",
        graphql_file=GRAPHQL_SCHEMAS / "production.graphql",
        id_=DataModelId(_SPACE, "production", "1"),
    ),
    "cogshop": PowerOpsModel(
        name="CogShop",
        description="The CogShop model describes the interaction between Cognite Data Fusion (CDF) and "
        "Sintef's SHOP algorithm. The scenario is used to determine which SHOP runs are"
        "executed daily and configuration of those SHOP runs.",
        graphql_file=GRAPHQL_SCHEMAS / "cogshop.graphql",
        id_=DataModelId(_SPACE, "cogshop", "1"),
        extra_types=[
            _ExtraTypes.value_transformation,
            _ExtraTypes.input_timeseries_mapping,
            _ExtraTypes.scenario_mapping,
        ],
    ),
    "dayahead": PowerOpsModel(
        name="DayAhead",
        description="The DayAhead model describes the day-ahead market.",
        graphql_file=GRAPHQL_SCHEMAS / "dayahead-market.graphql",
        id_=DataModelId(_SPACE, "dayaheadMarket", "1"),
        extra_types=[
            _ExtraTypes.value_transformation,
            _ExtraTypes.date_transformation,
            _ExtraTypes.input_timeseries_mapping,
            _ExtraTypes.scenario_mapping,
            _ExtraTypes.shop_transformation,
            _ExtraTypes.market,
        ],
    ),
    "rkom": PowerOpsModel(
        name="RKomMarket",
        description="The RKOM market is a balancing market",
        graphql_file=GRAPHQL_SCHEMAS / "rkom-market.graphql",
        id_=DataModelId(_SPACE, "rkomMarket", "1"),
        extra_types=[
            _ExtraTypes.value_transformation,
            _ExtraTypes.date_transformation,
            _ExtraTypes.input_timeseries_mapping,
            _ExtraTypes.scenario_mapping,
            _ExtraTypes.shop_transformation,
            _ExtraTypes.market,
        ],
    ),
    "benchmark": PowerOpsModel(
        name="Benchmark",
        description="The Benchmark model is used for benchmarking different bid processes.",
        graphql_file=GRAPHQL_SCHEMAS / "benchmark-market.graphql",
        id_=DataModelId(_SPACE, "benchmarkMarket", "1"),
        extra_types=[
            _ExtraTypes.value_transformation,
            _ExtraTypes.date_transformation,
            _ExtraTypes.input_timeseries_mapping,
            _ExtraTypes.scenario_mapping,
            _ExtraTypes.shop_transformation,
            _ExtraTypes.market,
        ],
    ),
}
