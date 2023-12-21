from pathlib import Path

from cognite.client.data_classes.data_modeling import DataModelId

from cognite.powerops.resync.models.base import PowerOpsGraphQLModel

GRAPHQL_FILES = Path(__file__).parent / "graphql_files"
_SHARED_TYPES = GRAPHQL_FILES / "shared"


_SPACE = "power-ops"


class _ExtraTypes:
    input_timeseries_mapping = _SHARED_TYPES / "input_timeseries_mapping.graphql"
    value_transformation = _SHARED_TYPES / "value_transformation.graphql"
    scenario_mapping = _SHARED_TYPES / "scenario_mapping.graphql"


GRAPHQL_MODELS: dict[str, PowerOpsGraphQLModel] = {
    "production": PowerOpsGraphQLModel(
        name="Production",
        description="The production model describes the physical assets such as watercourses, "
        "plants, and generators located in a price area.",
        graphql_file=GRAPHQL_FILES / "production.graphql",
        id_=DataModelId(_SPACE, "production", "1"),
    ),
    "cogshop": PowerOpsGraphQLModel(
        name="CogShop",
        description="The CogShop model describes the interaction between Cognite Data Fusion (CDF) and "
        "Sintef's SHOP algorithm. The scenario is used to determine which SHOP runs are"
        "executed daily and configuration of those SHOP runs.",
        graphql_file=GRAPHQL_FILES / "cogshop.graphql",
        id_=DataModelId(_SPACE, "cogshop", "1"),
        extra_types=[
            _ExtraTypes.value_transformation,
            _ExtraTypes.input_timeseries_mapping,
            _ExtraTypes.scenario_mapping,
        ],
    ),
}
