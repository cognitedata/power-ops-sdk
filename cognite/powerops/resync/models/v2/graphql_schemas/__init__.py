from pathlib import Path

from cognite.client.data_classes.data_modeling import DataModelId

from cognite.powerops.resync.models.base import PowerOpsGraphQLModel

GRAPHQL_FILES = Path(__file__).parent / "graphql_files"


_SPACE = "power-ops"


GRAPHQL_MODELS: dict[str, PowerOpsGraphQLModel] = {
    "production": PowerOpsGraphQLModel(
        name="Production",
        description="The production model describes the physical assets such as watercourses, "
        "plants, and generators located in a price area.",
        graphql_file=GRAPHQL_FILES / "production.graphql",
        id_=DataModelId(_SPACE, "production", "1"),
    )
}
