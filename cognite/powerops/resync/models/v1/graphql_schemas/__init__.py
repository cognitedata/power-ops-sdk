#
#
from dataclasses import dataclass
from pathlib import Path

from cognite.client.data_classes.data_modeling import DataModelId


@dataclass
class PowerOpsGraphQLModel:
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


graphql_file = Path(__file__).resolve().parent / "cogshop1.graphql"

GRAPHQL_MODELS = {
    "cogshop1": PowerOpsGraphQLModel(
        name="CogShop",
        description="This is the first iteration of the cogshop model",
        graphql_file=graphql_file,
        id_=DataModelId("cogShop", "CogShop", "2"),
    )
}
