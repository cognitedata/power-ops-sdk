from pathlib import Path

from cognite.client.data_classes.data_modeling import DataModelId

from cognite.powerops.resync.models.base import PowerOpsGraphQLModel

graphql_file = Path(__file__).resolve().parent / "cogshop1.graphql"

GRAPHQL_MODELS = {
    "cogshop1": PowerOpsGraphQLModel(
        name="CogShop",
        description="This is the first iteration of the cogshop model",
        graphql_file=graphql_file,
        id_=DataModelId("cogShop", "CogShop", "2"),
    )
}
