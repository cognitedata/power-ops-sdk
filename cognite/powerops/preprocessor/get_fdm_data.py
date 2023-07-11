from typing import Optional

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteReadTimeout
from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from cognite.powerops.utils.retry import retry


class GraphQlModel(BaseModel, alias_generator=to_camel):  # type: ignore
    ...


class Transformation(GraphQlModel):
    method: str
    arguments: Optional[str] = None


class TransformationItems(GraphQlModel):
    items: list[Transformation]


class Commands(GraphQlModel):
    commands: list[str]


class FileRef(GraphQlModel):
    file_external_id: str
    type: Optional[str] = None


class FileRefItems(GraphQlModel):
    items: list[FileRef]


class Mapping(GraphQlModel):
    path: str
    timeseries_external_id: Optional[str] = None
    transformations: Optional[TransformationItems] = None
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None


class MappingItems(GraphQlModel):
    items: list[Mapping]


class ModelTemplate(GraphQlModel):
    watercourse: str
    model: FileRef
    base_mappings: MappingItems


class Scenario(GraphQlModel):
    name: str
    model_template: ModelTemplate
    commands: Commands
    mappings_override: Optional[MappingItems] = None
    extra_files: Optional[FileRefItems] = None


class Case(GraphQlModel):
    start_time: str
    end_time: str
    scenario: Scenario


def render_query(space: str, case_external_id: str) -> str:
    query_template = """
    query MyQuery {{
        getCaseById(instance: {{space: "{space}", externalId: "{external_id}"}}) {{
            items {{
                startTime
                endTime
                scenario {{
                    name
                    extraFiles (first: 500) {{
                        items {{
                            fileExternalId
                            type
                        }}
                    }}
                    mappingsOverride (first: 500) {{
                        items {{
                            path
                            timeseriesExternalId
                            retrieve
                            aggregation
                            transformations {{
                                items {{
                                    arguments
                                    method
                                }}
                            }}
                        }}
                    }}
                    modelTemplate {{
                        watercourse
                        model {{
                            fileExternalId
                        }}
                        baseMappings (first: 500) {{
                            items {{
                                path
                                timeseriesExternalId
                                retrieve
                                aggregation
                                transformations {{
                                    items {{
                                        method
                                        arguments
                                    }}
                                }}
                            }}
                        }}
                    }}
                    commands {{
                        commands
                    }}
                }}
            }}
        }}
    }}
    """
    return query_template.format(space=space, external_id=case_external_id)


def parse_response(response) -> list[Case]:
    data: dict = response.json()["data"]
    query_key = tuple(data.keys())[0]
    items = data[query_key]["items"]
    return [Case.parse_obj(item) for item in items]


@retry(CogniteReadTimeout, tries=5, delay=1, backoff=2, jitter=(0.1, 0.3))
def query_fdm(
    client: CogniteClient,
    space: str,
    case_external_id: str,
    model_extenal_id: Optional[str] = None,
    model_version: Optional[int] = None,
):
    project = client.iam.token.inspect().projects[0].url_name
    model = space if model_extenal_id is None else model_extenal_id
    version = str(model_version) if model_version is not None else "1"

    url = f"/api/v1/projects/{project}/userapis/spaces/{space}/datamodels/{model}/versions/{version}/graphql"
    return client.post(url, json={"query": render_query(space, case_external_id)})


def get_case(
    client: CogniteClient,
    space: str,
    case_external_id: str,
    model_extenal_id: Optional[str] = None,
    model_version: Optional[int] = None,
) -> Case:
    response = query_fdm(
        client=client,
        space=space,  # type: ignore[arg-type]
        case_external_id=case_external_id,  # type: ignore[arg-type]
        model_extenal_id=model_extenal_id,  # type: ignore[arg-type]
        model_version=model_version,  # type: ignore[arg-type]
    )

    return parse_response(response)[0]
