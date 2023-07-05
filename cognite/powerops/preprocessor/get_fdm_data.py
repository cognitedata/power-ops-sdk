from typing import Optional

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteReadTimeout
from pydantic import BaseModel, Field

from cognite.powerops.utils.retry import retry


class Transformation(BaseModel):
    method: str
    arguments: Optional[str]


class TransformationItems(BaseModel):
    items: list[Transformation]


class Commands(BaseModel):
    commands: list[str]


class FileRef(BaseModel):
    file_external_id: str = Field(alias="fileExternalId")
    type: Optional[str]


class FileRefItems(BaseModel):
    items: list[FileRef]


class Mapping(BaseModel):
    path: str
    timeseries_external_id: Optional[str] = Field(alias="timeseriesExternalId")
    transformations: Optional[TransformationItems]
    retrieve: Optional[str]
    aggregation: Optional[str]


class MappingItems(BaseModel):
    items: list[Mapping]


class ModelTemplate(BaseModel):
    watercourse: str
    model: FileRef
    base_mappings: MappingItems = Field(alias="baseMappings")


class Scenario(BaseModel):
    name: str
    model_template: ModelTemplate = Field(alias="modelTemplate")
    commands: Commands
    mappings_override: Optional[MappingItems] = Field(alias="mappingsOverride")
    extra_files: Optional[FileRefItems] = Field(alias="extraFiles")


class Case(BaseModel):
    start_time: str = Field(alias="startTime")  # datetime.datetime
    end_time: str = Field(alias="endTime")  # datetime.datetime
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
