from pydantic import BaseModel, constr

ExternalId = constr(min_length=1, max_length=255)


class AssetModel(BaseModel):
    name: str
    external_id: ExternalId
