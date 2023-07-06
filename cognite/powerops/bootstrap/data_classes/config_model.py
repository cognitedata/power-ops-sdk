from pydantic import BaseModel, ConfigDict


class Configuration(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
