from pydantic import BaseModel


class Configuration(BaseModel):
    class Config:
        allow_population_by_field_name = True
