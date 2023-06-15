import json
from dataclasses import dataclass
from typing import List, Optional, Union

from cognite.powerops.preprocessor.transformation_functions import Transformation, transformation_factory

from ..get_fdm_data import Mapping


@dataclass
class TimeSeriesMapping:
    shop_model_path: str
    transformations: List[Transformation]
    time_series_external_id: Optional[str] = None
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None

    @classmethod
    def from_mapping_model(cls, mapping: Mapping) -> "TimeSeriesMapping":
        transformations = []
        if mapping.transformations is not None:
            for t in mapping.transformations.items:
                kwargs = None
                if isinstance(t.arguments, str):
                    kwargs = json.loads(t.arguments)
                kwargs = {} if kwargs is None else kwargs

                transformation = transformation_factory(t.method, kwargs=kwargs)
                transformations.append(transformation)

        return cls(
            shop_model_path=mapping.path,
            time_series_external_id=mapping.timeseries_external_id,
            transformations=transformations,
            retrieve=mapping.retrieve,
            aggregation=mapping.aggregation,
        )

    @classmethod
    def from_dict(cls, d: dict) -> "TimeSeriesMapping":
        transformations_string = "".join(
            d.get(key, "")
            for key in (
                "transformations",
                "transformations1",
                "transformations2",
                "transformations3",
            )
        )
        transformation_dicts = json.loads(transformations_string) if transformations_string else []
        transformations = [
            transformation_factory(transformation_type=d["transformation"], kwargs=d.get("kwargs", {}))
            for d in transformation_dicts
        ]
        return cls(
            shop_model_path=d["shop_model_path"],
            time_series_external_id=d.get("time_series_external_id"),
            transformations=transformations,
            retrieve=d.get("retrieve"),
            aggregation=d.get("aggregation"),
        )

    def split_shop_model_path(self) -> List[str]:
        return self.shop_model_path.split(".")

    @property
    def object_type(self) -> str:
        return self.split_shop_model_path()[0]

    @property
    def instance(self) -> Union[int, str]:
        instance = self.split_shop_model_path()[1]
        # TODO: confirm that this approach is OK
        if instance.isdigit():
            # E.g. market or scenario number
            return int(instance)
        return instance

    @property
    def attribute(self) -> str:
        return self.split_shop_model_path()[2]
