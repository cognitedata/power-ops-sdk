from functools import cached_property
from typing import Union
from cognite.client import CogniteClient


class DataSetsAPI:
    def __init__(self, client: CogniteClient, read: str, write: str):
        self.cdf = client
        self._read = read
        self._write = write

    @cached_property
    def read_dataset_id(self) -> int:
        return self._retrieve_with_raise_if_none(self._read).id

    @cached_property
    def write_dataset_id(self) -> int:
        return self._retrieve_with_raise_if_none(self._read).id

    def _retrieve_with_raise_if_none(self, _dataset: Union[str, int]):
        dataset = None
        if isinstance(_dataset, int):
            dataset = self.cdf.data_sets.retrieve(id=_dataset)
        elif isinstance(_dataset, str):
            dataset = self.cdf.data_sets.retrieve(external_id=_dataset)
        if dataset is None:
            raise ValueError(f"Dataset {dataset} not found.")
        return dataset
