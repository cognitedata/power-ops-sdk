from functools import cached_property

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

    def _retrieve_with_raise_if_none(self, dataset):
        dataset = self.cdf.data_sets.retrieve(dataset)
        if dataset is None:
            raise ValueError(f"Dataset {dataset} not found.")
        return dataset
