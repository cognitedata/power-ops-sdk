from typing import Any, Protocol, Literal

from cognite.client import CogniteClient

from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.client.data_classes import filters


class API(Protocol):
    def list(self, space: str, limit: int) -> list:
        ...

    def delete(self, id: Any) -> list:
        ...


class InstanceAdapter(API):
    def __init__(self, client: CogniteClient, instance_type: Literal["node", "edge"]):
        self.client = client
        self.instance_type = instance_type

    def list(self, space: str, limit: int) -> list:
        is_space = filters.Equals([self.instance_type, "space"], space)
        return self.client.data_modeling.instances.list(filter=is_space, limit=limit, instance_type=self.instance_type)

    def delete(self, id: Any) -> list:
        if self.instance_type == "node":
            return self.client.data_modeling.instances.delete(nodes=id).nodes
        else:
            return self.client.data_modeling.instances.delete(edges=id).edges


def delete_resources(api: API, space: str):
    resource_name = type(api).__name__.removesuffix("API")
    while True:
        resources = api.list(space=space, limit=-1)
        for no, resource in enumerate(resources):
            print(f"{no}): {resource.external_id}")
        print("\na): Delete all")
        print("\nq): Quit")
        if not resources:
            print(f"\nNo more {resource_name} to delete")
            break

        delete_no = input(f"Which {resource_name} to delete? (You can type multiple numbers separated by comma)\n")
        if delete_no.casefold() == "q":
            break
        if delete_no.casefold() == "a":
            delete_numbers = range(len(resources))
        else:
            delete_numbers = delete_no.split(",")
        deleted = api.delete([resources[int(no)].as_id() for no in delete_numbers])
        if deleted:
            print(f"Deleted {', '.join((d.external_id for d in deleted))}")


def main():
    space = "cogShop"

    client = PowerOpsClient.from_settings().cdf
    print(f"Connected to {client.config.project}")
    delete_resources(client.data_modeling.data_models, space)
    delete_resources(client.data_modeling.views, space)
    delete_resources(client.data_modeling.containers, space)
    delete_resources(InstanceAdapter(client, "edge"), space)
    delete_resources(InstanceAdapter(client, "node"), space)


if __name__ == "__main__":
    main()
