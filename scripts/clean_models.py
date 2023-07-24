from typing import Any, Protocol

from cognite.powerops.clients.powerops_client import get_powerops_client


class API(Protocol):
    def list(self, space: str) -> list:
        ...

    def delete(self, id: Any) -> list:
        ...


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
    space = "power-ops"

    client = get_powerops_client().cdf
    delete_resources(client.data_modeling.data_models, space)
    delete_resources(client.data_modeling.views, space)
    delete_resources(client.data_modeling.containers, space)


if __name__ == "__main__":
    main()
