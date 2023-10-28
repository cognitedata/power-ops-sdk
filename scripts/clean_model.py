from time import sleep

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewList, MappedProperty, DataModelList

from cognite.powerops.client.powerops_client import PowerOpsClient
from tests.constants import REPO_ROOT
from cognite.powerops.utils.serialization import chdir


def delete_model(cdf: CogniteClient, space: str):
    while True:
        resources = cdf.data_modeling.data_models.list(space=space, limit=-1, inline_views=True)
        for no, resource in enumerate(resources):
            print(f"{no}): {resource.external_id}")
        print("\na): Delete all")
        print("\nq): Quit")
        if not resources:
            print(f"\nNo more Data Models to delete")
            break

        delete_no = input(f"Which Data Model(s) to delete? (You can type multiple numbers separated by comma)\n")
        if delete_no.casefold() == "q":
            break
        if delete_no.casefold() == "a":
            delete_numbers = range(len(resources))
        else:
            delete_numbers = delete_no.split(",")

        data_models_to_delete = DataModelList([resources[int(no)] for no in delete_numbers])
        views = ViewList([view for model in data_models_to_delete for view in model.views])
        containers = list(
            set(
                [
                    prop.container
                    for view in views
                    for prop in view.properties.values()
                    if isinstance(prop, MappedProperty)
                ]
            )
        )

        deleted = cdf.data_modeling.data_models.delete(data_models_to_delete.as_ids())
        if deleted:
            print(f"Deleted {', '.join((d.external_id for d in deleted))} data models")

        while True:
            # Views can be painful to delete
            deleted = cdf.data_modeling.views.delete(views.as_ids())
            if deleted:
                print(f"Deleted {', '.join((d.external_id for d in deleted))} views")

            views = cdf.data_modeling.views.retrieve(views.as_ids())
            if not views:
                break
            sleep(2)

        deleted = cdf.data_modeling.containers.delete(containers)
        if deleted:
            print(f"Deleted {', '.join((d.external_id for d in deleted))} containers")
        break


def main():
    space = "power-ops"
    with chdir(REPO_ROOT):
        # os.environ["SETTINGS_FILES"] = ".secrets.toml"
        client = PowerOpsClient.from_settings().cdf
        print(f"Connected to {client.config.project}")
        delete_model(client, space)


if __name__ == "__main__":
    main()
