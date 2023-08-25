from cognite.powerops.clients.powerops_client import get_powerops_client
from cognite.powerops.utils.cdf import Settings


def main():
    settings = Settings()
    client = get_powerops_client().cdf
    apis = [client.assets, client.sequences, client.time_series, client.files]

    for target_types, api in zip([["asset", "ASSET"], ["sequence"], ["TIMESERIES", "timeSeries"], ["file"]], apis):
        relationships = client.relationships.list(
            data_set_external_ids=[settings.powerops.write_dataset],
            source_types=["asset"],
            target_types=target_types,
            limit=-1,
        )
        type_ids = list({r.target_external_id for r in relationships})
        if not type_ids:
            print(f"There are no relationships with target type {target_types}.")
            continue
        existing_timeseries = api.retrieve_multiple(external_ids=type_ids, ignore_unknown_ids=True)
        existing_timeseries_external_ids = {ts.external_id: ts for ts in existing_timeseries}
        relationships_to_delete = [
            r for r in relationships if r.target_external_id not in existing_timeseries_external_ids
        ]
        print(f"There are {len(relationships)} total relationships with target type {target_types}.")
        print(f"There are {len(relationships_to_delete)} relationships in which target does not exist.")

        if not relationships_to_delete:
            print("Nothing to delete.")
            continue
        answer = input("Do you want to delete them? (y/n)\n")
        if answer.casefold() == "y":
            client.relationships.delete([r.external_id for r in relationships_to_delete])
            print(f"Deleted {len(relationships_to_delete)} relationships.")
        else:
            print("Aborting.")


if __name__ == "__main__":
    main()
